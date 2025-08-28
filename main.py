import argparse
import datetime
import importlib
import logging
import sys
import time
import configuration
import logger_db_api
import socket
import common
import threading
import experiment

# Trigger a stack trace when SIGUSR1 is received
# For debugging stuck threads
if sys.platform != "win32":
    import faulthandler
    import signal
    signal.signal(signal.SIGUSR1, lambda sig, frame: faulthandler.dump_traceback())

aparser = argparse.ArgumentParser(
    prog = 'lims-node',
    description = 'LIMS processing/controlling node',
    epilog = 'Text at the bottom of help'
)

aparser.add_argument("--organization-name", "-o", dest="organization_name", help="An organization this node belongs to.")
aparser.add_argument("--node-name", "-n", dest="node_name", help="Name of this node. Must be unique among all of the running nodes. Default is the hostname.")
aparser.add_argument("--config-file", dest="config_file", help="Path to a yaml file that configures this node.")
aparser.add_argument("--sip-api-url", "-u", dest="sip_api_url", help="Base URL of the LIMS HTTTP API.")
aparser.add_argument("--sip-api-key", "-s", dest="sip_api_key", help="A key that is used to authorize organization in the LIMS API/")
aparser.add_argument("--sip-api-https-proxy", "-p", dest="sip_api_https_proxy", help="A proxy server to be used to communicatet with LIMS API")
aparser.add_argument("--refresh-interval", "-r", dest="refresh_interval", default=6.0, type=float, help="How often to ping LIMS database, fetch/submit configuration and adjust executed modules accordingly. Default 15sec.")
aparser.add_argument("-d --debug", dest="debug_mode", action='store_true')
arguments = aparser.parse_args()

# Configure root logger
loglvl = logging.DEBUG if arguments.debug_mode else logging.INFO
logging.basicConfig(level=loglvl)


# Handle obtaining configuration and connecting to LIMS
is_config_master = False
node_name = arguments.node_name if arguments.node_name else socket.gethostname()
debug_mode = bool(arguments.debug_mode)
config = configuration.LimsConfigWrapper(arguments.organization_name, node_name)

# Disable ssl verify warnings
if debug_mode:
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

if arguments.config_file:
    config.from_file(arguments.config_file)
    is_config_master = True
    # API arguments should be present in the file - load them
    api_config = config["SipApi"]
    arguments.organization_name = arguments.organization_name or api_config["Organization"]
    arguments.sip_api_url = arguments.sip_api_url or api_config["BaseUrl"]
    arguments.sip_api_key = arguments.sip_api_key or api_config["SecretKey"]

if not arguments.organization_name:
    aparser.error("Organization name must be provided either by command line argument (-o, --organization-name) or through config file (SipApi.Organization)")

if not arguments.sip_api_key:
    aparser.error("SIP API key must be provided either by command line argument (--sip-api-key) or through config file (SipApi.SecretKey)")

if not arguments.sip_api_url:
    aparser.error("SIP API URL must be provided either by command line argument (--sip-api-url) or through config file (SipApi.BaseUrl)")


# ========= Factories, edit them to provide required dependencies ===========
def lims_api_session_provider():
        return configuration.create_lims_session(arguments.sip_api_url, arguments.sip_api_key, arguments.sip_api_https_proxy, verify=not arguments.debug_mode)

def exp_storage_engine_factory(exp: experiment.ExperimentWrapper, e_config: configuration.JobConfigWrapper, logger: logging.Logger, module_config: configuration.LimsModuleConfigWrapper, engine: str=None):
    engine = engine or exp.storage.engine
    if (engine.startswith("fs")):
        import fs_storage_engine
        return fs_storage_engine.fs_storage_engine_factory(exp, e_config, logger, module_config, engine)
    if (engine.startswith("irods")):
        import irods_storage_engine
        return irods_storage_engine.irods_storage_engine_factory(exp, e_config, logger, module_config, engine)

# Prepare logger handler for saving logs to SIP server and make it run on separate thread
sip_logger_handler = logger_db_api.LimsApiLoggerHandler(lims_api_session_provider(), logging.INFO)
threading.Thread(target=sip_logger_handler.keep_flushing, daemon=True).start()

def make_module(cls, conf):
    name = conf["target"]
    logger = logging.getLogger(conf["target"])
    lims_logger = logger_db_api.prepare_lims_api_logger("lims/" + conf["target"], node_name, sip_logger_handler)
    module_config = configuration.LimsModuleConfigWrapper(name, node_name, config)
    if issubclass(cls, experiment.ExperimentModuleBase):
        return cls(name, logger, lims_logger, module_config, lims_api_session_provider(), exp_storage_engine_factory)
    if issubclass(cls, configuration.LimsNodeModule):
        return cls(name, logger, lims_logger, module_config, lims_api_session_provider())

def make_config_syncer(config_syncer_class):
    conf_syncer = config_syncer_class(
    config_syncer_class.__module__ + "." + config_syncer_class.__name__, 
    logging.getLogger("config_syncer"), 
    logger_db_api.prepare_lims_api_logger("lims_config_syncer", node_name, sip_logger_handler),
    configuration.LimsModuleConfigWrapper(None, node_name, config), lims_api_session_provider()
    ) 
    return conf_syncer
# ==============================================================

# Keep configuration up to date, and ping
conf_syncer = make_config_syncer(configuration.ConfigToDbSyncer if is_config_master else configuration.ConfigFromDbSyncer)

modules_dict = {}
action_targets = {} # module.method => (task, config)

while True:
    # Ping lims and fetch/submit configuration
    try:
        conf_syncer.step()
        logging.debug(f"{datetime.datetime.now()} Configuration synced.")
    except Exception as e:
        logging.exception(e)
        time.sleep(arguments.refresh_interval)
        continue
    
    mods = []
    if not config.is_empty:
        mods = config.node["Modules"]

    # Kill modules that are not present any longer
    for active_mod_name in list(action_targets):
        mod_config = next(filter(lambda x: x["target"] == active_mod_name, mods), None)
        if mod_config is None or ("enabled" in mod_config and not mod_config["enabled"]):
            action_targets[active_mod_name][0].set() # Set thread cancel event
            del action_targets[active_mod_name] # Remove thread from current threads
        
    for conf in mods:
        # Is this action enabled? If not, skip it.
        if "enabled" in conf and not conf["enabled"]:
            continue

        # Is this action already running?
        if conf["target"] in action_targets:
            continue

        # This action is not running - start it
        target_spl = conf["target"].split(".")
        module_name, class_name = ".".join(target_spl[:-1]), target_spl[-1]
        # Import and configure the module, if not yet done
        if module_name not in modules_dict:
            module = importlib.import_module(module_name)
            modules_dict[module_name] = module
        else:
            module = modules_dict[module_name]

        interval = common.parse_timedelta(conf["interval"])
        seconds = interval.total_seconds()
        
        try:
            task_instance = make_module(getattr(module, class_name), conf)
        except Exception as e:
            logging.error("Failed to initialize module action", exc_info=e)
            continue
        
        cancel_event = threading.Event()
        thrd = threading.Thread(target=common.action_thread, daemon=True, args=(task_instance.step, seconds, cancel_event, False))
        thrd.start()
        action_targets[conf["target"]] = (cancel_event, conf)

    try:
        time.sleep(arguments.refresh_interval)
    except KeyboardInterrupt:
        break
