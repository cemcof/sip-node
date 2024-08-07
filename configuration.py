"""Configuration and node/module management"""

import datetime
import logging
import pathlib, fnmatch
import sys, typing
import time
import yaml, json, requests

import common
from data_tools import DataRulesWrapper

class ConfigWrapperBase:
    def __init__(self, data, parent: 'ConfigWrapperBase'=None) -> None:
        self.data = data
        self.parent = parent


class JobConfigWrapper:
    def __init__(self, data) -> None:
        self._data = data

    @property
    def metadata(self):
        return self._data["Metadata"]
    
    @property
    def data_rules(self):
        return DataRulesWrapper(self._data["DataRules"] if "DataRules" in self._data else [])
    
    @property 
    def raw_data_rules(self):
        drls = self.data_rules
        [data_tools.DataRule(p, ["raw"], ".", True) for p in exp.storage.source_patterns]
    
    def __getitem__(self, item):
        return self._data[item]


class LimsConfigWrapper():
    def __init__(self, organization, node_name, config=None):
        if config is None:
            config = {}
        self._config = config
        self._file_path = None
        self._organization = organization
        self.node_name = node_name
        self._configstr_cached = yaml.dump(self._config)
        self._last_update = datetime.datetime.fromtimestamp(0)

    

    
    def from_file(self, path: str=None):
        if path is not None:
            self._file_path = path

        if self._file_path.startswith("http"):
            return self.from_url(self._file_path)
        else: 
            return self.from_local_file(pathlib.Path(self._file_path))
        
    
    def from_local_file(self, path: pathlib.Path):
        lastmodnew = datetime.datetime.utcfromtimestamp(path.stat().st_mtime)
        if lastmodnew > self._last_update or self._last_update == datetime.datetime.fromtimestamp(0):
            self._last_update = datetime.datetime.utcnow()
            self._configstr_cached = path.read_text()
            self._config = yaml.full_load(self._configstr_cached)
            # From now on, we will use json - yaml just for loading the file
            # self._configstr_cached = json.dumps(self._config)
            #  TODO not really necessary?
            return True

        return False # No config refresh

    def from_url(self, url: str): 
        result = requests.get(url)
        if result.text != self._configstr_cached: 
            self._configstr_cached = result.text
            self._config = yaml.full_load(self._configstr_cached)
            return True
        
        return False

    def reset(self):
        self._config = {}
        self._configstr_cached = "{}"
        self._last_update = datetime.datetime.min

    def from_obj(self, obj, dt_config):
        self._config = obj
        self._configstr_cached = json.dumps(obj)
        self._last_update = dt_config

    def __getitem__(self, item):
        return self._config[item]

    @property
    def config(self):
        return self._config
    
    @property
    def is_empty(self):
        return not bool(self._config)

    @property
    def node(self):
        return LimsModuleConfigWrapper(None, self.node_name, self)

    def get_node_config(self, node_name=None):
        return self._config["LimsNodes"][node_name or self.node_name]
    
    def get_module_config(self, module_name, node_name=None):
        return next(filter(lambda x: x["target"] == module_name, self.get_node_config(node_name)["Modules"]), None)
    
    def get_experiment_config(self, instrument, technique):
        return JobConfigWrapper(self._config["Experiments"][instrument][technique])

    
    def find_module_config_any_node(self, module_name):
        nodes = self.find_module_config_nodes(module_name)
        return next(nodes, (None, None))

    def find_module_config_nodes(self, module_name):
        for k,v in self._config["LimsNodes"].items():
            mod = next(filter(lambda x: x["target"] == module_name, v["Modules"]), None)
            if mod:
                yield LimsModuleConfigWrapper(module_name, k, self)

    def translate_path(self, path: pathlib.Path, safe_stem: str, path_mappings=None, to_proxy=False) -> pathlib.Path:
        if not path_mappings:
            path_mappings = self.node["PathMappings"]

        for mpp in path_mappings:
            fromp = common.path_universal_factory(mpp["From"])
            to = common.path_universal_factory(mpp["To"])
            proxy = common.path_universal_factory(mpp["Proxy"]) if "Proxy" in mpp and mpp["Proxy"] else None

            if to_proxy and not proxy:
                continue
            if to_proxy:
                to = proxy

            rel = common.try_translate_path(path, fromp, to)
            if rel and proxy:
                return to / safe_stem
            elif rel:
                return to / rel
        return None



class LimsModuleConfigWrapper():
    def __init__(self, module_name, node_name, lims_config: LimsConfigWrapper) -> None:
        self.lims_config: LimsConfigWrapper = lims_config
        self.module_name = module_name
        self.node_name = node_name

    def get(self, item_path):
        try: 
            return self.__getitem__(item_path)
        except KeyError:
            return None
        
    def __getitem__(self, item):
        if self.module_name:
            module_config = self.lims_config.get_module_config(self.module_name, self.node_name)
            val = common.get_dict_val_by_path(module_config, item)
            if val is not None:
                return val
            
        # Now try node config 
        node_config = self.lims_config.get_node_config(self.node_name)
        val = common.get_dict_val_by_path(node_config, item)
        if val is not None:
            return val

        # Try global config
        global_c = common.get_dict_val_by_path(self.lims_config.config, item)
        if global_c is not None:
            return global_c
        
        raise KeyError(f"Key {item} not found in the configuration")
        
    

class LimsNodeModule:
    def __init__(self, name, logger, lims_logger, module_config: LimsModuleConfigWrapper, api_session: requests.Session):
        self.name = name
        self.logger: logging.Logger = logger
        self._lims_logger: logging.Logger = lims_logger
        self._api_session: requests.Session = api_session
        self.module_config: LimsModuleConfigWrapper = module_config

    def step(self):
        pass

    
    

class ConfigToDbSyncer(LimsNodeModule):

    def step(self):
        config = self.module_config.lims_config

        # Load new configuration if changed
        config.from_file()

        # Ping LIMS, response of ping is the last time center configuration was updated 
        ping_result = self._api_session.get(f"centers/ping/{config.node_name}")
        last_config_update = datetime.datetime.fromisoformat(ping_result.text)

        # If currently loaded config is newer then that one in LIMS, push it ther
        if config._last_update > last_config_update: 
            res = self._api_session.post(f"centers?nodeSubmitter={config.node_name}", json=config._config)
            res.text
            self.logger.info(f"Submitted configuration to the LIMS, {config._last_update} > {last_config_update}")



class ConfigFromDbSyncer(LimsNodeModule):

    def step(self):
        config = self.module_config.lims_config

        # Ping LIMS, response of ping is the last time center configuration was updated 
        ping_result = self._api_session.get(f"centers/ping/{config.node_name}")
        last_config_update = datetime.datetime.fromisoformat(ping_result.text)

        # If current configuration is older then that one in LIMS, pull it from there
        if last_config_update == datetime.datetime.min: # This means there is no configuration available
            self.logger.info(f"No configuration available.")
            config.reset()
        elif last_config_update > config._last_update: # The config in db is newer - we should fetch
            config_fetched = self._api_session.get("centers").json()
            config.from_obj(config_fetched, last_config_update)
            self.logger.info(f"Fetched configuration from LIMS.")


   
def create_lims_session(base_url, api_key, https_proxy=None):
    session = common.BaseUrlSession(base_url)
    session.headers.update({"lims-organization": api_key})

    if https_proxy:
        session.proxies = {"https": https_proxy}
        
    return session

