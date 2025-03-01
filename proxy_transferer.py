import pathlib
import common
import experiment
import data_tools
import logging
import configuration
import experiment
import fs_storage_engine
import shutil


# TODO - this should work parallel for experiments!

def find_proxy_destination_directory_helper(exp: experiment.ExperimentWrapper, lims_conf: configuration.LimsConfigWrapper):
     # In order to be able to perform proxy transfer, we need to know which node is the "storage" node for given experiment
    # For now do it this way, not very clean tho:
    # 1. Find job lifecycle services and their configuration
    # 2. From them, find the one that is configured to handle this experiment's type
    node_mods = list(lims_conf.find_module_config_nodes("job_lifecycle_service.JobLifecycleService"))
    target_mod = None
    for nm in node_mods:
        matches = [True for exptype in nm["experiment_types"] if exp.exp_type_matches(exptype["pattern"])]
        if matches:
            target_mod = nm
            break   

    # There is no such module, nothing to do
    if not target_mod: 
        return
    
    # Prepare path mappings
    # Find proxy destination
    return lims_conf.translate_path(exp.data_source.source_directory, exp.secondary_id, path_mappings=target_mod["PathMappings"], to_proxy=True)

class ProxyTransferHandler(configuration.LimsNodeModule):
    
    def step(self):
        exps =  experiment.ExperimentsApi(self._api_session).get_active_experiments()
        for exp in exps:
            self._to_proxy_for_experiment(exp)

    def _to_proxy_for_experiment(self, exp: experiment.ExperimentWrapper):
        lims_conf = self.module_config.lims_config
        destination_dir = find_proxy_destination_directory_helper(exp, lims_conf)
            
        if not destination_dir: 
            # If destination is same after remap, there is nothing to proxy
            return
        
        data_rules = lims_conf.get_experiment_config(exp.instrument, exp.technique).data_rules
        # Only raw data rules
        data_rules = data_rules.with_tags("raw", "metadata")
        # Add rules for source patterns
        data_rules = exp.data_source.get_combined_raw_datarules(data_rules, exp.data_source.keep_source_files)

        transferer = data_tools.DataAsyncTransferer(
            data_tools.FsTransferSource(exp.data_source.source_directory),
            data_tools.FsTransferSource(destination_dir),
            data_rules,
            f"proxy_{exp.secondary_id}_{common.pathify_date(exp.dt_created)}"
        )

        transferer.transfer()