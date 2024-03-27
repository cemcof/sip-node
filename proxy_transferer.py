import common
import experiment
import data_tools
import logging
import configuration

class ProxyTransferHandler(experiment.ExperimentModuleBase):
    
    def on_experiment_running(self, exp_engine: experiment.ExperimentStorageEngine):
        exp = exp_engine.exp
        # In order to be able to perform proxy transfer, we need to know which node is the "storage" node for given experiment
        # For now do it this way, not very clean tho:
        # 1. Find job lifecycle services and their configuration
        # 2. From them, find the one that is configured to handle this experiment's type
        node_mods = list(self.module_config.lims_config.find_module_config_nodes("job_lifecycle_service.JobLifecycleService"))
        target_mod = next(filter(lambda x: exp.exp_type_matches(x["ExperimentTypes"]), node_mods), None)

        # There is no such module, nothing to do
        if not target_mod: 
            return
        
        node, mod = target_mod

        # Prepare path mappings
        # Find proxy destination
        destination_dir = self.module_config.lims_config.translate_path(exp.storage.source_directory, exp.secondary_id, for_node=node, to_proxy=True)
            
        if not destination_dir: 
            # If destination is same after remap, there is nothing to proxy
            return

        exp_file_patterns = exp.storage.source_patterns
        
        # ptstr = ", ".join(exp_file_patterns)
        # print(f"Proxy test: {exp.storage.source_directory} {destination_dir} {ptstr} {exp.storage.keep_source_files} {path_mappings}")
         
        # Perform the transfer
        transferer = data_tools.DataTransferer(
            src_path=exp.storage.source_directory, 
            dst_path=destination_dir,
            patterns=exp_file_patterns, 
            logger=self.logger, 
            remove_source=not exp.storage.keep_source_files)
        

        transferer.transfer()

