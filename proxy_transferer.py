import pathlib
import common
import experiment
import data_tools
import logging
import configuration
import experiment
import fs_storage_engine
import shutil

class ProxyTransferHandler(configuration.LimsNodeModule):
    
    def step(self):
        exps =  experiment.ExperimentsApi(self._api_session).get_active_experiments()
        for exp in exps:
            self._to_proxy_for_experiment(exp)

    def _to_proxy_for_experiment(self, exp: experiment.ExperimentWrapper):
        lims_conf = self.module_config.lims_config
        # In order to be able to perform proxy transfer, we need to know which node is the "storage" node for given experiment
        # For now do it this way, not very clean tho:
        # 1. Find job lifecycle services and their configuration
        # 2. From them, find the one that is configured to handle this experiment's type
        node_mods = list(lims_conf.find_module_config_nodes("job_lifecycle_service.JobLifecycleService"))
        matched_mods = filter(lambda x: exp.exp_type_matches(x["ExperimentTypes"]), node_mods)
        target_mod = next(matched_mods, None)

        # There is no such module, nothing to do
        if not target_mod: 
            return
        
        # Prepare path mappings
        # Find proxy destination
        destination_dir = lims_conf.translate_path(exp.storage.source_directory, exp.secondary_id, path_mappings=target_mod["PathMappings"], to_proxy=True)
            
        if not destination_dir: 
            # If destination is same after remap, there is nothing to proxy
            return
        
        data_rules = lims_conf.get_experiment_config(exp.instrument, exp.technique).data_rules
        # Only raw data rules
        data_rules = data_rules.with_tags("raw", "metadata")
        # Add rules for source patterns
        data_rules = data_tools.DataRulesWrapper(data_rules.data_rules + [data_tools.DataRule(p, ["raw"], ".", True) for p in exp.storage.source_patterns])

        source_dir = exp.storage.source_directory
        def proxy_transfer_handler(source_path: pathlib.Path, data_rule: data_tools.DataRule):
            path_rel = source_path.relative_to(source_dir)
            target = destination_dir / path_rel
            if target.exists() and target.stat().st_mtime >= source_path.stat().st_mtime:
                return
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source_path, target)
            if not exp.storage.keep_source_files:
                source_path.unlink()
            self.logger.info(f"PROXY COPY: {source_path} to {target}")

        sniffer = data_tools.DataRulesSniffer(source_dir, data_rules, proxy_transfer_handler)
        sniffer.sniff_and_consume()

