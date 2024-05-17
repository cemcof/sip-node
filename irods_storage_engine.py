
import pathlib, yaml, datetime, logging
import common
import data_tools
from experiment import ExperimentWrapper
import logger_db_api
import experiment
import configuration
from irods.session import iRODSSession
from irods.meta import iRODSMeta
from irods.ticket import Ticket
from irods.collection import iRODSCollection
import irods.message
import fnmatch
import fs_storage_engine

# Filter out aggressive debug logs from irods that spit out tons of binary data
logging.getLogger("irods.connection").setLevel(logging.INFO)
logging.getLogger("irods.message").setLevel(logging.INFO)
logging.getLogger("irods.pool").setLevel(logging.INFO)

class IrodsCollectionWrapper:
    def __init__(self, irods_session: iRODSSession, collection_path: pathlib.Path, logger: logging.Logger = logging.getLogger()) -> None:
        self.irods_session = irods_session
        self.collection_path = collection_path
        self.logger = logger

    @property
    def collection(self):
        return self.irods_session.collections.get(str(self.collection_path))

    def ensure_exists(self, col: pathlib.Path = None):
        # TODO - whole hierarchy?
        if not col:
            col = self.collection_path

        existed = self.irods_session.collections.exists(str(col))

        if not existed:
            self.irods_session.collections.create(str(col))

        return existed
    
    def ensure_file(self, 
        source_base: pathlib.Path, 
        target_relative: pathlib.Path, 
        source_relative: pathlib.Path = None,
        replace: bool = False):

        target = self.collection_path / target_relative
        source = source_base if not source_relative else source_base / source_relative

        if self.irods_session.data_objects.exists(str(target)) and not replace:
            return False

        # Ensure collection exists
        self.ensure_exists(target.parent)

        # Start and measure file upload    
        size = source.stat().st_size
        # size_fmt = common.sizeof_fmt(source.stat().st_size)
        t_start = datetime.datetime.now(datetime.timezone.utc)
        self.irods_session.data_objects.put(str(source), str(target))
        t_done = datetime.datetime.now(datetime.timezone.utc)
        time_delta_secs = (t_done - t_start).total_seconds()
        # self.logger.info(f"Transfered to iRODS: {str(source_relative or source_base.name)} -> {target_relative}, {size_fmt}, {time_delta_secs:.3f}s")
        return time_delta_secs, size
    
    def exists(self,relative_path: pathlib.Path):
        return self.irods_session.data_objects.exists(str(self.collection_path / relative_path))

    def open_dataobject(self, path, mode="r+"):
        pth = self.collection_path / path
        exists = self.irods_session.data_objects.exists(str(pth))
        if not exists:
            self.irods_session.data_objects.create(str(pth))

        experiment_dataobj = self.irods_session.data_objects.get(str(pth)) 
        return experiment_dataobj.open(mode)
    
    def generate_ticket(self, permission='read'):
        return Ticket(self.irods_session).issue(permission, self.collection_path).string

    def read_file(self, path_relative: pathlib.Path, as_text=True):
        dataobj = self.irods_session.data_objects.get(str(self.collection_path / path_relative))
        with dataobj.open('r') as file: 
            data = file.read()
            return data if not as_text else str(data, "utf-8")
        
    def get_file(self, path_relative_src: pathlib.Path, path_dst: pathlib.Path):
        return self.irods_session.data_objects.get(str(self.collection_path / path_relative_src), str(path_dst), forceFlag=True)

    def write_file(self, path_relative: pathlib.Path, content):
        with self.open_dataobject(path_relative, "w") as f:
            f.write(content if isinstance(content, bytes) else str.encode(content))
        # dataobj = self.irods_session.data_objects.get(str(self.collection_path / path_relative))
        # with dataobj.open('w') as file: 
        #     file.write(content)
            
    def glob(self, patterns):
        patterns = list(patterns)
        def walk_collection(collection: iRODSCollection):
            for subcol in collection.subcollections:
                yield from walk_collection(subcol) # Recurse into subcollections
            
            for dobj in collection.data_objects:
                yield dobj
        
        for dataobj in walk_collection(self.collection):

            relative_dataobj = pathlib.Path(dataobj.path).relative_to(self.collection_path)

            # Does this relative path fnmatch one of given patterns?
            for pattern in patterns:
                # print(f"matching {pattern} against {relative_dataobj} = {fnmatch.fnmatch(str(relative_dataobj), pattern)}")
                if fnmatch.fnmatch(str(relative_dataobj), pattern):
                    yield relative_dataobj
                    break

    def store_irods_metadata(self, metadata: dict, target_file: pathlib.Path=None):
        col = self.collection 

        if target_file:
            with self.open_dataobject(str(target_file)) as f:
                # yaml.dump({ k: v for k,v,_ in metad }, f) Doesnt work...
                # So lets try this explicitly
                yamlstr = yaml.dump(metadata)
                f.write(str.encode(yamlstr))
                self.logger.info(f"Experiment data model saved to the collection {str(self.collection_path)}")

        for met_name, met_value in metadata.items():
            if met_value:
                imeta = iRODSMeta(met_name, met_value, '')
                col.metadata[imeta.name] = imeta
                
        self.logger.info(f"Attached metadata to collection: {str(self.collection_path)}")         

    def drop_collection(self):
        self.collection.remove(recurse=True)


class IrodsExperimentStorageEngine(experiment.ExperimentStorageEngine):

    def __init__(self, experiment: ExperimentWrapper, 
                 logger: logging.Logger,
                 data_rules: configuration.DataRulesWrapper,
                 metadata_model: dict,
                 connection: dict,
                 collection_base,
                 
                 mount_point=None,
                 metadata_target="experiment.yml") -> None:
        super().__init__(experiment, logger, data_rules, metadata_model, metadata_target)
        self.connection_config = connection
        self.collection_base = pathlib.Path(collection_base)
        self.mount_point = pathlib.Path(mount_point) if mount_point else None

        self.irods_collection = IrodsCollectionWrapper(
            irods_session=iRODSSession(**self.connection_config), 
            collection_path=self.collection_base / self.exp.secondary_id, # TODO - internal path to exp structure?
            logger=self.logger)
        
        self.fs_underlying_storage = None
        if self.mount_point:
            self.fs_underlying_storage = fs_storage_engine.FsExperimentStorageEngine(
                experiment=self.exp, 
                logger=self.logger, 
                data_rules=self.data_rules, 
                metadata_model=self.metadata_model,
                base_path=self.mount_point, 
                server_base_path=self.mount_point, 
                server=None,
                metadata_target=self.metadata_target)

    def resolve_target_location(self, src_relative: pathlib.Path = None) -> pathlib.Path:
        if self.mount_point:
            return self.mount_point / self.get_exp_subpath() / self.exp.secondary_id / (src_relative or "")
        return None
    
    def restore_metadata(self, metadata={}):
        meta = super().restore_metadata(metadata)
        if meta: 
            self.irods_collection.store_irods_metadata(target_file=self.metadata_target, metadata=meta)

    def is_accessible(self):
        if self.fs_underlying_storage:
            return self.fs_underlying_storage.is_accessible()
        try:
            self.irods_collection.irods_session.pam_pw_negotiated
            return True
        except Exception:
            return False
    
    def get_access_info(self):
        return {
            "Target": self.irods_collection.irods_session.host, 
            "Path": str(self.irods_collection.collection_path),
            "Token": self.irods_collection.generate_ticket()
        }
    
    def prepare(self):
        self.irods_collection.ensure_exists()

    def read_file(self, path_relative: pathlib.Path, as_text=True):
        if (self.fs_underlying_storage):
            return self.fs_underlying_storage.read_file(path_relative, as_text)
        return self.irods_collection.read_file(path_relative, as_text)
    
    def write_file(self, path_relative: pathlib.Path, content):
        if (self.fs_underlying_storage):
            self.fs_underlying_storage.write_file(path_relative, content)
        else:
            self.irods_collection.ensure_exists(self.irods_collection.collection_path / path_relative.parent)
            self.irods_collection.write_file(path_relative, content)
    
    def put_file(self, path_relative: pathlib.Path, src_file: pathlib.Path, skip_if_exists=True):
        if (self.fs_underlying_storage):
            return self.fs_underlying_storage.put_file(path_relative, src_file, skip_if_exists)
        return self.irods_collection.ensure_file(src_file, path_relative, replace=not skip_if_exists)
        
    def get_file(self, path_relative_src: pathlib.Path, path_dst: pathlib.Path):
        if (self.fs_underlying_storage):
            return self.fs_underlying_storage.get_file(path_relative_src, path_dst)
        return self.irods_collection.get_file(path_relative_src, path_dst)

    def file_exists(self, path_relative: pathlib.Path):
        if (self.fs_underlying_storage):
            return self.fs_underlying_storage.file_exists(path_relative)
        return self.irods_collection.exists(path_relative)
    
    def purge(self):
        self.irods_collection.drop_collection()
    
    def glob(self, patterns):
        if (self.fs_underlying_storage):
            return self.fs_underlying_storage.glob(patterns)
        return self.irods_collection.glob(patterns)


def irods_storage_engine_factory(exp, e_config: configuration.JobConfigWrapper, logger, module_config: configuration.LimsModuleConfigWrapper, engine: str=None):
    conf: dict = module_config.get(engine or exp.storage.engine)
    if not conf:
        return None
    
    return IrodsExperimentStorageEngine(
        exp, logger, e_config.data_rules, e_config.metadata["model"],

        collection_base=conf["base_path"], 
        metadata_target=e_config.metadata["target"],
        connection=conf["connection"],
        mount_point=conf.get("mount_point", None) 
    )


# If we are running as main script...

if __name__ == "__main__":
    import sys
    conffile = pathlib.Path(sys.argv[1])
    key = sys.argv[2]

    connection = yaml.safe_load(conffile.open("r"))[key]  
    # Now quickly test that irods zone is working 
    irods_session = iRODSSession(**connection)
    print(irods_session.pam_pw_negotiated)


