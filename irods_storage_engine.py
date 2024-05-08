
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
            self.logger.info(f"Created new iRODS collection: {str(col)}")

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
        size_fmt = common.sizeof_fmt(source.stat().st_size)
        t_start = datetime.datetime.utcnow()
        self.irods_session.data_objects.put(str(source), str(target))
        t_done = datetime.datetime.utcnow()
        time_delta_secs = (t_done - t_start).total_seconds()
        self.logger.info(f"Transfered to iRODS: {str(source_relative or source_base.name)} -> {target_relative}, {size_fmt}, {time_delta_secs:.3f}s")
        return True
    
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

    def __init__(self, experiment: ExperimentWrapper, e_config: configuration.JobConfigWrapper, logger: logging.Logger, config: configuration.LimsModuleConfigWrapper) -> None:
        super().__init__(experiment, e_config, logger, config)

        self.irods_collection = IrodsCollectionWrapper(
            irods_session=iRODSSession(**self.config["Irods"]["Connection"]), 
            collection_path=pathlib.Path(self.config["Irods"]["base_path"]) / self.exp.secondary_id, 
            logger=self.logger)
        
        self.mount_point = pathlib.Path(self.config["Irods"]["mount_point"]) if "mount_point" in self.config["Irods"] else None
        # Once refactoring, create FsStorageService and use it when mount point is available

    def resolve_target_location(self, src_relative: pathlib.Path = None) -> pathlib.Path:
        if self.mount_point:
            return self.mount_point / self.exp.secondary_id / (src_relative or "")
        return None
    
    def restore_metadata(self, metadata={}):
        meta = super().restore_metadata(metadata)
        if meta: 
            self.irods_collection.store_irods_metadata(target_file=self.metadata_target, metadata=meta)

    def is_accessible(self):
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
        return self.irods_collection.read_file(path_relative, as_text)
    
    def write_file(self, path_relative: pathlib.Path, content):
        self.irods_collection.ensure_exists(self.irods_collection.collection_path / path_relative.parent)
        self.irods_collection.write_file(path_relative, content)
    
    def put_file(self, path_relative: pathlib.Path, src_file: pathlib.Path, skip_if_exists=True):
        return self.irods_collection.ensure_file(src_file, path_relative, replace=not skip_if_exists)
        
    def get_file(self, path_relative_src: pathlib.Path, path_dst: pathlib.Path):
        return self.irods_collection.get_file(path_relative_src, path_dst)

    def file_exists(self, path_relative: pathlib.Path):
        return self.irods_collection.exists(path_relative)
    
    def purge(self):
        self.irods_collection.drop_collection()
    
    def glob(self, patterns):
        return self.irods_collection.glob(patterns)


# If we are running as main script...

if __name__ == "__main__":
    import sys
    conffile = pathlib.Path(sys.argv[1])
    key = sys.argv[2]

    connection = yaml.safe_load(conffile.open("r"))[key]  
    # Now quickly test that irods zone is working 
    irods_session = iRODSSession(**connection)
    print(irods_session.pam_pw_negotiated)


