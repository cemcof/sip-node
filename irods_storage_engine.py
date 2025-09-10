import base64
import pathlib, yaml, datetime, logging

from irods.keywords import FORCE_CHKSUM_KW

import common
from data_tools import TransferCondition
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
        return time_delta_secs, size
    
    def exists(self,relative_path: pathlib.Path):
        return self.irods_session.data_objects.exists(str(self.collection_path / relative_path))

    def stat(self, path_relative: pathlib.Path):
        dataobj = self.irods_session.data_objects.get(str(self.collection_path / path_relative))
        return dataobj.size, dataobj.modify_time.timestamp()

    def open_dataobject(self, path, mode="r+"):
        pth = self.collection_path / path
        exists = self.irods_session.data_objects.exists(str(pth))
        if not exists:
            self.irods_session.data_objects.create(str(pth))

        experiment_dataobj = self.irods_session.data_objects.get(str(pth)) 
        return experiment_dataobj.open(mode)

    def get_dataobject(self, path_relative: pathlib.Path):
        pth = self.collection_path / path_relative
        return self.irods_session.data_objects.get(str(pth))
    
    def generate_ticket(self, permission='read'):
        return Ticket(self.irods_session).issue(permission, self.collection_path).string

    def read_file(self, path_relative: pathlib.Path, as_text=True):
        dataobj = self.irods_session.data_objects.get(str(self.collection_path / path_relative))
        with dataobj.open('r') as file: 
            data = file.read()
            return data if not as_text else str(data, "utf-8")
        
    def get_file(self, path_relative_src: pathlib.Path, path_dst: pathlib.Path):
        return self.irods_session.data_objects.get(str(self.collection_path / path_relative_src), str(path_dst), forceFlag=True)
    
    def unlink_file(self, path_relative: pathlib.Path):
        self.irods_session.data_objects.unlink(str(self.collection_path / path_relative))

    def write_file(self, path_relative: pathlib.Path, content):
        with self.open_dataobject(path_relative, "w") as f:
            f.write(content if isinstance(content, bytes) else str.encode(content))
        # dataobj = self.irods_session.data_objects.get(str(self.collection_path / path_relative))
        # with dataobj.open('w') as file: 
        #     file.write(content)
            
    def walk(self):
        def walk_collection(collection: iRODSCollection):
            for subcol in collection.subcollections:
                yield from walk_collection(subcol) # Recurse into subcollections
            
            for dobj in collection.data_objects:
                yield dobj

        yield from walk_collection(self.collection)

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
                imeta = iRODSMeta(met_name, str(met_value), '')
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
            collection_path=self.collection_base / self.exp.storage.subpath,
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
        if self.fs_underlying_storage:
            return self.fs_underlying_storage.resolve_target_location(src_relative)
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
            "Path": str(self.collection_base),
            "Token": self.irods_collection.generate_ticket()
        }
    
    def prepare(self):
        self.irods_collection.ensure_exists()

    def read_file(self, path_relative: pathlib.Path, as_text=True):
        if self.fs_underlying_storage:
            return self.fs_underlying_storage.read_file(path_relative, as_text)
        return self.irods_collection.read_file(path_relative, as_text)
    
    def write_file(self, path_relative: pathlib.Path, content):
        if self.fs_underlying_storage:
            self.fs_underlying_storage.write_file(path_relative, content)
        else:
            self.irods_collection.ensure_exists(self.irods_collection.collection_path / path_relative.parent)
            self.irods_collection.write_file(path_relative, content)
    
    def put_file(self, path_relative: pathlib.Path, src_file: pathlib.Path, condition: TransferCondition = TransferCondition.IF_MISSING):
        if self.fs_underlying_storage:
            return self.fs_underlying_storage.put_file(path_relative, src_file, condition)
        return self.irods_collection.ensure_file(src_file, path_relative, replace=condition != TransferCondition.IF_MISSING)
        
    def get_file(self, path_relative_src: pathlib.Path, path_dst: pathlib.Path):
        if self.fs_underlying_storage:
            return self.fs_underlying_storage.get_file(path_relative_src, path_dst)
        return self.irods_collection.get_file(path_relative_src, path_dst)

    def file_exists(self, path_relative: pathlib.Path):
        if self.fs_underlying_storage:
            return self.fs_underlying_storage.file_exists(path_relative)
        return self.irods_collection.exists(path_relative)
    
    def del_file(self, path_relative: pathlib.Path):
        if self.fs_underlying_storage:
            return self.fs_underlying_storage.del_file(path_relative)
        
        self.irods_collection.unlink_file(path_relative)

    def purge(self):
        self.irods_collection.drop_collection()
    
    def glob(self, data_rules: data_tools.DataRulesWrapper=None):
        if self.fs_underlying_storage:
            return self.fs_underlying_storage.glob(data_rules)
        files = { pathlib.Path(dobject.path).relative_to(self.irods_collection.collection_path) : dobject for dobject in self.irods_collection.walk() }
        for f, dr in data_rules.match_files(files.keys()):
            yield f, dr, files[f].modify_time.timestamp(), files[f].size # TODO - maybe old meta values for longer processing

    def stat(self, path_relative: pathlib.Path):
        if self.fs_underlying_storage:
            return self.fs_underlying_storage.stat(path_relative)
        return self.irods_collection.stat(path_relative)

    def supported_checksums(self):
        return frozenset({'sha256'})

    def checksum(self, path_relative: pathlib.Path, sumtype: str):
        if self.fs_underlying_storage:
            return self.fs_underlying_storage.checksum(path_relative, sumtype)

        if sumtype not in self.supported_checksums():
            raise ValueError(f"Checksum type {sumtype} not supported by iRODS")

        dataobj = self.irods_collection.get_dataobject(path_relative)
        sum = dataobj.chksum(FORCE_CHKSUM_KW='')
        prefix, basehash = sum[0:5], sum[5:]
        if prefix == 'sha2:':
            # To hex
            raw_hash = base64.b64decode(basehash)
            return raw_hash.hex()
        else:
            raise ValueError(f"Invalid checksum format: {sum}")
        
    def is_same(self, other):
        return isinstance(other, IrodsExperimentStorageEngine) and self.irods_collection.collection_path == other.irods_collection.collection_path

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

    connection = yaml.safe_load(conffile.open("r"))
    if 2 in sys.argv:
        connection = connection[sys.argv[2]] 
    # Now quickly test that irods zone is working 
    sess = iRODSSession(**connection)
    home = pathlib.Path("/") / connection["zone"] / "home" / connection["user"]

    def cols(path: pathlib.Path=home, recur=False, pref=""):
        c = sess.collections.get(str(path))
        for sc in c.subcollections:
            print(pref + sc.name)
            if recur:
                cols(sc.path, recur, pref + "  ")

    def col(path: pathlib.Path=""):
        c = sess.collections.get(str(home / path))
        return c
    
    def colw(path: pathlib.Path=""):
        return IrodsCollectionWrapper(sess, home / path)
    
    def colc(path: pathlib.Path=""):
        sess.collections.create(str(home / path))
        return col(path)
    
    def down(colw, target):
        target = pathlib.Path(target)
        patts = ["*"]
        for src, _, _ in colw.glob(patts):
            tar = target / src
            tar.parent.mkdir(parents=True, exist_ok=True)
            colw.get_file(src, target / src)
            print("Downloaded", src, "to", tar)

    def checksum(path: pathlib.Path):
        dataobj = sess.data_objects.get(str(home / path))
        print(dataobj.chksum({ FORCE_CHKSUM_KW: '' }))

    sess.pam_pw_negotiated



