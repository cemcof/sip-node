import logging
import pathlib
import experiment
import tempfile, tifffile, mrcfile, numpy as np, os

def module(lmod_path, command, *arguments):
  # /usr/share/lmod/lmod/libexec/lmod
  commands = os.popen('%s python %s %s'\
                      % (lmod_path, command, ' '.join(arguments))).read()
  exec(commands)


class GainRefConverter:
    """ Given path to gain file, can convert it to different format. Puts the result into the same directory as the source file. """
    def __init__(self, gain_file: pathlib.Path, lmodule=None) -> None:
        """ lmodule is a tuple (path_to_lmod, module_name) of lmod module that must be loaded so the conversion software is available """
        self.gain_file = gain_file
        self.lmodule = lmodule

    def convert_to_mrc(self):

        suffix_map = {
            ".mrc": lambda x,y: self.gain_file,
            ".dm4": self.dm4_to_mrc,
            ".gain": self.eer_to_mrc
        }

        suffix = self.gain_file.suffix
        if suffix not in suffix_map:
            raise ValueError(f"Unsupported gain file format {suffix}")
        
        # Execute the conversion and return path to the file
        out_file = self.gain_file.with_suffix(".mrc")
        suffix_map[suffix](self.gain_file, out_file)
        return out_file

    def dm4_to_mrc(self, in_file: pathlib.Path, out_file: pathlib.Path):
        if not self.lmodule:
            raise ValueError("lmod is required to convert dm4 to mrc, no lmod config given")
        
        module(self.lmodule[0], 'load', self.lmodule[1])
        os.system('dm2mrc %s %s' % (in_file,out_file))
        module(self.lmodule[0], 'unload', self.lmodule[1])

    def eer_to_mrc(self, in_file: pathlib.Path, out_file: pathlib.Path):
        Iref = tifffile.imread(in_file)
        outData  = np.flip(np.reciprocal(Iref),0)
        with mrcfile.new(out_file) as mrc:
            mrc.set_data(np.array(outData,dtype=np.float32))


class EmMoviesHandler:
    def __init__(self, storage_engine: experiment.ExperimentStorageEngine) -> None:
        self.storage_engine = storage_engine
        self.logger = logging.getLogger("EmMoviesHandler")
    
    def extract_value_from_meta_content(meta_content: str, meta_type: str, key: str):
        if meta_type == "mdoc":
            SPLIT_VALUE_POSITION = 2 
            # Parse .mdoc file 
            for line in meta_content.splitlines():
                spl = line.split()
                if len(spl) >= SPLIT_VALUE_POSITION and spl[0] == key:
                    return spl[SPLIT_VALUE_POSITION]
            return None
                    
        if meta_type == "xml":
            # TODO - extract value from xml metadata file 
            raise NotImplementedError()
        
        raise ValueError(f"Unsupported metadata file type {meta_type}")

    def extract_value_from_metafile(self, metafile_path: pathlib.Path, key: str):
        return self.extract_value_from_meta_content(metafile_path.read_text(), metafile_path.suffix[1:], key)
    

    def find_gain_reference(self, metafile_path: pathlib.Path):
        meta = self.storage_engine.file_exists(metafile_path) and self.storage_engine.read_file(metafile_path)
        if meta:
            gain_ref = self.extract_value_from_meta_content(meta, metafile_path.suffix[1:], "GainReference")
            return gain_ref
            

    def convert_gain_reference(self, gain_ref: pathlib.Path):
        """ Converts given gain reference to supported format and transfers it into target location in the storage 
            Returns: path to the converted target gain file """
        if not self.storage_engine.file_exists(gain_ref):
            raise ValueError(f"Gain reference file {gain_ref} does not exist in the storage")
        
        gain_ref_target = gain_ref.parent / (gain_ref.stem + ".mrc")
        if self.storage_engine.file_exists(gain_ref_target):
            return gain_ref_target

        with tempfile.TemporaryDirectory() as td:
            # First, copy from storage to temporary storage 
            tmp_srcgain = pathlib.Path(td) / gain_ref.name
            self.storage_engine.get_file(gain_ref, tmp_srcgain)
            # Convert it
            # lmod_config = self.storage_engine.config["Lmod"] TODO 
            converted_gain_path = GainRefConverter(tmp_srcgain, None).convert_to_mrc()
            self.storage_engine.put_file(gain_ref_target, converted_gain_path, skip_if_exists=True)

        return gain_ref_target



    def find_movie_information(self):
        """ There are several supported data and metadata file types for movies/micrographs
            This information is necessary before creating scipion project and scheduling processing on it
            This method tries to extract this information by scanning raw data file of this experiment 
            
            Returns: tuple (movie file path, metadata file path, path to gain file) or None if no raw movie data found"""

        # Get movie file path
        movie_datarule: experiment.DataRuleWrapper = self.storage_engine.e_config.data_rules.with_tags("movie", "raw").data_rules[0]
        first_movie = next(self.storage_engine.glob(movie_datarule.get_target_patterns()), None)
        
        self.logger.debug(f"First movie: {first_movie}")
        if not first_movie:
            return None
        
        # Get metadata file path
        moviemeta_data_rule: experiment.DataRuleWrapper = self.storage_engine.e_config.data_rules.with_tags("movie_metafile", "raw").data_rules[0]
        first_meta = next(self.storage_engine.glob(moviemeta_data_rule.get_target_patterns()), None)
        self.logger.debug(f"First meta: {first_meta}")

        # Now gain file
        gain_file_rule = next(iter(self.storage_engine.e_config.data_rules.with_tags("gain", "raw")), None)
        if gain_file_rule:
            gain_ref = next(self.storage_engine.glob(gain_file_rule.get_target_patterns()), None)
            self.logger.debug(f"Gain ref: {gain_ref}")
        else:
            gain_ref = None

        return (first_movie, first_meta, gain_ref)
    
class WorkflowWrapper:
    def __init__(self, workflow: list) -> None:
        self.workflow = workflow

    def find(self, key: str, default=ValueError):
        for prot in self.workflow:
            if key in prot:
                return prot[key]
            
        if isinstance(default, Exception):
            raise default(f"Key {key} not found in workflow")
        
        return default