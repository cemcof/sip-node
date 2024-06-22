import logging
import pathlib
import experiment
import typing
from common import lmod_getenv
import functools, subprocess
import tempfile, tifffile, mrcfile, numpy as np, os

class GainRefConverter:
    """ Given path to gain file, can convert it to different format. Puts the result into the same directory as the source file. """
    def __init__(self, gain_file: pathlib.Path, imod_env_provider: dict=None) -> None:
        """ lmodule is a tuple (path_to_lmod, module_name) of lmod module that must be loaded so the conversion software is available """
        self.gain_file = gain_file

        if isinstance(imod_env_provider, dict):
            # Do we have env directly? 
            if "env" in imod_env_provider:
                self.imod_env_provider = lambda: self.imod_env_provider["env"]
            else:
                # Try lmod module manager
                self.imod_env_provider = functools.partial(lmod_getenv, imod_env_provider["lmod_path"], imod_env_provider["module"])
        elif callable(imod_env_provider):
            self.imod_env_provider = imod_env_provider
        else:
            def no_imod_env_provider():
                raise ValueError("No imod environment provider given")
            self.imod_env_provider = no_imod_env_provider


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
        env = self.imod_env_provider()
        subprocess.run(["dm2mrc", in_file, out_file], env=env, stderr=subprocess.PIPE, check=True)  

    def eer_to_mrc(self, in_file: pathlib.Path, out_file: pathlib.Path):
        Iref = tifffile.imread(in_file)
        outData  = np.flip(np.reciprocal(Iref),0)
        with mrcfile.new(out_file) as mrc:
            mrc.set_data(np.array(outData,dtype=np.float32))


class EmMoviesHandler:
    def __init__(self, storage_engine: experiment.ExperimentStorageEngine, imod_config: dict=None) -> None:
        self.storage_engine = storage_engine
        self.logger = logging.getLogger("EmMoviesHandler")
        self.imod_config = imod_config  
    
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

        try:
            with tempfile.TemporaryDirectory() as td:
                # First, copy from storage to temporary storage 
                tmp_srcgain = pathlib.Path(td) / gain_ref.name
                self.storage_engine.get_file(gain_ref, tmp_srcgain)
                # Convert it
                # lmod_config = self.storage_engine.config["Lmod"] TODO 
                converted_gain_path = GainRefConverter(tmp_srcgain, self.imod_config).convert_to_mrc()
                self.storage_engine.put_file(gain_ref_target, converted_gain_path, skip_if_exists=True)
        except subprocess.CalledProcessError as e:
            raise ValueError(f"Error during gain reference conversion: {e} {e.stderr}")
        return gain_ref_target



    def find_movie_information(self):
        """ There are several supported data and metadata file types for movies/micrographs
            This information is necessary before creating scipion project and scheduling processing on it
            This method tries to extract this information by scanning raw data file of this experiment 
            
            Returns: tuple (movie file path, metadata file path, path to gain file) or None if no raw movie data found"""

        # Get movie file path
        movie_datarule: experiment.DataRule = self.storage_engine.data_rules.with_tags("movie", "raw").data_rules[0]
        first_movie = next(self.storage_engine.glob(movie_datarule.get_target_patterns()), None)
        
        self.logger.debug(f"First movie: {first_movie}")
        if not first_movie:
            return None
        
        first_movie = first_movie[0] # Only path component
        
        # Get metadata file path
        moviemeta_data_rule: experiment.DataRule = self.storage_engine.data_rules.with_tags("movie_metafile", "raw").data_rules[0]
        first_meta = next(self.storage_engine.glob(moviemeta_data_rule.get_target_patterns()), None)
        if first_meta:
            first_meta = first_meta[0] # Only path component  
        self.logger.debug(f"First meta: {first_meta}")

        # Now gain file
        gain_file_rule = next(iter(self.storage_engine.data_rules.with_tags("gain", "raw")), None)
        if gain_file_rule:
            gain_ref = next(self.storage_engine.glob(gain_file_rule.get_target_patterns()), None)
            if gain_ref:
                gain_ref = gain_ref[0] # Only path component
            self.logger.debug(f"Gain ref: {gain_ref}")
        else:
            gain_ref = None

        return (first_movie, first_meta, gain_ref)
    
    def set_importmovie_info(self, workflow: list, processing_source_path: pathlib.Path):
        movies_info = self.find_movie_information()
        
        if not movies_info:
            return None # There is no movie for the experiment - not ready to create the project, not enough information
                
        print(movies_info)
        for prot in filter(lambda x: x["TYPE"] == "ProtImportMovies", workflow):
            # 1) Path to the source files
            path_to_movies_relative : pathlib.Path = self.storage_engine.data_rules.with_tags("movie", "raw").data_rules[0].target
            prot["filesPath"] = str(processing_source_path / path_to_movies_relative) 
            # 2) Pattern of the source files and movie suffix
            movie_path = movies_info[0]
            prot["filesPattern"] = f"*{movie_path.suffix}"
            prot["movieSuffix"] = movie_path.suffix
            # 3) Gain file, if any
            if movies_info[2]:
                gain_reference = self.convert_gain_reference(movies_info[2])
                # Set reference to the gainfile for scipion
                prot["gainFile"] = str(processing_source_path / gain_reference)

        return movies_info
    
    
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
    
class ProcessingBase:
    def __init__(self) -> None:
        pass