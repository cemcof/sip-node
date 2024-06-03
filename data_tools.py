""" Tools and utilities for sniffing files, transfering files or continually reading files while writing to them from another process """

import datetime
import logging
import os
import pathlib
import time
import shutil
import typing

import yaml
import common

class DataRuleWrapper:
    def __init__(self, patterns: typing.List[str], tags: typing.List[str], target: str = None, keep_tree: bool = False, skip_if_exists=False) -> None:
        self.patterns = patterns
        self.tags = tags
        self.target = pathlib.Path(target) if target else None
        self.keep_tree = keep_tree
        self.skip_if_exists = skip_if_exists

    def translate_to_target(self, path_relative: pathlib.Path):
        if self.target:
            return self.target / path_relative if self.keep_tree else self.target / path_relative.name
        return path_relative
    
    def get_target_patterns(self):
        """ Gets glob pattern through which files for this rule can be searched in the target location """
        target_base = self.target if not self.keep_tree else self.target / "**"
        return [str(target_base / pathlib.Path(p).name) for p in self.patterns]
    
    def __str__(self) -> str:
        return f"DataRuleWrapper({self.patterns}, {self.tags}, {self.target}, {self.keep_tree}, {self.skip_if_exists})"

class DataRulesWrapper:
    def __init__(self, data_rules: list) -> None:
        # Data_rules arg is a list that can contain both dicts or datarulewrapper objects
        # Create self.data_rules where all items are DataRuleWrapper objects
        self.data_rules : typing.List[DataRuleWrapper] = []
        for dr in data_rules:
            if isinstance(dr, DataRuleWrapper):
                self.data_rules.append(dr)
            else:
                self.data_rules.append(DataRuleWrapper(dr["Patterns"], dr["Tags"], dr["Target"] if "Target" in dr else None, dr["KeepTree"] if "KeepTree" in dr else False, dr["SkipIfExists"] if "SkipIfExists" in dr else False))

    def with_tags(self, *tags) -> 'DataRulesWrapper':
        # Filter current data rules by tag and return new dataruleswrapper object
        return DataRulesWrapper(list(filter(lambda x: set(tags).issubset(x.tags), self.data_rules)))
    
    def __iter__(self):
        return iter(self.data_rules)

class DataRulesSniffer:
    def __init__(self, path: pathlib.Path, data_rules: DataRulesWrapper, consumer, metafile: pathlib.Path = None, min_nochange_sec=10, reconsume_on_change=True) -> None:
        self.path = path
        self.data_rules = data_rules
        self.consumer = consumer
        self.metafile = metafile
        self.min_nochange_sec = min_nochange_sec
        self.reconsume_on_change = reconsume_on_change

    def _load_metafile(self):
        if self.metafile and self.metafile.exists():
            with self.metafile.open("r") as metafile:
                return yaml.full_load(metafile)
        return None
    
    def should_exclude(self, path: pathlib.Path):
        return path.name.startswith("_") 

    def sniff_and_consume(self):
        consumation_start = time.time()
        meta = self._load_metafile() or {}
        metafile_append = self.metafile.open("a") if self.metafile else None

        for data_rule in self.data_rules:
            for f in common.multiglob(self.path, data_rule.patterns):
                if self.should_exclude(f):
                    continue
                time_change = f.stat().st_mtime
                consumed_time = meta.get(str(f), None)
                now = time.time()

                is_ready = (now - time_change) > self.min_nochange_sec
                should_consume = consumed_time is None or (self.reconsume_on_change and consumed_time < time_change)

                if is_ready and should_consume:
                    try:
                        self.consumer(f, data_rule)
                        # Consumation succeeded, mark this file consumed
                        meta[str(f)] = now
                        if metafile_append:
                            metafile_append.write(f"{str(f)}: {now}\n")
                    except Exception as e:
                        # Consumation failed - TODO - implement some error handling strategy
                        print(f"Consumation of {f} failed", file=sys.stderr)
                        pass
        metafile_append.close()
        # Return list of tuples of consumed files (only new ones in this sniff run) and their consumation times, sorted by the time
        filtered_new = filter(lambda x: x[1] > consumation_start, meta.items())
        return sorted(filtered_new, key=lambda x: x[1])



class DataSyncUnit:
    def __init__(self, src_path : pathlib.Path, dst_path : pathlib.Path, logger):
        self.src_path = src_path
        self.dst_path = dst_path
        self.logger = logger
        self.transfer_time_secs = -1

    def is_transfer_ready(self, last_modified_max_timestamp):
        exists = self.src_path.exists()
        if not exists:
            return False
        modified = self.src_path.stat().st_mtime
        return modified < last_modified_max_timestamp and (not self.dst_path.exists() or self.dst_path.stat().st_mtime < modified)

    def transfer(self, remove_source=False):
        if not self.dst_path.parent.exists():
            self.dst_path.parent.mkdir(parents=True, exist_ok=True)
        self.transfer_time_secs = -1    
        tstart = time.time()
        shutil.copy(self.src_path, self.dst_path)
        if remove_source:
            self.src_path.unlink(missing_ok=True)

        self.transfer_time_secs = time.time() - tstart

    
class DirectorySniffer:
    def __init__(self, path: pathlib.Path, patterns, min_nochange_secs=15) -> None:
        self._path = path
        self.total_sniff_count = 0
        self.patterns = patterns
        self.min_nochange_secs = min_nochange_secs

    def sniff(self):
        self.total_sniff_count = self.total_sniff_count + 1
        for patt in self.patterns:
            fpaths = self._path.glob(patt)
            for pth in fpaths:
                if pth.is_file() and (time.time() - self._path.stat().st_mtime) >= self.min_nochange_secs:
                    yield pth

class DataSniffer:
    def __init__(self, sniffer, consumer) -> None:
        pass

class DataTransferer:
    """
    A class capable of transfering files from one filesystem location to another.
    The transfer is supposed to be executed in intervals. 
    Each interval execution observes the source directory and copies files that are ready.
    A file is considered ready when for specified period of time (min_nochange_sec) it was not modified.
    This allows this transferer to handle files that are still being written to by the source. 
    """
    def __init__(self, src_path: pathlib.Path, dst_path: pathlib.Path, patterns: list, logger, 
                 remove_source=False, min_nochange_secs=15, done_after_no_source_secs=30,
                 target_resolver=None):
        if src_path == dst_path:
            raise ValueError(f"DataSynchronizer: src_path and dst_path must not be the same")
        
        def default_target_resolver(src_path_relative: pathlib.Path):
            return self.dst_path / src_path_relative
        
        self.target_resolver = target_resolver or default_target_resolver
        self.src_path = src_path
        self.dst_path = dst_path
        self.patterns = patterns
        self.logger = logger
        self.remove_source = remove_source
        self.min_nochange_secs = min_nochange_secs
        self.dt_last_file_transfered = None
        self.done_after_no_source_secs = done_after_no_source_secs
        self.total_sniff_count = 0
        self.successful_transfers = []

        # Ensure base paths exist
        src_path.mkdir(parents=True, exist_ok=True)

        if dst_path:
            dst_path.mkdir(parents=True, exist_ok=True)

    def sniff(self):
        self.total_sniff_count = self.total_sniff_count + 1
        for patt in self.patterns:
            fpaths = self.src_path.glob(patt)
            for pth in fpaths:
                if pth.is_file():
                    rel = pth.relative_to(self.src_path)
                    dst = self.target_resolver(rel)
                    yield DataSyncUnit(pth, dst, logger=self.logger)

    def transfer(self):
        max_modified = time.time() - self.min_nochange_secs
        total_files = 0
        total_size = 0
        for fileinfo in self.sniff():
            if fileinfo.is_transfer_ready(max_modified):
                fileinfo.transfer(self.remove_source)
                self.dt_last_file_transfered = datetime.datetime.utcnow()
                self.successful_transfers.append(fileinfo)
                fsize = fileinfo.dst_path.stat().st_size
                total_size = total_size + fsize
                total_files = total_files + 1
                self.logger.info(f"Transfered {common.sizeof_fmt(fsize)} file {fileinfo.src_path} to {fileinfo.dst_path} in {fileinfo.transfer_time_secs:.3f}s")

        return self.is_finished()

    def is_finished(self):
        return False # TODO  return datetime.datetime.utcnow() - self.dt_last_file_transfered 


class DataTransferSimulator:
    """ Copies a directory tree from src to dst, simulating a data transfer 
        Copies files from oldest to newest, sleeping for the difference between modification times.
    """
    def __init__(self, src: pathlib.Path, dst: pathlib.Path, logger: logging.Logger) -> None:
        self.src = src
        self.dst = dst
        self.logger = logger

        self.filelist = list(src.glob("**/*.*"))
        self.filelist.sort(key=lambda x: x.stat().st_mtime)

    def transfer(self):
        last_time = self.filelist[0].stat().st_mtime
        current_index = 0
        for f in self.filelist:
            # Sleep for the difference between modification times
            sleep_time = f.stat().st_mtime - last_time
            self.logger.info(f"Next sleep time {sleep_time:.3f}s")
            time.sleep(sleep_time)
            last_time = f.stat().st_mtime

            rel = f.relative_to(self.src)
            dst = self.dst / rel
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(f, dst)
            current_index = current_index + 1
            self.logger.info(f"[{current_index}/{len(self.filelist)} ({(current_index/len(self.filelist)):.2%})] Transfered {f} to {dst}")


# Basic test
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger("transfer_simulator")

    import sys
    src = sys.argv[1]
    dst = sys.argv[2]
    logger.info(f"Creating simulator for {src} -> {dst}")
    simulator = DataTransferSimulator(pathlib.Path(src), pathlib.Path(dst), logger)
    simulator.transfer()
    

""" Used by filebrowser, refactor to pathlib """
def map_direntry(direntry):
    return {
        "Path": os.path.join(direntry.path, "") if direntry.is_dir() else direntry.path,
        "Name": direntry.name,
        "IsDirectory": direntry.is_dir()
    }


def map_direntry_from_config(direntry):
    return {
        "Path": direntry["Path"],
        "Name": direntry["Name"] if "Name" in direntry else None,
        "IsDirectory": True
    }

def sort_file_items(items: list):
    # First process and sort these starting by a number, sort them by modification time, from newest to oldest
    numbered = sorted(filter(lambda x: x["Name"][0].isdigit(), items), key=lambda x: os.path.getmtime(x["Path"]), reverse=True)
    # Then process and sort these starting by a letter, sort them by name, from A to Z
    lettered = sorted(filter(lambda x: not x["Name"][0].isdigit(), items), key=lambda x: x["Name"])
    
    return numbered + lettered
    
def list_directory(path: str, roots: list, logger: logging.Logger):
    dirn = os.path.join(os.path.dirname(path), "")
    parentdirn = os.path.join(os.path.dirname(os.path.dirname(path)), "")
    logger.debug(f"Processing directory: {path}, parentdir is {parentdirn}, dirname is {dirn}")

    # If requested directory does not start with any configured root paths, return rootpaths themselves.
    root = [p for p in roots if dirn.startswith(p["Path"])]
    if not root:
        return list(map(map_direntry_from_config, roots))

    result = []
    # Otherwise, scan the directory
    if root[0] != dirn:  # Add parent and current directory entry
        result.append({"Path": parentdirn, "Name": "..", "IsDirectory": True})
        result.append({"Path": dirn, "Name": ".", "IsDirectory": True})

    logger.debug("Before os.scandir.")
    tm = time.time()
    try:
        with os.scandir(dirn) as dir_entries:
            result.extend(sort_file_items(list(map(map_direntry, dir_entries))))
    except FileNotFoundError:
        return list(map(map_direntry_from_config, roots))
    logger.debug(f"After os.scandir. Took {time.time() - tm}s.")
    logger.debug(f"Scanned directory {dirn}, resulted: \n" +
                  '\n'.join(f"{p['Path']: <35} - {p['Name']: <25}  {p['IsDirectory']}" for p in result))

    return result



# ========== LOG SNIFFFING TOOLS ==================

class FileWatcher:
    def __init__(self, file_path: pathlib.Path, name=None, file_status_path: pathlib.Path=None, file_timeout=0) -> None:
        self.file_path = file_path
        self.name = name or self.file_path.name
        self.file_timeout = file_timeout
        self.file_status_path = file_status_path or file_path.parent / ("_tmpfilewatch_" + file_path.name + ".tmp")

    def get_current_position(self):
        try:
            return int(self.file_status_path.read_text())
        except:
            return 0
        
    def save_current_position(self, position: int):
        self.file_status_path.write_text(str(position))

    def has_changed_since_last_mark(self):
        if not self.file_path.exists():
            return False
        
        if not self.file_status_path.exists():
            return True
        
        return self.file_path.stat().st_mtime > self.file_status_path.stat().st_mtime
    
    def mark_processed(self):
        self.file_status_path.touch()

    def read_new_text_lines(self):
        with self.file_path.open() as file:
            file.seek(self.get_current_position())
            lines = file.readlines()
            self.save_current_position(file.tell())
            return lines

    def read_new_text(self):
        with self.file_path.open() as file:
            file.seek(self.get_current_position())
            text = file.read()
            self.save_current_position(file.tell())
            return text

class FilesWatcher:
    def __init__(self, fileWatchers: typing.List[FileWatcher], consumer) -> None:
        self.watchers = fileWatchers
        self.consumer = consumer

    def get_changed_paths(self, mark=True):
        for w in self.watchers:
            if w.has_changed_since_last_mark():
                if mark:
                    w.mark_processed()
                yield w.file_path

    def get_changed(self):
        for w in self.watchers:
            if w.has_changed_since_last_mark():
                yield w

    def consume_new_data(self):
        """ For all watchers, checks if there is new data and passes it to the consumer as whole """
        for w in self.get_changed():
            data = w.read_new_text()
            if data:
                self.consumer(w, data)
            w.mark_processed()
        

    def consume_new_lines(self):
        for w in self.get_changed():
            lines = w.read_new_text_lines()
            if lines:
                self.consumer(w, lines)
            w.mark_processed()
        

class LogSnifferSource:
    
    def read_new_logs(self) -> typing.List[str]:
        raise NotImplementedError()

class FileLogSnifferSource(FileWatcher, LogSnifferSource):
    def __init__(self, file_path: pathlib.Path, name=None, file_status_path: pathlib.Path=None) -> None:
        FileWatcher.__init__(self, file_path, file_status_path)
        LogSnifferSource.__init__(self)
        self.name = name or file_path.name
    
    def get_level(self, line: str):
        # By default, use INFO
        level = logging.INFO

        if self.file_path.name.endswith(".stderr") or "error" in line.lower():
            level = logging.ERROR

        return level

    def merge_lines(self, lines: typing.List[str]):
        """ Kinda stupid strategy for merging lines, but at least something... """
        tmpres = ""
        if not lines:
            return [] 
        
        for line in lines:
            # If line does not start with a whitespace character nor ends in a dot, consider it as 
            # a part of previous message and start a new message
            if not line.startswith(" ") and not line.endswith("."):
                yield tmpres + "\n" + line
                tmpres = ""
            else:
                tmpres += "\n" + line

        yield tmpres

    def read_new_logs(self) -> typing.List[logging.LogRecord]:
        """ Read new logs from file, return list of LogRecord 
            TODO: Be more intelligent about building messages - if there are multiple lines starting with whitespace, group them together
        """
        logmessages = self.merge_lines(self.read_new_text_lines())
        return [logging.LogRecord(self.name, self.get_level(line), str(self.file_path), 0, line, None, None) for line in logmessages]
        


class LogSnifferCompositeSource(LogSnifferSource):
    """ Composite source of logs from multiple sources
        Reads log from multiple sources but unites them under one name  
    """
    def __init__(self, name, sources: typing.List[LogSnifferSource]) -> None:
        self.sources = sources
        self.name = name

    def read_new_logs(self) -> typing.List[logging.LogRecord]:
        """ Read new logs from file, return list of LogRecord """
        logs = [l for s in self.sources for l in s.read_new_logs()]   
        for log in logs:
            log.name = self.name
        return logs
    

class LogSniffer:
    """ Sniff logs from sources and send them to consumer """
    def __init__(self, consumer: logging.Logger, sources: typing.List[LogSnifferSource]) -> None:
        self.consumer = consumer    
        self.sources = sources

    def sniff_and_consume(self):
        """ Read new logs from sources and send them to consumer, meant to be called periodically """
        for source in self.sources:
            logs = source.read_new_logs()
            for l in logs:
                self.consumer.log(l.levelno, l.msg, extra={"origin": l.name})

        
def log_sniffer_file_source_factory(directory: pathlib.Path, globs_relative: typing.List[str]):
    return [FileLogSnifferSource(p, str(p.relative_to(directory))) for globrl in globs_relative for p in directory.glob(globrl)]
            


class MetadataModel:
    def __init__(self, model: dict) -> None:
        self.model = model

    def get_metakey_info(self, key: str):
        if isinstance(self.model[key], dict):
            default = self.model[key].get("Default", None)
            sources = self.model[key].get("Sources", [])
            unit = self.model[key].get("Unit", None)
            return sources, default, unit
        
        if isinstance(self.model[key], list):
            return self.model[key], None, None
        
        if isinstance(self.model[key], str):
            return [self.model[key]], None, None
        
        return None, None, None


    def extract_metadata(self, source_handlers: dict):
        """ Extract metadata from sources """

        for k in self.model:
            sources, default, unit = self.get_metakey_info(k)
            result = None
            if sources is not None:
                for s in sources:
                    source, param = s.split(":")
                    if source in source_handlers:
                        val = source_handlers[source](param)
                        if val is not None:
                            result = val
                            break
            result = result if result is not None else default
            yield k, result, unit 

