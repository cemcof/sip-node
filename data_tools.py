""" Tools and utilities for sniffing files, transfering files or continually reading files while writing to them from another process """
import asyncio
import concurrent
import hashlib
import random
import tempfile
import threading, traceback
from datetime import datetime
from enum import Enum
import logging
import os, sys
import pathlib
import time
import shutil
import re
import typing
import traceback
import yaml
import common
from common import as_list
import functools
from fnmatch import fnmatch

class TransferAction(Enum):
    COPY = "copy"
    MOVE = "move"

class TransferCondition(Enum):
    ONCE = "once"
    IF_MISSING = "if_missing"
    IF_NEWER = "if_newer"
    ALWAYS = "always"
    # IF_DIFFERENT = "IfDifferent"

class PathPattern:
    def __init__(self, filename: str, dirname: typing.Union[str, pathlib.Path] = None):
        self.filename = str(filename)
        self.dirname = dirname
        self._combined = self.filename if not dirname else str(dirname) + "/" + filename

    @staticmethod
    def parse(pattern: str):
        if pattern.startswith("re:"):
            return RegexPattern.parse(pattern)
        else:
            return FnMatchPattern.parse(pattern)

    def with_dir(self, new_dir: pathlib.Path) -> 'PathPattern':
        raise NotImplementedError()

    def match(self, path: pathlib.Path):
        raise NotImplementedError()

    def __str__(self):
        return self._combined

class FnMatchPattern(PathPattern):
    ANY_DIR = "**"

    def __init__(self, filename: str, dirname: typing.Union[str, pathlib.Path] = None):
        super().__init__(filename, dirname)
        if dirname == self.ANY_DIR:
            self._combined = str(dirname) + "/" + filename
        else:
            self._combined = filename

    def with_dir(self, new_dir: pathlib.Path):
        return FnMatchPattern(self.filename, new_dir)

    def match(self, path: pathlib.Path):
        res = fnmatch(str(path), self._combined)
        if not res and self.dirname == self.ANY_DIR:
            # Also include matching just filename itself, without directory (without /)
            return fnmatch(path.name, self.filename)
        return res

    @staticmethod
    def parse(pattern: str):
        pth = pathlib.Path(pattern)

        return FnMatchPattern(pth.name, pth.parent)

class RegexPattern(PathPattern):
    @staticmethod
    def parse(pattern: str):
        if pattern.startswith("re:"):
            pattern = pattern[3:]
        spl = pattern.rsplit('/', 1)
        if len(spl) == 1:
            return RegexPattern(pattern)
        else:
            return RegexPattern(spl[1], spl[0])

    def with_dir(self, new_dir: pathlib.Path):
        return RegexPattern(self.filename, new_dir)

    def match(self, path: pathlib.Path):
        return re.match(self._combined, str(path))

class DataRule:
    def __init__(self, 
                 patterns: typing.Union[typing.List[PathPattern], str],
                 tags: typing.Union[typing.List[str], typing.Tuple[str], str],
                 target: str = None, 
                 keep_tree: bool = False, 
                 subfiles=True, 
                 action: typing.Union[str, TransferAction] = TransferAction.COPY,
                 condition: typing.Union[str, TransferCondition] = TransferCondition.IF_MISSING,
                 checksum = True,
                 delay = 1.0,
                 del_delay = 0.5
                ) -> None:

        # Load patterns
        self.patterns = []
        if isinstance(patterns, str) or isinstance(patterns, PathPattern):
            patterns = [patterns]

        for p in patterns:
            if isinstance(p, PathPattern):
                self.patterns.append(p)
            elif isinstance(p, str):
                self.patterns.append(PathPattern.parse(p))
            else:
                raise ValueError(f"Invalid pattern: {p}")

        self.tags = as_list(tags)
        self.target = pathlib.Path(target) if target else None
        self.keep_tree = keep_tree
        self.action = TransferAction(action)
        self.condition = TransferCondition(condition)
        self.subfiles = subfiles
        self.checksum = checksum
        self.delay = delay
        self.del_delay = 0.5

    def translate_to_target(self, path_relative: pathlib.Path):
        if self.target:
            return self.target / path_relative if self.keep_tree else self.target / path_relative.name
        return path_relative
    
    def match_files(self, files: typing.Iterable[pathlib.Path]):
        if self.subfiles:
            # True = any subfiles, number > 0 = minimal amout of subfiles
            min_subfiles = 0 if self.subfiles is True else self.subfiles
            return self._match_files_with_subfiles(files, self.patterns, min_subfiles)
        else:
            return self._match_files_without_subfiles(files, self.patterns)

    @staticmethod
    def _match_files_without_subfiles(files: typing.Iterable[pathlib.Path], patterns: typing.List[PathPattern]):
        files = list(files)
        for f in files:
            if any(p.match(f) for p in patterns):
                yield f

    @staticmethod
    def _match_files_with_subfiles(files: typing.Iterable[pathlib.Path], patterns: typing.List[PathPattern], min_subfiles: int = 0):
        """ For performance reasons, we need better algorithm than n^2, start by sorting the files,
         then iterating them and upon match, search for subfiles near that match, they should be in one sequence thanks to the sorting """
        files = sorted(files)
        index = 0   
        while index < len(files):
            f = files[index]
            if any(p.match(f) for p in patterns):
                # Now search for subfiles - thanks to sorting, they should be in one sequence with the main file
                start_index, end_index = DataRule._search_subfiles_indices(files, index)
                subfiles_count = end_index - start_index
                if subfiles_count < min_subfiles:
                    # Skip this match completely
                    index = end_index + 1
                    continue

                # First come subfiles
                for i in range(start_index, end_index + 1):
                    if i != index:
                        yield files[i]

                # Primary file last
                yield f
                index = end_index + 1
            else:
                index = index + 1

    @staticmethod
    def _search_subfiles_indices(files: typing.List[pathlib.Path], index: int):
        def _matches(f1: pathlib.Path, f2: pathlib.Path):
            # Must have same parent 
            if not f1.parent == f2.parent:
                return False

            # F1's name is same as F2's stem
            if f1.name == f2.stem:
                return True
            
            # F2 stem is basename of F1 (must be followed immediately by extension)
            return f1.name.startswith(f2.stem + ".")
        
        # Subfiles are around the given index, find start index and end index, then yeild them
        start_index = index
        while start_index > 0 and _matches(files[start_index - 1], files[index]):
            start_index = start_index - 1
        end_index = index
        while end_index < len(files) - 1 and _matches(files[end_index + 1], files[index]):
            end_index = end_index + 1
        
        return start_index, end_index

    
    def get_target_patterns(self):
        """ Gets glob pattern through which files for this rule can be searched in the target location """
        new_dir_base = self.target if not self.keep_tree else None
        return [p.with_dir(new_dir_base or p.dirname) for p in self.patterns]
    
    def __str__(self) -> str:
        return f"DataRule({self.patterns}, {self.tags}, {self.target}, {self.keep_tree}, {self.subfiles}, {self.action}, {self.condition})"

class DataRulesWrapper:
    def __init__(self, data_rules: typing.Union[list, DataRule, dict]) -> None:
        # Data_rules arg is a list that can contain both dicts or DataRule objects
        # Create self.data_rules where all items are DataRule objects
        self.data_rules : typing.List[DataRule] = []
        if isinstance(data_rules, DataRule) or isinstance(data_rules, dict):
            data_rules = [data_rules]
        for dr in data_rules: 
            self.data_rules.append(dr if isinstance(dr, DataRule) else DataRule(**dr))

    def with_tags(self, *tags) -> 'DataRulesWrapper':
        # Filter current data rules by tag and return new dataruleswrapper object
        # Make each given "tag" to be a set
        tag_sets = [item if isinstance(item, set) else {item} for item in tags]
        out_rules = []
        for dr in self.data_rules:
            # Any of tag_sets is subset of dr.tags?
            dr_ready = any(tg.issubset(dr.tags) for tg in tag_sets)
            if dr_ready:
                out_rules.append(dr)

        return DataRulesWrapper(out_rules)
    
    def match_files(self, files: typing.Iterable[pathlib.Path]):
        files = set(files)
        for dr in self.data_rules:
            matched = set()
            for m in dr.match_files(files):
                yield m, dr
                matched.add(m)
            files = files.difference(matched)

    def get_target_for(self, *tags, **rule_args) -> DataRule:
        patts_result = []
        for rule in self.with_tags(*tags):
            patts_result = patts_result + rule.get_target_patterns()
        return DataRule(patts_result, tags, **rule_args)

    def get_tags_patterns(self):
        for rule in self.data_rules:
            for tag in rule.tags:
                yield tag, rule.patterns


    def __iter__(self):
        return iter(self.data_rules)

    def __str__(self) -> str:
        return "[ " + ", ".join(map(lambda x: str(x), self.data_rules)) + " ] "
    
class DataRulesSniffer:
    def __init__(self, globber: typing.Union[pathlib.Path, typing.Callable], data_rules: DataRulesWrapper, consumer, metafile: pathlib.Path = None, min_nochange_sec=10, reconsume_on_change=True) -> None:
        self.data_rules = data_rules
        self.consumer = consumer
        self.metafile = metafile
        self.min_nochange_sec = min_nochange_sec
        self.reconsume_on_change = reconsume_on_change

        self.globber = globber
        if isinstance(globber, pathlib.Path):
            self.globber = functools.partial(multiglob, globber)

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
        errors = []
        metafile_append = self.metafile.open("a") if self.metafile else None
        
        for f, data_rule, ts_mod, size in self.globber(self.data_rules):
            if self.should_exclude(f):
                continue
            time_change = ts_mod
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
                        metafile_append.flush()
                except Exception as e:
                    errors.append((f, e))
                    traceback.print_exc()
                    print(f"Consumation of {f} failed", e, file=sys.stderr)

        if metafile_append:
            metafile_append.close()
            
        # Return list of tuples of consumed files (only new ones in this sniff run) and their consumation times, sorted by the time
        filtered_new = filter(lambda x: x[1] > consumation_start, meta.items())
        return sorted(filtered_new, key=lambda x: x[1]), errors



class DataTransferTarget:
    def stat(self, path: pathlib.Path) -> typing.Tuple[int, float]:
        raise NotImplementedError()

    def checksum(self, path: pathlib.Path, type: str) -> str:
        raise NotImplementedError()

    def supported_checksums(self) -> typing.FrozenSet[str]:
        raise NotImplementedError()

    def resolve_target_location(self, path: pathlib.Path = None) -> pathlib.Path:
        raise NotImplementedError()

    def put_file(self, target_path: pathlib.Path, source_path: pathlib.Path) -> bool:
        raise NotImplementedError()

    def is_same(self, other: 'DataTransferTarget', path_src: pathlib.Path, path_dst: pathlib.Path) -> bool:
        raise NotImplementedError()


class DataTransferSource(DataTransferTarget):
    def glob(self, data_rules: DataRulesWrapper):
        raise NotImplementedError()

    def del_file(self, path: pathlib.Path):
        raise NotImplementedError()

    def get_file(self, path: pathlib.Path, target_path: pathlib.Path):
        raise NotImplementedError()

class FsTransferSource(DataTransferSource):
    def __init__(self, root: pathlib.Path) -> None:
        self.root = root

    def supported_checksums(self):
        return frozenset({"md5", "sha256"})

    def glob(self, data_rules: DataRulesWrapper):
        target = self.resolve_target_location()
        for f, dr, m, s in multiglob(target, data_rules):
            yield f.relative_to(target), dr, m, s

    def exists(self, path_relative: pathlib.Path):
        target = self.resolve_target_location(path_relative)
        return target.exists()

    def stat(self, path_relative: pathlib.Path):
        target = self.resolve_target_location(path_relative)
        stat = target.stat()
        return stat.st_mtime, stat.st_size

    def get_file(self, path_relative_src: pathlib.Path, path_dst: pathlib.Path):
        target = self.resolve_target_location(path_relative_src)
        shutil.copyfile(target, path_dst)
        return True

    def del_file(self, path_relative: pathlib.Path):
        target = self.resolve_target_location(path_relative)
        target.unlink()

    def put_file(self, path_relative: pathlib.Path, src_file: pathlib.Path):
        target = self.resolve_target_location(path_relative)
        # Ensure target directory for the file exists
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src_file, target)

    def is_same(self, other, path_src: pathlib.Path, path_dst: pathlib.Path):
        return isinstance(other, FsTransferSource) and other.resolve_target_location(path_dst) == self.resolve_target_location(path_src) is not None

    def checksum(self, path_relative: pathlib.Path, sumtype: str):
        file_path = self.resolve_target_location(path_relative)
        if sumtype.lower() == "md5":
            hash_func = hashlib.md5()
        elif sumtype.lower() == "sha256":
            hash_func = hashlib.sha256()
        else:
            raise ValueError("Unsupported checksum type. Use 'md5' or 'sha256'.")

        with open(file_path, "rb") as f:
            chunk = f.read(8192)
            while chunk:
                hash_func.update(chunk)
                chunk = f.read(8192)

        return hash_func.hexdigest()

    def resolve_target_location(self, src_relative: pathlib.Path = None) -> pathlib.Path:
        return self.root / (src_relative or "")

class TransferResult:
    def __init__(self, file: pathlib.Path, dr: DataRule, total_time: float, transfer_time: float, size: float, modif: float,
                 checksum):
        self.file = file
        self.dr = dr
        self.total_time = total_time
        self.transfer_time = transfer_time
        self.size = size
        self.modif = modif
        self.checksum = checksum

class TargetNotSameSizeOrModifyError(Exception):
    pass

class ChecksumMismatchError(Exception):
    def __init__(self, src_file, trg_file, src_sum, trg_sum):
        self.src_file = src_file
        self.trg_file = trg_file
        self.src_sum = src_sum
        self.trg_sum = trg_sum

    def __str__(self):
        base = super().__str__()
        return f"{base} | Checksum mismatch for {self.src_file} and {self.trg_file}: {self.src_sum} != {self.trg_sum}"


class DataAsyncTransferer:
    def __init__(self,
                 source: DataTransferSource,
                 target: DataTransferTarget,
                 data_rules: DataRulesWrapper,
                 identifier: str,
                 logger: logging.Logger=None,
                 on_start = None,
                 on_finish = None,
                 on_file_done = None):
        self.source = source
        self.target = target
        self.data_rules = data_rules
        self.identifier = identifier
        self.logger = logger or logging.getLogger("sniff")
        self.metafile = pathlib.Path(tempfile.gettempdir()) / f"_sniff_{identifier}.yml"
        self.on_start = on_start or (lambda: None)
        self.on_finish = on_finish or (lambda: None)
        self.on_file_done = on_file_done or (lambda: None)
        self.max_consecutive_errors = 10

        self.executor = None
        self.ev_loop = None

    def _load_metafile(self):
        if self.metafile and self.metafile.exists():
            with self.metafile.open("r") as metafile:
                return yaml.full_load(metafile)
        return None

    def should_exclude(self, path: pathlib.Path):
        return path.name.startswith("_")

    def _submit(self, fn, *args, **kwargs):
        future = self.executor.submit(fn, *args, **kwargs)
        return asyncio.wrap_future(future, loop=self.ev_loop)

    def _transfer_strategy_download(self, file: pathlib.Path, data_rule: DataRule):
        target = self.target.resolve_target_location()
        absolute_target = target / data_rule.translate_to_target(file)
        absolute_target.parent.mkdir(parents=True, exist_ok=True)
        start_ts = time.time()
        self.source.get_file(file, absolute_target)
        return time.time() - start_ts


    def _transfer_strategy_upload(self, file: pathlib.Path, data_rule: DataRule):
        source = self.source.resolve_target_location(file)
        relative_target = data_rule.translate_to_target(file)
        start_ts = time.time()
        self.target.put_file(relative_target, source)
        return time.time() - start_ts

    def _transfer_strategy_buffer_file(self, file: pathlib.Path, data_rule: DataRule):
        buffer_file = pathlib.Path(tempfile.gettempdir()) / f"_transfer_buffer_{self.identifier}.dat"
        # Get into buffer, put from buffer
        start_ts = time.time()
        self.source.get_file(file, buffer_file)
        self.target.put_file(data_rule.translate_to_target(file), buffer_file)
        return time.time() - start_ts

    def _transfer_strategy_fs_direct(self, file: pathlib.Path, data_rule: DataRule):
        # In this strategy, we should skip if locations are same
        source, target = self.source.resolve_target_location(file), self.target.resolve_target_location(data_rule.translate_to_target(file))
        if source == target:
            raise ValueError("Cannot copy to the same location!")

        # Here we perform standard fs copy
        start = time.time()
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(source, target)
        return time.time() - start

    def _determine_transfer_strategy(self):
        src_loc, trg_loc = self.source.resolve_target_location() is not None, self.target.resolve_target_location() is not None
        map = {
            (True, True): self._transfer_strategy_fs_direct,
            (True, False): self._transfer_strategy_upload,
            (False, True): self._transfer_strategy_download,
            (False, False): self._transfer_strategy_buffer_file
        }
        return map[(src_loc, trg_loc)]

    async def transfer_unit(self, file: pathlib.Path, strategy: callable, data_rule: DataRule, order: int):
        initial_modify, initial_size  = self.source.stat(file)
        initial_time = time.time()

        # Keep sleeping delay until stat matches
        while True:
            await asyncio.sleep(data_rule.delay)
            stat = self.source.stat(file)
            if stat == (initial_modify, initial_size):
                break
            initial_modify, initial_size = stat

        # Now, file is likely ready, commence transfer
        stime = time.time()
        # print(f"[{order}] Submitting {file}")
        transfer_time = await self._submit(strategy, file, data_rule, priority=order)

        took_time = time.time() - stime
        print(f"[{order}] Finished transfer, time: {transfer_time:.3f}, took: {took_time:.3f}")
        
        # Transfer done, now before checksum, check if size/modify changed, in that case fail and start again
        if self.source.stat(file) != (initial_modify, initial_size):
            print(f"[{order}] File size/modify changed, aborting transfer: {file}, {initial_modify}, {initial_size}, {self.source.stat(file)}")
            raise TargetNotSameSizeOrModifyError()

        # Correct, lets do checksum if desired...
        if data_rule.checksum:
            # Find checksum type that both target and source support
            sumtype = next(iter(self.source.supported_checksums().intersection(self.target.supported_checksums())), None)
            src_file = file
            trg_file = data_rule.translate_to_target(file)
            srcsum = await self._submit(self.source.checksum, src_file, sumtype, priority=order)
            trgsum = await self._submit(self.target.checksum, trg_file, sumtype, priority=order)
            # print(f"[{order}] Computed checksums: {srcsum} {trgsum}")
            if srcsum != trgsum:
                raise ChecksumMismatchError(src_file, trg_file, srcsum, trgsum)


        # Delete action, currently, if we fail to del, just continue normally and leave it
        if data_rule.action == TransferAction.MOVE:
            # Wait a bit before actually deleting the source
            await asyncio.sleep(data_rule.del_delay)
            try:
                await self._submit(self.source.del_file, file, priority=order)
            except Exception as e:
                self.logger.warning(f"Failed to delete file {file}, ignoring it: {e}")

        # Yeah! Transfer done, no exception, return times it took and size
        return TransferResult(file, data_rule, time.time() - initial_time, transfer_time, initial_size, initial_modify,
                              checksum=data_rule.checksum)

    async def transfer_all(self):
        consumation_start = time.time()
        meta = self._load_metafile() or {}
        errors = []
        successes = []
        metafile_append = self.metafile.open("a") if self.metafile else None

        def _mark_as_done_helper(file, mod):
            meta[str(file)] = mod
            successes.append((file, mod))  # TODO what?
            if metafile_append:
                metafile_append.write(f"{str(file)}: {mod}\n")
                metafile_append.flush()

        tasks = []
        transfer_strategy = self._determine_transfer_strategy()

        # Go through globbed files and if desirable schedule the transfer
        total_size_to_transfer = 0
        for f, dr, modif, size in self.source.glob(self.data_rules):
            if self.should_exclude(f):
                continue
            last_transfer_modtime = meta.get(str(f), None)

            # We do not transfer if not modified
            if dr.condition == TransferCondition.IF_NEWER and last_transfer_modtime == modif:
                continue

            # We do not transfer if already transferred
            if dr.condition == TransferCondition.IF_MISSING and last_transfer_modtime is not None:
                continue

            if self.source.is_same(self.target, f, dr.translate_to_target(f)):
                # In this case, do not copy! Locations are the same and we just mark as done!
                _mark_as_done_helper(f, modif)
                continue

            # Transfer this unit (schedule)
            # print("Would Transfer: ", f, " ", dr.tags, " ", modif, " ", size, " ", transfer_strategy, " ", len(tasks), "")
            tsk = self.ev_loop.create_task(self.transfer_unit(f, transfer_strategy,  dr, len(tasks)))
            tasks.append(tsk)
            total_size_to_transfer += size

        # Log what we scanned and queued
        rules_str = ""
        for rule in self.data_rules:
            rules_str += f"[{', '.join(rule.tags)}] - [{', '.join([str(p) for p in rule.patterns])}] \n"

        print(f"Scanned and queued {len(tasks)} files of total size {common.sizeof_fmt(total_size_to_transfer)} for transfer. ")
        self.logger.info(f"Scanned and queued {len(tasks)} files of total size {common.sizeof_fmt(total_size_to_transfer)} for transfer. "
                         f"Rules: \n {rules_str}")

        # Now, wait for the transfer tasks and react to their result
        consecutive_errors = 0
        for tsk in asyncio.as_completed(tasks):
            try:
                result = await tsk

                _mark_as_done_helper(result.file, result.modif)

                message = f"TRANSFER [{', '.join(result.dr.tags)}]; {common.sizeof_fmt(result.size)}, {result.transfer_time:.3f} sec, \n {result.file.name}"
                if result.checksum:
                    message += f", checksum validated"
                self.logger.info(message)
                consecutive_errors = 0
            except Exception as e:
                self.logger.error("File transfer failed: " + str(e))
                traceback.print_exc()
                errors.append(e)
                consecutive_errors += 1

            if consecutive_errors > self.max_consecutive_errors:
                self.logger.error("Too many consecutive errors, storage failure is likely... prematurely terminating, not finishing all scanned files")
                # for tsk in tasks:
                #     tsk.cancel()
                break

        return successes, errors

    def stop(self):
        tsks = asyncio.all_tasks(self.ev_loop)
        for tsk in tsks:
            tsk.cancel()
        self.ev_loop.run_until_complete(asyncio.gather(*tsks, return_exceptions=True))
        self.ev_loop.close()
        self.ev_loop = None

        self.executor.shutdown(cancel_futures=True)
        self.executor = None

    def transfer(self, until: threading.Event=None):
        self.ev_loop = asyncio.new_event_loop()
        self.executor = common.PriorityThreadPoolExecutor(max_workers=1, thread_name_prefix=f"transfer_{self.identifier}")
        asyncio.set_event_loop(self.ev_loop)
        # Bad transfer config (no files selected) - new one has been launched
        try:
            result = self.ev_loop.run_until_complete(self.transfer_all())
        finally:
            self.stop()

        return result


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
        "IsDirectory": True,
        "AllowPick": False
    }

def sort_file_items(items: list):
    def extract_date_from_string(input_string):
        # Try parsing with yyyyMMdd format
        try:
            date = datetime.strptime(input_string[:8], "%Y%m%d")
            return date
        except ValueError:
            pass
        
        # Try parsing with yyMMdd format
        try:
            date = datetime.strptime(input_string[:6], "%y%m%d")
            return date
        except ValueError:
            pass
        
        # If no valid date is found, return None
        return None


    # First process and sort these starting by a number, sort them by this number value and then by rest of the name, descnding
    numbered = sorted(filter(lambda x: extract_date_from_string(x["Name"]) is not None, items), key=lambda x: (extract_date_from_string(x["Name"]), x["Name"]), reverse=True)

    # Then process and sort these starting by a letter, sort them by name, from A to Z
    lettered = sorted(filter(lambda x: extract_date_from_string(x["Name"]) is None, items), key=lambda x: x["Name"])
    
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
    rootdir = root[0]["Path"]
    logger.debug(f"Rootdir is: {rootdir}")
    result.append({"Path": parentdirn, "Name": "..", "IsDirectory": True, "AllowPick": rootdir != parentdirn})
    result.append({"Path": dirn, "Name": ".", "IsDirectory": True, "AllowPick": rootdir != dirn})

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


def multiglob(path: pathlib.Path, data_rules: DataRulesWrapper):
    # Glob whole tree and then match against the data rules, in given order
    all_files = path.glob("**/*")
    all_rel_files = map(lambda x: x.relative_to(path), all_files)

    for f, dr in data_rules.match_files(all_rel_files):
        abspath = path / f
        if abspath.is_file():
            stat = abspath.stat()
            yield abspath, dr, stat.st_mtime, stat.st_size


# Basic test
if __name__ == "__main__":
    from concurrent.futures.thread import ThreadPoolExecutor

    pe = ThreadPoolExecutor()
    for i in range(5):
        pe.submit(time.sleep, 50)


    async def task(n):
        delay = random.uniform(1, 4)
        print(f"Task {n} step 0, delay {delay}")
        await asyncio.sleep(delay)
        print( f"Task {n} step 1")
        await asyncio.sleep(delay)
        print( f"Task {n} step 2")
        await asyncio.sleep(delay)
        return f"Task {n} done"




    async def main():
        # [asyncio.create_task(task(i)) for i in range(5)]
        # loop = asyncio.new_event_loop()
        pe = ThreadPoolExecutor()
        for i in range(5):
            pe.submit(time.sleep, 50)
        # for coro in asyncio.as_completed([asyncio.create_task(task(i)) for i in range(5)]):
        #     result = await coro
        #     print(result)
        # asyncio.Future()
        # await asyncio.sleep(10)


    # asyncio.run(main())
    #
    # logging.basicConfig(level=logging.DEBUG)
    # logger = logging.getLogger("transfer_simulator")
    #
    # import sys
    #
    # src = sys.argv[1]
    # dst = sys.argv[2]
    # logger.info(f"Creating simulator for {src} -> {dst}")
    # simulator = DataTransferSimulator(pathlib.Path(src), pathlib.Path(dst), logger)
    # simulator.transfer()
