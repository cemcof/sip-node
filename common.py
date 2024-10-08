
import logging
import re
import time
import pathlib
import threading
import datetime
import requests
import urllib.parse
import subprocess
import types
# Utility to convert file size to huma readable format
# from https://stackoverflow.com/questions/1094841/get-human-readable-version-of-file-size
def sizeof_fmt(num, suffix="B"):
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"

# Because default datetime.fromisoformat is limited and cannot handle timezone postfix
def parse_iso_date(date_str: str):
    is_frac = "." in date_str
    pattern = "%Y-%m-%dT%H:%M:%S.%fZ" if is_frac else "%Y-%m-%dT%H:%M:%SZ"  
    dt = datetime.datetime.strptime(date_str, pattern).replace(tzinfo=datetime.timezone.utc)
    return dt

def parse_date(date_str: str):
    """ Parse string representation of the date, return None on default values (start of epoch, -infinity etc.) """
    if not date_str:
        return None
    dt = parse_iso_date(date_str)
    if dt.year == 1 or dt.year == 0:
        return None
    return dt
    
def stringify_date(dt: datetime.datetime):
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

def elapsed_since(interval: datetime.timedelta, *sinces : datetime.datetime, dt_from=None):
    """ Return True if the interval has passed since first given date that is not None. Raises if all given dates are None"""
    if not dt_from:
        dt_from = datetime.datetime.now(datetime.timezone.utc)

    for since in sinces:
        if not since: 
            continue

        return dt_from - since > interval
    
    raise ValueError("No valid since date provided")


def parse_timedelta(timedelta: str) -> datetime.timedelta:
    # TODO - fix - incorrect regex
    pattern = r'(?:(?P<days>\d+)\.)?(?:(?P<hours>\d+):)?(?:(?P<minutes>\d+):)?(?P<seconds>\d+\.?\d*)'
    match = re.match(pattern, timedelta)
    
    if match:
        groups = match.groupdict()
        days = int(groups['days']) if groups['days'] else 0
        hours = int(groups['hours']) if groups['hours'] else 0
        minutes = int(groups['minutes']) if groups['minutes'] else 0
        seconds = float(groups['seconds'])
        
        return datetime.timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)
    else:
        raise ValueError("Invalid timedelta format")


# This is supposted to be run on thread
def action_thread(func, sleeptimesec, cancel_event: threading.Event, reraise_exception=False):
    while True:
        if cancel_event.is_set():
            return 
        
        try:
            func()
        except KeyboardInterrupt as ke:
            print("Keyboard interrupt happening in a thread")
            exit(0)
        except Exception as e:
            logging.error("Error in controller module step", exc_info=e)
            if reraise_exception:
                raise

        time.sleep(sleeptimesec)


def path_universal_factory(path: str):
    """ Creates appropriate pathlib path object depending on platform. """
    if "\\" in path or ":" in path:
        # This is most likely windows path
        try:
            result = pathlib.WindowsPath(path)
        except NotImplementedError: # It is windows path, but apperently we are not on windows platform - use just pure windows path.
            result = pathlib.PureWindowsPath(path)
    else:
        # This is most likely unix path
        try:
            result = pathlib.PosixPath(path)
        except NotImplementedError:  # It unix path, but apperently we are not on unix platform - use just pure unix path.
            result = pathlib.PurePosixPath(path)

    return result

def translate_path(path: pathlib.Path, mappings, replacing_stem=None):
    for src, dst in mappings:
        if src in [path] + [p for p in path.parents]:
            rel = path.relative_to(src)
            return dst / rel.parent / replacing_stem if replacing_stem else dst / rel
    
    return path.parent / replacing_stem if replacing_stem else path

def try_translate_path(path: pathlib.Path, src: pathlib.Path, dst: pathlib.Path):
    if src in [path] + [p for p in path.parents]:
        rel = path.relative_to(src)
        return rel 

    return None

class DataWrapper:
    def __init__(self, data) -> None:
        self.data = data

# Base url options is missing in requests library, thus this
class BaseUrlSession(requests.Session):
    def __init__(self, base_url, timeout=5) -> None:
        super().__init__()
        self.base_url = base_url
        self.timeout = timeout

    def request(self, method, url, *args, **kwargs):
        joined_url = urllib.parse.urljoin(self.base_url, url)
        if not "timeout" in kwargs:
            kwargs["timeout"] = self.timeout
        res = super().request(method, joined_url, *args, **kwargs)
        # res.raise_for_status()
        return res
    

class StateObj:

    def get_state(self):
        raise NotImplementedError()
    
    def exec_state(self, state_map: dict = None, continue_on_change=True):
        cont = True
        last_state = None

        while cont: 

            state = self.get_state()
            args = []
            # If state is tuple, the second item is an argument to be passed to state function
            if isinstance(state, tuple):
                args = state[1:]
                state = state[0]

            if not state_map or state not in state_map:
                state_func = getattr(self, state)
                # args.insert(0, self)
            else:
                state_func = state_map[state]

            if state != last_state:
                state_func(*args)

            cont = continue_on_change and state != last_state
            last_state = state

def exec_state(on: StateObj, state_map: dict, continue_on_change=True):
    """ Take StateObject, get its state and do the appropriate action. """
    cont = True
    last_state = None

    while cont: 

        state = on.get_state()
        args = []
        # If state is tuple, the second item is an argument to be passed to state function
        if isinstance(state, tuple):
            args = state[1:]
            state = state[0]

        state_func = state_map[state]

        if state != last_state:
            state_func(*args)

        cont = continue_on_change and state != last_state
        last_state = state


def dict_to_json_patch(input_dict, path=""):
    operations = []

    for key, value in input_dict.items():
        current_path = f"{path}/{key}" if path else key

        if isinstance(value, dict):
            # Recursively process nested dictionaries
            operations.extend(dict_to_json_patch(value, path=current_path))
        else:
            # Add a "replace" operation for each leaf value
            operation = {"op": "replace", "path": f"/{current_path}", "value": value}
            operations.append(operation)

    return operations

def search_for_key(data, key):
    """ Recursively search key in given data object, which can be dict, list, or object  """
    if isinstance(data, dict):
        for k, v in data.items():
            if k == key:
                yield v
            else:
                yield from search_for_key(v, key)
    elif isinstance(data, list):
        for item in data:
            yield from search_for_key(item, key)
    else:
        return
    
def get_dict_val_by_path(data, path, default=None):
    # path_parts = path.split("/")
    # current = data
    # for part in path_parts:
    #     if current is not None and part in current:
    #         current = current[part]
    #     else:
    #         return None
    # return current
    tmp_val = data
    for subkey in path.split("/"):
        try:
            tmp_val = tmp_val[subkey]
        except (KeyError, TypeError): 
            return default
    return tmp_val

def set_dict_val_by_path(data, path, value):
    path_parts = path.split("/")
    current = data
    for part in path_parts[:-1]:
        if part not in current:
            current[part] = {}
        current = current[part]
    current[path_parts[-1]] = value
        
def to_safe_filename(string):
    return re.sub(r'[\\/*?:"<>| ]', "_", string)


def lmod_getenv(lmod_path, module_name):
    """ Get environment variables from lmod module """
    res = subprocess.run([lmod_path, "python", "load", module_name], capture_output=True, text=True, check=True)
    pattern = r'os\.environ\[("|\')(.*?)("|\')\] = ("|\')(.*?)("|\')'
    matches = re.findall(pattern, res.stdout)
    env_dict = {m[1]: m[4] for m in matches}
    return env_dict

def as_list(item):
    if isinstance(item, list):
        return item
    if isinstance(item, tuple) or isinstance(item, set) or isinstance(item, types.GeneratorType):
        return list(item)
    
    return [item]