"""
this is second of two scripts
    first script
        is periodically started on TEMs to generate data files
        is not publicly available
    second script
        parses datafiles and generates states of autoloaders on TEMs at CEMCOF
        sends the data to LIMS for displaying to the user and further processing
"""

import datetime
import shlex
import sys
import common
import logging
import pathlib
import yaml, json, re
import configuration

def timestamp2t(ts):
    return datetime.datetime.strptime(ts, '%y%m%d%H%M%S')

def parse_states(filename: pathlib.Path, initial_state: dict):
    states = { }
    lines = filename.read_text().splitlines(keepends=False)
    if not lines:
        return states

    state = initial_state.copy()
    last_timestamp = None

    timestamp = None
    for line in lines:
        if not line.strip():  # treat empty lines
            continue
        
        mathes = re.match(r"(?P<timestamp>\d+)\s+(?P<kw>.*')\s+(?P<val>.*)", line)
        timestamp, kw, val = mathes.group('timestamp'), mathes.group('kw').strip("' "), mathes.group('val').strip("' ")
        if timestamp != last_timestamp:
            if last_timestamp is not None:
                # t = timestamp2t(last_timestamp)
                states[last_timestamp] = state.copy()
                # we keep state for next iteration
            last_timestamp = timestamp
        state[kw] = val
    # last:
    # t = timestamp2t(timestamp)
    # assert t not in states, t
    states[timestamp] = state.copy()

    return states

def autoloaders(config: dict, logger: logging.Logger):
    result = { }
    total_raw_data_size = 0
    for instrument, target in config["InstrumentData"].items():
        logger.debug(f"Loading autoloader data - {instrument}: {target}")
        result[instrument] = {}
        target = pathlib.Path(target)
        if not target.is_dir():
            logger.error(f"Configured autoloader data path {target} does not exist")
            continue
        filenames = sorted(target.glob("*.dat"))
        if not filenames:
            logger.info(f'No .dat files found in {target}!')
            continue

        for i, filename in enumerate(filenames, start=1):
            logger.info(f'Processing {i}/{len(filenames)}: {filename.name}')
            states = parse_states(filename, config["InitialState"])
            total_raw_data_size = total_raw_data_size + filename.stat().st_size
            result[instrument][str(filename)] = states

    logger.info(f"Total raw data size: {common.sizeof_fmt(total_raw_data_size)}")
    return result



class AutoloadersHandler(configuration.LimsNodeModule):
    def step(self):

        # Gather autloaders data
        result = autoloaders(self.module_config["Autoloaders"], self.logger)

        # Submit them to LIMS 
        self._api_session.post("autoloaders", json=result)

        
if __name__ == '__main__':
    with open(sys.argv[1], "r") as f:
        conf = yaml.safe_load(f)

    logging.basicConfig(level=logging.DEBUG)
    res = autoloaders(conf["Autoloaders"], logging.root)

    for inst, data in res.items():
        jsndata = json.dumps(data)
        print(f"Data size for {inst}: {common.sizeof_fmt(len(jsndata))}, len {len(data)}")
        