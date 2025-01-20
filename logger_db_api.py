import datetime
import logging
import queue
import time
import uuid

import requests

DOTNET_LOGMAP = {
    logging.getLevelName(logging.DEBUG): "Debug",
    logging.getLevelName(logging.INFO): "Information",
    logging.getLevelName(logging.ERROR): "Error",
    logging.getLevelName(logging.CRITICAL): "Critical",
    logging.getLevelName(logging.WARNING): "Warning",
}

class LimsApiLoggerHandler(logging.Handler):
        def __init__(self, 
                     http_session: requests.Session, 
                     level=logging.INFO,
                     queue_max=6400,
                     max_logs_per_request=64):
            super().__init__(level)
            self._session = http_session
            self._buffer = queue.Queue(queue_max)
            self._max_logs_per_request = max_logs_per_request
            
        def emit(self, record: logging.LogRecord):
            self.format(record)
            if not hasattr(record, "log_id"):
                record.log_id = str(uuid.uuid4())
            if not hasattr(record, "exp_id"):
                record.exp_id = None
            if not hasattr(record, "origin"):
                record.origin = "-"
            self._buffer.put(record)

        def flush(self):
            # Take items from queue into list, maximum max_logs_per_request, do that until queue is exhausted
            while not self._buffer.empty():
                logs_data = []
                while not self._buffer.empty() and len(logs_data) < self._max_logs_per_request:
                    record = self._buffer.get()
                    logs_data.append({
                        "Id": record.log_id,
                        "ExperimentId": record.exp_id,
                        "Dt": datetime.datetime.utcfromtimestamp(record.created).isoformat(),
                        "Origin": record.origin,
                        "Level": DOTNET_LOGMAP[record.levelname],
                        "Message": record.getMessage()
                    })

                if not logs_data:
                    continue

                self._submit_logs(logs_data)

        def _submit_logs(self, logs_data, max_retries=5, delay=1):
            for attempt in range(max_retries):
                try:
                    res = self._session.post("experiments/logs", json=logs_data, stream=False)
                    res.raise_for_status()
                    return  # if successful, exit the function
                except Exception as e:
                    if attempt < max_retries - 1:
                        print(f"Retrying after error: {e}. Attempt {attempt + 1}/{max_retries}")
                        time.sleep(delay)
                        delay *= 2  # exponential backoff
                    else:
                        print(f"Logger handler error after {max_retries} attempts: {e}, dropping {len(logs_data)} logs")
                        break
            
            
        def keep_flushing(self, interval=0.7):
            while True:
                self.flush()
                time.sleep(interval)

        def close(self):
            self.flush()
            super().close()
            self._session.close()


class AddExtraAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        prev_extra = kwargs["extra"] if "extra" in kwargs else {}
        kwargs["extra"] = { **self.extra, **prev_extra }
        return msg, kwargs

def prepare_lims_api_logger(name: str, origin: str, handler: LimsApiLoggerHandler):
    logger = logging.getLogger(name)
    logger.propagate = True
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    return AddExtraAdapter(logger, {"origin": origin})

def experiment_logger_adapter(logger: logging.Logger, exp_id): 
    return AddExtraAdapter(logger, {"exp_id": exp_id})

def wrap_logger_origin(logger: logging.Logger, origin: str):
    return AddExtraAdapter(logger, {"origin": origin})
