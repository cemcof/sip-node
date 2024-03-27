import time

import requests
import logging
import uuid
import datetime

DOTNET_LOGMAP = {
    logging.getLevelName(logging.DEBUG): "Debug",
    logging.getLevelName(logging.INFO): "Information",
    logging.getLevelName(logging.ERROR): "Error",
    logging.getLevelName(logging.CRITICAL): "Critical",
    logging.getLevelName(logging.WARNING): "Warning",
}

class LimsApiLoggerHandler(logging.Handler):
        def __init__(self, http_session: requests.Session, level):
            super().__init__(level)
            self._session = http_session
            self._buffer = []
            self._buffer_limit = 1

        def emit(self, record: logging.LogRecord):
            self.format(record)
            if not hasattr(record, "log_id"):
                record.log_id = str(uuid.uuid4())
            if not hasattr(record, "exp_id"):
                record.exp_id = None
            if not hasattr(record, "origin"):
                record.origin = "-"
            self._buffer.append(record)
            if len(self._buffer) >= self._buffer_limit:
                self.flush()

        def flush(self):
            try:
                logs_data = [
                    {
                        "Id": l.log_id,
                        "ExperimentId": l.exp_id,
                        "Dt": datetime.datetime.utcfromtimestamp(l.created).isoformat(),
                        "Origin": l.origin,
                        "Level": DOTNET_LOGMAP[l.levelname],
                        "Message": l.getMessage()
                    } for l in self._buffer
                ]

                if not logs_data:
                    return

                # Submit log data to the endpoint
                res = self._session.post("experiments/logs", json=logs_data, stream=False)
                # Ensure request was OK
                res.raise_for_status() 
                # Clean the buffer
                self._buffer = []
            except Exception as e: # TODO - just DB error
                print(f"Logger handler error: {e}")
            

        def close(self):
            self.flush()
            super().close()
            self._session.close()


class AddExtraAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        prev_extra = kwargs["extra"] if "extra" in kwargs else {}
        kwargs["extra"] = { **self.extra, **prev_extra }
        return msg, kwargs

def prepare_lims_api_logger(name: str, origin: str, api_session: requests.Session):
    logger = logging.getLogger(name)
    logger.propagate = True
    logger.setLevel(logging.DEBUG)
    hnd = LimsApiLoggerHandler(api_session, level=logging.INFO)
    logger.addHandler(hnd)
    return AddExtraAdapter(logger, {"origin": origin})

def experiment_logger_adapter(logger: logging.Logger, exp_id): 
    return AddExtraAdapter(logger, {"exp_id": exp_id})

def wrap_logger_origin(logger: logging.Logger, origin: str):
    return AddExtraAdapter(logger, {"origin": origin})
