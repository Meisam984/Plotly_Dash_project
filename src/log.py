import json
import os
import sys
import logging
import logging.config
from datetime import datetime

os.makedirs('.logs', exist_ok=True)

# Set the logger config, using dictConfig and loading the log_dict_config.json
with open(file='log_dict_config.json') as f:
    config_dict = json.load(f)
    config_dict["handlers"]["file"]["filename"] = f".logs/{datetime.now().strftime('%d-%B-%Y')}.log"
    logging.config.dictConfig(config_dict)


# Instantiate logger object
logger = logging.getLogger("main")


# Override the excepthook, for uncaught exceptions
def handle_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logger.exception("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))

sys.excepthook = handle_exception