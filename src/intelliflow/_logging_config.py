# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import importlib
import logging
import logging.handlers
import os
import sys
from pathlib import Path

""" 
Provide default logging setup 
"""

LOG_DATE_FORMAT = "%m/%d/%Y %I:%M:%S %p"
LOG_FORMAT = "%(levelname)s | %(asctime)-15s | %(message)s"
CORE_LOG_FILE = "rheoceros_core.log"


def init_basic_logging(log_dir=None, enable_console_logging=True, root_level=logging.INFO):
    logger = logging.getLogger()
    logger.setLevel(root_level)

    from intelliflow.core.deployment import is_on_remote_dev_env

    if enable_console_logging and not is_on_remote_dev_env():
        # add stdout handler, with level INFO
        console = logging.StreamHandler(sys.stdout)
        console.setLevel(root_level)
        console_formatter = logging.Formatter("%(asctime)s - %(name)-13s: %(levelname)-8s %(message)s")
        console.setFormatter(console_formatter)
        logger.addHandler(console)

    # Add file rotating handler, with level DEBUG
    if log_dir:
        if not Path(log_dir).exists():
            Path(log_dir).mkdir(parents=True)
        # log_dir = os.curdir
        rotatingHandler = logging.handlers.RotatingFileHandler(filename=log_dir + os.path.sep + CORE_LOG_FILE, maxBytes=5000, backupCount=5)
        rotatingHandler.setLevel(logging.DEBUG)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        rotatingHandler.setFormatter(formatter)
        logger.addHandler(rotatingHandler)

    # logging.captureWarnings(True)

    # refer
    #   https: // docs.python.org / 3 / library / logging.html  # logging.basicConfig
    # for more details.
    logging.basicConfig(format=LOG_FORMAT, datefmt=LOG_DATE_FORMAT, level=root_level)

    return logger
