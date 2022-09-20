import json
import logging
import sys
import traceback

import click
import urllib3

from tax_loss.email import Emailer
from tax_loss.strategy import DirectIndexTaxLossStrategy
from tax_loss.util import read_config

logger = logging.getLogger(__name__)


def setup_logging(config):
    log_level = config.log_level
    log_file = config.log_file
    log_fmt = "[%(asctime)s] %(levelname)s %(filename)s:%(funcName)s:%(lineno)i :: %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    logging.basicConfig(filename=log_file, format=log_fmt, level=log_level, datefmt=datefmt)
    #  Stop libraries from logging too much info
    logging.getLogger("numba").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)  # silance warning about no ssl verify


@click.command()
@click.option("--config", "config_file", required=True)
def main(config_file):
    config = read_config(config_file)
    setup_logging(config)
    logger.info(f"Starting app with config : \n{json.dumps(config, indent=4)}")
    try:
        strategy = DirectIndexTaxLossStrategy(config)
        strategy.run()
    except Exception as e:
        logger.critical(f"Exception while running: {e}\n{traceback.format_exc()}")
        emailer = Emailer(config.secrets_filepath)
        emailer.send_msg(
            f"Exception while running: {e}\n{traceback.format_exc()}", subject="Direct Indexing Failure Notification"
        )
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
