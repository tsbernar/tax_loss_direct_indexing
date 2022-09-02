import json
import logging
import sys
import urllib3

import click
import munch
import yaml  # type: ignore

from tax_loss.strategy import DirectIndexTaxLossStrategy

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


def read_config(filepath):
    config = yaml.safe_load(open(filepath))
    config = munch.munchify(config)
    return config


@click.command()
@click.option("--config", "config_file", required=True)
def main(config_file):
    config = read_config(config_file)
    setup_logging(config)
    logger.info(f"Starting app with config : \n{json.dumps(config, indent=4)}")
    strategy = DirectIndexTaxLossStrategy(config)
    strategy.run()

    return 0


if __name__ == "__main__":
    sys.exit(main())
