import json
import logging
import sys

import click
import munch
import yaml  # type: ignore


def setup_logging(config):
    log_level = config.log_level
    log_file = config.log_file
    log_fmt = "[%(asctime)s] %(levelname)s %(filename)s:%(funcName)s:%(lineno)i :: %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    logging.basicConfig(filename=log_file, format=log_fmt, level=log_level, datefmt=datefmt)


def read_config(filepath):
    config = yaml.safe_load(open(filepath))
    config = munch.munchify(config)
    return config


@click.command()
@click.option("--config", "config_file")
def main(config_file):
    config = read_config(config_file)
    print(config)
    setup_logging(config)
    logger = logging.getLogger(__name__)
    logger.info(f"Starting app with config : \n{json.dumps(config, indent=4)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
