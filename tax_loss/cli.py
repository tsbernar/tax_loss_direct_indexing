"""Console script for tax_loss."""
import sys

import click


@click.command()
def main(args=None):
    """Console script for tax_loss."""
    click.echo("Replace this message by putting your code into " "tax_loss.cli.main")
    click.echo("See click documentation at https://click.palletsprojects.com/")
    return 0


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
