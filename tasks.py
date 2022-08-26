from invoke import task


@task
def build(c):
    c.run("poetry build")


@task
def update(c):
    c.run("poetry update")


@task
def install(c, no_dev=False):
    if no_dev:
        c.run("poetry install --no-dev")
    else:
        c.run("poetry install")


@task
def check(c, ignore_types=False):
    print("Running isort...")
    c.run("isort tax_loss")
    print("Running black...")
    c.run("black tax_loss")
    print("Running flake8...")
    c.run("flake8 tax_loss")
    if not ignore_types:
        print("Running mypy...")
        c.run("mypy tax_loss")
