from invoke import task

_dirs = ["tax_loss", "scripts", "tests"]


@task
def update(c):
    c.run("poetry update")


@task
def test(c):
    c.run("pytest tests --cov -s -v --color=yes")


@task
def install(c, no_dev=False):
    if no_dev:
        c.run("poetry install --no-dev")
    else:
        c.run("poetry install")


@task
def check(c, ignore_types=False):
    print("Running isort...")
    c.run(f"isort {' '.join(_dirs)}")
    print("Running black...")
    c.run(f"black {' '.join(_dirs)}")
    print("Running flake8...")
    c.run(f"flake8 {' '.join(_dirs)}")
    if not ignore_types:
        print("Running mypy...")
        c.run(f"mypy {' '.join(_dirs)}")


@task(pre=[check, test])
def build(c):
    c.run("poetry build")
