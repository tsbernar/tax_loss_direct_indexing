from invoke import task


@task
def check(c):
    print("Running isort...")
    c.run("isort tax_loss")
    print("Running black...")
    c.run("black tax_loss")
    print("Running flake8...")
    c.run("flake8 tax_loss")
