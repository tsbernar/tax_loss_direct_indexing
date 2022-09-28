import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--e2e", action="store_true", default=False, help="End to end tests can be slow, only run when --e2e is given"
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "e2e: mark test as e2e and slow to run")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--e2e"):
        # --runslow given in cli: do not skip slow tests
        return
    skip_e2e = pytest.mark.skip(reason="need --e2e option to run")
    for item in items:
        if "e2e" in item.keywords:
            item.add_marker(skip_e2e)
