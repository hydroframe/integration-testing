"""Configuration file to read options for pytests."""


def pytest_addoption(parser):
    """Add supported options"""

    parser.addoption("--cache", action="store", default="cold")
    parser.addoption("--wy", action="store", default="1997")
    parser.addoption("--comment", action="store", default="")
    parser.addoption("--server", action="store", default="remote")
