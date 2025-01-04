"""Configuration file to read options for pytests."""


def pytest_addoption(parser):
    """Add supported options"""

    parser.addoption("--cache", action="store", default="cold")
    parser.addoption("--wy", action="store", default="2002")
    parser.addoption("--wy_month", action="store", default="02")
    parser.addoption("--comment", action="store", default="")
    parser.addoption("--cpus", action="store", default="8")
