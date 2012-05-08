# needed for py.test to accept the --valgrind option
def pytest_addoption(parser):
    parser.addoption("--no-valgrind", action="store_true", help="disable valgrind analysis")
