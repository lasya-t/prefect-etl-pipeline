import pytest
from prefect.testing.utilities import prefect_test_harness


@pytest.fixture(autouse=True, scope="session")
def prefect_backend():
    # isolated temporary Prefect database for the whole test session
    with prefect_test_harness():
        yield
