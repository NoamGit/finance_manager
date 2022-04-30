import pytest
from prefect.utilities.testing import prefect_test_harness

@pytest.fixture(autouse=True)
def prefect_test_fixture():
    with prefect_test_harness():
        yield

def test_isracard_flow(prefect_test_fixture):
    assert True==False