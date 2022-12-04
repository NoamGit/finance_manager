import json

import pytest
from pytest import fixture

@fixture()
def raw_data():
    with open("../mock/otsar_data.json",'r') as f:
        res = json.loads(f.read())
    return res

def test_dummy(raw_data):
    return pytest.fail()