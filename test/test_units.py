from unittest.mock import Mock

from main import main

def assertion(data):
    req = Mock(get_json=Mock(return_value=data), args=data)
    res = main(req)
    for i in res['results']:
        assert i['num_processed'] >= 0
        if i['output_rows']:
            assert i['num_processed'] == i['output_rows']
            assert i['errors'] is None

def test_auto():
    data = {}
    assertion(data)

def test_manual():
    data = {
        "start": "2021-01-01",
        "end": "2021-06-30"
    }
    assertion(data)
