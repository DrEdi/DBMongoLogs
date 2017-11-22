import pytest

from main import Connector

conn = Connector(27017, 'test', 'test_data')

mocked_data = [
    {'URL': 'test.com',
     'IP': '121.213.21.21',
     'timeStamp': 100,
     'timeSpent': 41},
    {'URL': 'test_s.com',
     'IP': '121.213.21.21',
     'timeStamp': 999,
     'timeSpent': 11},
    {'URL': 'test.com',
     'IP': '12.213.21.21',
     'timeStamp': 222,
     'timeSpent': 4}
]


@pytest.fixture
def clear_db():
    conn.collection.delete_many({})
    return


@pytest.fixture
def collect_data():
    conn.collection.insert_many(mocked_data)
    return


def test_connector_init():
    assert conn.db and conn.collection


def test_get_ips_by_url(clear_db, collect_data):
    assert conn.get_ips_by_url('test.com') == ['121.213.21.21', '12.213.21.21']


def test_get_url_by_ip(clear_db, collect_data):
    assert conn.get_url_by_ip('12.213.21.21') == ['test.com']


def test_get_urls_by_period(clear_db, collect_data):
    assert conn.get_urls_by_period(220, 1000) == ['test_s.com', 'test.com']


def test_spent_time_stat(clear_db, collect_data):
    assert conn.get_spent_time_stat() == [{'_id': 'test_s.com', 'timeSpent': 11},
                                          {'_id': 'test.com', 'timeSpent': 45}]


def test_visit_count_stat(clear_db, collect_data):
    assert conn.get_visit_count_stat() == [{'_id': 'test_s.com', 'visits': 1},
                                           {'_id': 'test.com', 'visits': 2}]


def test_get_ip_stat(clear_db, collect_data):
    assert conn.get_ip_stats() == [{'_id': '12.213.21.21', 'visits': 1,
                                    'timeSpent': 4},
                                   {'_id': '121.213.21.21', 'visits': 2,
                                    'timeSpent': 52}]
