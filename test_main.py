from main import Connector

conn = Connector(27017, 'test', 'test_data')


def test_connector_init():
    assert conn.db and conn.collection


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
