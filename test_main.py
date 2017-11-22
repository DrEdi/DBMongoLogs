from main import Connector

conn = Connector(27017, 'test', 'test_data')


def test_connector_init():
    assert conn.db and conn.collection


def test_get_ips_by_url():

