import csv
import json

from pymongo import MongoClient


def convert_csv_to_json(path):
    data = {'data': []}
    with open(path, 'r') as csvfile:
        fieldnames = ['URL', 'IP', 'timeStamp', 'timeSpent']
        reader = csv.DictReader(csvfile, fieldnames=fieldnames)
        for row in reader:
            data['data'].append(row)
    return json.dumps(data)


class Connector:
    """Class provides access to MongoDB with definite methods."""

    def __init__(self, port, db_name):
        """Constructor for Connector class.

        :param port: open port with DB
        :param db_name: name of DB

        :Example:

        >>> connector = Connector(27017, 'clients')
        """

        client = MongoClient('mongodb://localhost:{p}/'.format(p=port))
        self.db = client[db_name]
