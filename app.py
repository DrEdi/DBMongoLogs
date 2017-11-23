import csv

from bson.son import SON
from pymongo import MongoClient


class Connector:
    """Class provides access to MongoDB with definite methods."""

    def __init__(self, port, db_name, collection=None):
        """Constructor for Connector class.

        :param port: open port with DB
        :param db_name: name of DB

        :Example:

        >>> connector = Connector(27017, 'clients')
        """

        client = MongoClient('mongodb://localhost:{p}/'.format(p=port))
        self.db = client[db_name]
        if collection:
            self.collection = self.db[collection]

    def __convert_csv_to_json(self, path):
        """Convert data from csv to json format.

        :param path: path to csv file in string format

        """
        data = []
        with open(path, 'r') as csvfile:
            fieldnames = ['URL', 'IP', 'timeStamp', 'timeSpent']
            reader = csv.DictReader(csvfile, fieldnames=fieldnames)
            for row in reader:
                row['timeStamp'] = int(row['timeStamp'])
                row['timeSpent'] = int(row['timeSpent'])
                data.append(row)
        return data

    def save_data_from_csv(self, path):
        """Migrate data from given csv file to DB.

        :param path: path to csv file in string format

        """
        json_data = self.__convert_csv_to_json(path)
        if self.collection:
            self.collection.insert_many(json_data)

    def get_ips_by_url(self, url):
        """Return list of visitors' ips for giver url.

        :param url: url for serach

        """
        return [*self.collection.find({'URL': url}, {'IP': 1}).distinct('IP')]

    def get_urls_by_period(self, start_date, end_date):
        """Return list of urls visited by given period.

        :param start_date: start of period
        :param end_date: end of period

        """
        return [*self.collection.find(
            {'timeStamp': {'$gt': start_date, '$lt': end_date}},
            {'URL': 1}
        ).distinct('URL')]

    def get_url_by_ip(self, ip):
        """Return list of urls visited by given user ip.

        :param ip: ip address of user

        """
        return [*self.collection.find({'IP': ip}, {'URL': 1}).distinct('URL')]

    def get_spent_time_stat(self):
        """Return stats about spent time for each URL."""
        pipeline = [
            {"$unwind": "$URL"},
            {"$group": {"_id": "$URL", "timeSpent": {"$sum": "$timeSpent"}}},
            {"$sort": SON([("count", 1)])}
        ]
        return [*self.collection.aggregate(pipeline)]

    def get_visit_count_stat(self):
        """Return stats about visits count for each URL."""
        pipeline = [
            {"$unwind": "$URL"},
            {"$group": {"_id": "$URL", "visits": {"$sum": 1}}},
            {"$sort": SON([("count", 1)])}
        ]
        return [*self.collection.aggregate(pipeline)]

    def get_ip_stats(self):
        """Return stats about visits and time spent for IP."""
        pipeline = [
            {"$unwind": "$IP"},
            {"$group": {"_id": "$IP", "visits": {"$sum": 1},
                        'timeSpent': {"$sum": "$timeSpent"}}},
            {"$sort": SON([("count", 1), ("timeSpent", 1)])}
        ]
        return [*self.collection.aggregate(pipeline)]


if __name__ == '__main__':
    conn = Connector(27017, 'test', 'test_data')
