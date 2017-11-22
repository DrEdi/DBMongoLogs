import csv

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


if __name__ == '__main__':
    a = Connector(27017, 'logs', 'data')
    # a.save_data_from_csv('./data.csv')
    print(a.get_ips_by_url('www.hello.com'))
    print(a.get_urls_by_period(32, 32131231246))
    print(a.get_url_by_ip('128.292.1.2'))
