from typing import Any, List

import requests

from clickdom.core.query_builder import query_type, build_query, QueryType
from clickdom.core.exceptions import QueryError
from clickdom.core.response_reader import ResponseReader
from clickdom.core.mappings import to_bytes


class CoreClient:

    def __init__(self, url: str, **kwargs):
        self._session = requests.Session()
        self._url = url
        self._db_params = dict()
        self._db_params['user'] = kwargs.get('user', None)
        self._db_params['password'] = kwargs.get('password', None)
        self._db_params['database'] = kwargs.get('database', 'default')

    @property
    def alive(self):
        resp = self._session.get(self._url)
        return resp.ok

    def _execute(self, query_str: str, *args):
        q_type = query_type(query_str)
        query = build_query(query_str, q_type, *args)
        params = self._db_params
        params['query'] = query
        if args:
            data = to_bytes(args)
            resp = self._session.post(self._url, params=self._db_params, data=data)
        else:
            resp = self._session.post(self._url, params=self._db_params)
        if resp.status_code != 200:
            raise QueryError(resp.text)
        if q_type == QueryType.RETRIEVE:
            return resp
        return []

    def execute(self, query: str, *args):
        self._execute(query, *args)

    def fetch_all(self, query):
        resp = self._execute(query)
        reader = ResponseReader(resp.content)
        return reader.read_response()

    def fetch_one(self, query):
        resp = self._execute(query)
        reader = ResponseReader(resp.content)
        return reader.read_response(fetch_one=True)

    def fetch_value(self, query):
        resp = self._execute(query)
        reader = ResponseReader(resp.content)
        return reader.read_value()


if __name__ == '__main__':
    import datetime as dt
    client = CoreClient('http://localhost:8123/')
    #client._execute('CREATE TABLE LOL (array Array(String), uint_field UInt8) ENGINE = Memory')
    #results = client.execute("INSERT INTO LOL VALUES", (['Hello', 'World'], 8))
    results = client.fetch_all('SELECT * FROM LOL')
    print(results)