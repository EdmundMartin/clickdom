from aiohttp.client import ClientSession, ClientResponse

from clickdom.core.query_builder import query_type, build_query, QueryType
from clickdom.core.mappings import to_bytes
from clickdom.core.exceptions import QueryError
from clickdom.core.response_reader import AsyncReader


async def get_client(klass) -> ClientSession:
    sess = getattr(klass, '_session')
    if sess:
        return sess
    client = ClientSession()
    setattr(klass, '_session', client)
    return client


class AsyncCoreClient:

    def __init__(self, url: str, **kwargs):
        self._session = None
        self._url = url
        self._db_params = dict()
        if 'user' in kwargs:
            self._db_params['user'] = kwargs.get('user', '')
        if 'password' in kwargs:
            self._db_params['password'] = kwargs.get('password', '')
        self._db_params['database'] = kwargs.get('database', 'default')

    async def close(self):
        if self._session:
            await self._session.close()

    async def alive(self):
        session = await get_client(self)
        async with session.get(url=self._url) as resp:
            return resp.status == 200

    async def _execute(self, query_str: str, *args):
        q_type = query_type(query_str)
        query = build_query(query_str, q_type, *args)

        params = self._db_params
        params['query'] = query
        data = None
        if args:
            data = to_bytes(args)
        session = await get_client(self)
        try:
            resp = await session.post(self._url, params=params, data=data)
            if resp.status != 200:
                raise QueryError(await resp.read())
            if q_type == QueryType.RETRIEVE:
                return resp
        except Exception:
            raise QueryError('LOLAGE')
        return None

    async def execute(self, query_str: str, *args):
        resp: ClientResponse = await self._execute(query_str, *args)
        await resp.close()

    async def fetch_all(self, query):
        resp = await self._execute(query)
        reader = AsyncReader(resp)
        return await reader.read_response()

    async def fetch_one(self, query):
        resp = await self._execute(query)
        reader = AsyncReader(resp)
        return await reader.read_one()

    async def fetch_value(self, query):
        resp = await self._execute(query)
        reader = AsyncReader(resp)
        return await reader.read_value()


if __name__ == '__main__':
    import asyncio
    a_client = AsyncCoreClient('http://localhost:8123/')
    loop = asyncio.get_event_loop()
    res = loop.run_until_complete(a_client.fetch_value('SELECT * FROM trial'))
    print(res)