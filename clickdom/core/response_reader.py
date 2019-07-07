from typing import Any, List

from aiohttp import ClientResponse

from clickdom.core.mappings import CLICK_TO_PY, resolve_nullable, resolve_tuple, resolve_array


def read_headers(resp_line: bytes):
    headers = resp_line.split(b'\t')
    header_vals = []
    for header in headers:
        header_vals.append(header.decode())
    return header_vals


def extract_types(resp_line: bytes) -> List[str]:
    click_types = resp_line.split(b'\t')
    type_values = []
    for click_type in click_types:
        type_values.append(click_type.decode())
    return type_values


def _composite_type(ch_type: str, val: str):
    if ch_type.startswith('Array'):
        return resolve_array(ch_type, val)
    elif ch_type is 'Nullable':
        return resolve_nullable(ch_type, val)
    elif ch_type.startswith('Tuple'):
        return resolve_tuple(ch_type, val)
    else:
        return val


def transform(row: bytes, ch_types: List[str]):
    values = [v.decode() for v in row.split(b'\t')]
    row_data = []
    for i in zip(values, ch_types):
        val, ch_type = i
        if ch_type in CLICK_TO_PY:
            row_data.append(CLICK_TO_PY[ch_type](val))
        else:
            row_data.append(_composite_type(ch_type, val))
    return row_data


class Row:

    def __init__(self, headers: List[str], values: List[Any]):
        for i, h in enumerate(headers):
            self.__dict__[h] = values[i]

    def __repr__(self):
        return repr(self.__dict__)


class ResponseReader:

    def __init__(self, response: bytes):
        self.response = response

    def read_response(self, fetch_one: bool = False):
        rows = []
        lines = self.response.split(b'\n')
        headers = read_headers(lines[0])
        click_types = extract_types(lines[1])
        if fetch_one:
            data = transform(lines[2], click_types)
            return Row(headers, data)
        for row in lines[2:]:
            if row != b'':
                data = transform(row, click_types)
                rows.append(Row(headers, data))
        return rows

    def read_value(self):
        lines = self.response.split(b'\n')
        target_row = lines[2]
        if target_row == b'':
            return
        v = target_row.split(b'\t')[0].decode()
        ch_type = lines[1].split(b'\t')[0].decode()
        if ch_type in CLICK_TO_PY:
            return CLICK_TO_PY[ch_type](v)
        else:
            return _composite_type(ch_type, v)


class AsyncReader:

    def __init__(self, response):
        self.response: ClientResponse = response

    async def _extract_headers(self):
        raw_headers = await self.response.content.readline()
        raw_types = await self.response.content.readline()
        headers = read_headers(raw_headers.strip(b'\n'))
        click_types = extract_types(raw_types.strip(b'\n'))
        return headers, click_types

    async def read_one(self):
        headers, click_types = await self._extract_headers()
        line = await self.response.content.readline()
        data = transform(line.strip(b'\n'), click_types)
        return Row(headers, data)

    async def read_response(self):
        rows = []
        headers, click_types = await self._extract_headers()
        while True:
            line = await self.response.content.readline()
            line = line.strip(b'\n')
            if line == b'':
                break
            data = transform(line, click_types)
            rows.append(Row(headers, data))
        return rows

    async def read_value(self):
        headers, click_types = await self._extract_headers()
        click_type = click_types[0]
        data = await self.response.content.readline()
        val = data.split(b'\t')[0].decode()
        if click_type in CLICK_TO_PY:
            return CLICK_TO_PY[click_type](val)
        else:
            return _composite_type(click_type, val)