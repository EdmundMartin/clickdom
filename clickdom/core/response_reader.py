from collections import namedtuple
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


def transform(row: bytes, ch_types: List[str]):
    values = [v.decode() for v in row.split(b'\t')]
    row_data = []
    for i in zip(values, ch_types):
        val, ch_type = i
        if ch_type in CLICK_TO_PY:
            row_data.append(CLICK_TO_PY[ch_type](val))
        elif ch_type.startswith('Array'):
            row_data.append(resolve_array(ch_type, val))
        elif ch_type is 'Nullable':
            row_data.append(resolve_nullable(ch_type, val))
        elif ch_type.startswith('Tuple'):
            row_data.append(resolve_tuple(ch_type, val))
        else:
            row_data.append(val)
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
        headers = read_headers(lines[0])
        click_types = extract_types(lines[1])
        target_row = lines[2]
        if target_row == b'':
            return
        v = target_row.split(b'\t')[0].decode()
        print(v)


class AsyncReader:

    def __init__(self, response):
        self.response: ClientResponse = response

    async def read_response(self, fetch_one: bool = False):
        rows = []
        raw_headers = await self.response.content.readline()
        raw_types = await self.response.content.readline()
        headers = read_headers(raw_headers.strip(b'\n'))
        click_types = extract_types(raw_types.strip(b'\n'))
        print(headers, click_types)
        while True:
            line = await self.response.content.readline()
            line = line.strip(b'\n')
            if line == b'':
                break
            data = transform(line, click_types)
            rows.append(Row(headers, data))
        return rows
