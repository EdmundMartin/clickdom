Clickdom provides an Async and Synchronous client for the Clickhouse database from Yandex. The project is in it's very 
early stages and I plan to eventually add ORM like support for interacting with the database.

# Client
```python
import datetime as dt
from clickdom.core import CoreClient

client = CoreClient('http://localhost:8123/')
client.execute("INSERT INTO t VALUES", (1, (dt.date(2029, 9, 7), None)), (2, (dt.date(2029, 9, 8), 3.14)),)

# Retrieve all rows
res = client.fetch_all('SELECT * FROM t')

# Retrieve first row
res = client.fetch_one('SELECT * FROM t')

# Retrieve first value
res = client.fetch_value('SELECT * FROM t')
```