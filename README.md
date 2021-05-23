# This is a simplistic implementation of log structured storage engine
User can store `key value` pairs in this database.
Values are Base64 encoded before storing to remove any issue regarding `,` and ` `

There is daemon thread running, which monitors the size of the file and the number of keys in `global_hash_table` and `database file (ex: db.txt)`.
Since the insertion work in append only mode, there might be duplicate keys. The ask of the daemon thread is to compact the `db.txt` file.

# How to use
Run the file by `python main.py`

Then you can do operations like `set <key> <value>` or `get <key>`.
To get all the entries use `get *`

# example
```python
>>> set key_1 1234
>>> set object {"name": "john", "numbers": [1,2,3,4]}
>>> get key_1
key_1: 1234
>>> get key_2
key_2: {"name": "john", "numbers": [1,2,3,4]}
```
