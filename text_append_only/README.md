# Text Append-Only Module

## Description
The Text Append-Only module is a part of the Toy DBs project, focusing on demonstrating how an append-only datastore can be implemented using plain text files.

## Features
- Append-only data storage: Ensures data immutability and simple recovery.
- Key-value storage: Stores data as key-value pairs for easy access.
- Compaction: Periodically compacts data to remove outdated entries and free up space.

### Usage
1. Import the `TextOnlyAppendDataStore` class from the `datastore.py` file.
2. Instantiate the `TextOnlyAppendDataStore` class with a specified directory for storage.
3. Use the `set_key` method to add data and the `get_key` method to retrieve data.

Example:
```python
from text_append_only.datastore import TextOnlyAppendDataStore

db = TextOnlyAppendDataStore('/path/to/db_directory')
db.set_key('my_key', 'my_value')
value = db.get_key('my_key')
print(value)  # Outputs: 'my_value'

# As segments grow you can run db.compact() to compact the segments
db.compact()
```

