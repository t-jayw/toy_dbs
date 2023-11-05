import os
import random
import json
import bisect
from collections import OrderedDict

class SSTableDatastore:
    def __init__(self, db_name, segment_size_threshold=10):
        self.db_name = db_name
        self.segment_size_threshold = segment_size_threshold
        self.segments = []
        self.memtable = OrderedDict()
        os.makedirs(self.db_name, exist_ok=True)
        self._load_existing_segments()
        self.current_segment_number = 0

    def _load_existing_segments(self):
        """Load existing segments from disk."""
        segment_files = sorted(
            file for file in os.listdir(self.db_name) if file.endswith('.json')
        )
        for segment_file in segment_files:
            segment_path = os.path.join(self.db_name, segment_file)
            with open(segment_path, 'r') as f:
                # We only load the first key to keep the index in memory
                first_key = next(iter(json.load(f)))
            self.segments.append((first_key, segment_path))

    def _write_segment(self):
        if not self.memtable:  # Check if memtable is empty
            return  # If empty, nothing to write, so just return

        # Sort the memtable by key before writing
        sorted_memtable = OrderedDict(sorted(self.memtable.items()))

        segment_name = f"{self.db_name}/segment_{self.current_segment_number}.json"
        self.current_segment_number += 1

        with open(segment_name, 'w') as f:
            json.dump(sorted_memtable, f)

        # Assume the first key is the lowest because we sorted the memtable
        first_key = next(iter(sorted_memtable))
        self.segments.append((first_key, segment_name))
        self.memtable.clear()


    def set(self, key, value):
        """Set the value for a key in the memtable. Write to segment if threshold is reached."""
        self.memtable[key] = value
        if len(self.memtable) >= self.segment_size_threshold:
            self._write_segment()

    def get(self, key):
        """Get the value for a key from the SSTable."""
        # Check if the key is in the memtable first
        if key in self.memtable:
            return self.memtable[key]
        
        # If not in the memtable, check the segments
        for _, segment_path in reversed(self.segments):
            with open(segment_path, 'r') as f:
                segment = json.load(f)
                if key in segment:
                    return segment[key]
        return None  # Key not found

    def compact(self):
        """Compact the datastore by consolidating segments and removing duplicates."""
        all_data = OrderedDict()
        for _, segment_path in self.segments:
            with open(segment_path, 'r') as f:
                segment_data = json.load(f)
                all_data.update(segment_data)

        # Clear the current segments list
        self.segments.clear()

        # Write all data back to a new segment file
        # We don't need to swap memtable data here, just write all_data out.
        compacted_segment_name = f"{self.db_name}/segment_compacted.json"
        with open(compacted_segment_name, 'w') as f:
            json.dump(all_data, f)

        # Assume the first key is the lowest because all_data is ordered
        first_key = next(iter(all_data))
        self.segments.append((first_key, compacted_segment_name))

        # Remove old segments
        segment_files = [
            file for file in os.listdir(self.db_name) if file.startswith('segment_') and file.endswith('.json')
        ]
        for segment_file in segment_files:
            if segment_file != 'segment_compacted.json':  # Don't remove the new compacted segment
                segment_path = os.path.join(self.db_name, segment_file)
                try:
                    os.remove(segment_path)
                except OSError as e:
                    print(f"Error removing file {segment_path}: {e}")

        # Update the current segment number to reflect the new state
        self.current_segment_number = 1  # Reset to 1 since we just have one compacted segment now

        # Now, we clear the old memtable which should be empty already but it's a safety measure
        self.memtable.clear()

if __name__ == "__main__":
	# Usage
	datastore = SSTableDatastore("my_sstable_db", segment_size_threshold=512)

	key_prefix = "key"
	value_prefix = "value"
	num_keys = 2048  # Set the number of keys you want to insert

	# Create a list of keys
	keys = [i for i in range(num_keys)]
	# Shuffle the list of keys to ensure random order
	random.shuffle(keys)

	# Insert each key into the datastore
	for i, key in enumerate(keys):
		value = f"{value_prefix}{key}"
		# Simulate the set operation, which adds the key-value pair to the memtable
		datastore.set(key, value)

		# Check if the number of items in the memtable reaches the threshold to write to a segment
		if len(datastore.memtable) >= datastore.segment_size_threshold:
			datastore._write_segment()  # This method will write the memtable to a new segment and clear it

		# Uncomment the following line if you want to see the key being inserted
		if i%512==0:
			print(f"Inserted {key}: {value}")


	print(f"Getting value 1: {datastore.get('1')}")
	print(f"Segments: {[x for x in datastore.segments]}")
	#TODO
	print(f"Compacting")
	print(f"Segments: {datastore.segments}")
