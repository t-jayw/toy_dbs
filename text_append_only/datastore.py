import os
import json

class TextOnlyAppendDatastore:
    def __init__(self, segment_size_threshold, db_name):
        self.index = {}  # In-memory index
        self.segment_size_threshold = segment_size_threshold  # Max size of each segment file
        self.db_name = db_name
        self.db_directory = f"{db_name}"  # Directory where the segments will be stored
        self.current_segment_number = 0  # Keep track of the current segment number
        self.current_segment_size = 0  # Keep track of the current segment size
        os.makedirs(self.db_directory, exist_ok=True)  # Create the directory if it does not exist
        self.current_segment = self._get_new_segment_file()  # Open the initial segment


    def _get_new_segment_file(self):
        """Get a new segment file handle."""
        segment_name = f"{self.db_directory}/{self.current_segment_number}.txt"
        self.current_segment_number += 1
        self.current_segment_size = 0  # Reset the size for the new segment
        return open(segment_name, 'a')  # Open the new segment file in append mode

    def set(self, key, value):
        """Set the value for a key."""
        # Serialize the record
        record = json.dumps({key: value})
        record_size = len(record) + 1  # +1 for newline character

        # Check if the current segment file has reached the threshold before writing
        if self.current_segment_size + record_size > self.segment_size_threshold:
            self.current_segment.close()
            self.current_segment = self._get_new_segment_file()
            print(f"Switching to new segment: {self.current_segment.name}")  # Debug line

        # Write the record to the current segment
        self.current_segment.write(record + '\n')
        self.current_segment.flush()  # Ensure data is written to disk

        # Update the current segment size
        self.current_segment_size += record_size

        # Capture the offset immediately after writing the record
        offset = self.current_segment.tell() - record_size
        self.index[key] = (self.current_segment.name, offset)

        # Print debug information
        print(f"Set key: {key} at offset: {offset} in file: {self.current_segment.name}")
        print(f"Current segment size: {self.current_segment_size} (Threshold: {self.segment_size_threshold})")  # Debug line


    def get(self, key):
        """Get the value for a key."""
        if key in self.index:
            segment_name, offset = self.index[key]
            try:
                with open(segment_name, 'r') as segment_file:
                    segment_file.seek(offset)
                    record = segment_file.readline().strip()
                    return json.loads(record)
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON for key {key}: {e}")
            except Exception as e:
                print(f"An error occurred while retrieving the key {key}: {e}")
        return None  # If the key is not found, return None


    def compact(self):
        """Compact the datastore by consolidating segments."""
        print("Starting compaction process...")
        new_data = {}

        # Consolidate the data
        for key, (segment_name, offset) in self.index.items():
            print(f"Retrieving value for key: {key}")
            value = self.get(key)  # Get the most recent value for the key
            if value is not None:  # Ensure that the value is not None
                print(f"Adding {key}: {value[key]} to new_data for compaction.")
                new_data[key] = value[key]  # Save the actual value associated with the key

        # Determine the next segment number after the last existing segment
        existing_segments = [int(filename.split('.')[0]) for filename in os.listdir(self.db_directory) if filename.endswith('.txt')]
        max_segment_number = max(existing_segments) if existing_segments else self.current_segment_number

        # Write the consolidated data to a new segment
        self.current_segment.close()
        print(f"Closed current segment. Preparing to create a new segment...")
        self.current_segment_number = max_segment_number + 1
        self.current_segment = self._get_new_segment_file()
        print(f"New segment file created: {self.current_segment.name}")

        # Reset the current segment size before repopulating the data
        self.current_segment_size = 0

        for key, value in new_data.items():
            print(f"Setting {key}: {value} in new segment.")
            self.set(key, value)  # This will update the new_index indirectly as self.index is used in set

        # Remove old segments
        for filename in existing_segments:
            segment_path = f"{self.db_directory}/{filename}.txt"
            if segment_path != self.current_segment.name:  # Do not delete the new segment
                try:
                    print(f"Attempting to remove old segment: {segment_path}")
                    os.remove(segment_path)
                    print(f"Successfully removed old segment: {segment_path}")
                except OSError as e:
                    print(f"Error removing file {segment_path}: {e}")

        print("Compaction process completed.")


