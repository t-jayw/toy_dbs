import unittest
import os
import shutil
from text_append_only.datastore import TextOnlyAppendDatastore

class TestTextOnlyAppendDatastore(unittest.TestCase):
    db_name = 'test_text_only_append_data'
    db_path = f'{db_name}'
    segment_size_threshold = 64

    @classmethod
    def setUpClass(cls):
        # Create the test_db directory if it doesn't exist
        if not os.path.exists(cls.db_path):
            os.makedirs(cls.db_path)

    @classmethod
    def tearDownClass(cls):
        # Remove the test_db directory after tests
        if os.path.exists(cls.db_path):
            shutil.rmtree(cls.db_path)

    def setUp(self):
        # Instantiate the Datastore before each test
        self.datastore = TextOnlyAppendDatastore(self.segment_size_threshold, self.db_name)

    def tearDown(self):
        # Close the datastore and remove files after each test
        self.datastore.current_segment.close()
        files = os.listdir(self.db_path)
        for f in files:
            os.remove(os.path.join(self.db_path, f))

    def test_set_and_get(self):
        # Test setting a key and retrieving it
        key, value = 'test_key', 'test_value'
        self.datastore.set(key, value)
        retrieved_value = self.datastore.get(key)
        self.assertIsNotNone(retrieved_value)
        self.assertEqual(retrieved_value[key], value)

    def test_compaction(self):
        # Test the compaction process
        keys_values = {
            'apple': 3,
            'banana': 2,
            'cherry': 5
        }

        for key, value in keys_values.items():
            self.datastore.set(key, value)

        self.datastore.compact()

        # Ensure the data is still retrievable after compaction
        for key in keys_values:
            retrieved_value = self.datastore.get(key)
            self.assertIsNotNone(retrieved_value)
            self.assertEqual(retrieved_value[key], keys_values[key])

        # Verify that only one segment file exists after compaction
        self.assertEqual(len(os.listdir(self.db_path)), 1)


if __name__ == '__main__':
    unittest.main()

