See the wiki at https://git.tc.bbn.com/bbn/ta3-api-bindings-python/wikis/home for a full descrition

# Quick start

1. Start the starc services (See https://git.tc.bbn.com/bbn/tc-in-a-box#automated-testing-starting-the-services-above-using-salt)

2. Run produce_file to produce random records, write them to a file, and publish them to a topic, TestRandomRecords.  No additional command line arguments are needed.

   python produce_file.py

3. Run publish_from_file to read the previously produced file of randomly generated records and publish them to a different topic, TestFileRecords.

   python publish_from_file.py

4. Run consume_file to read 100 records from TestFileRecords, and write them to another file in string format

   python consume_file.py
