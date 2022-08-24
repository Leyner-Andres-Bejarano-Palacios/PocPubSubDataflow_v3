import fastavro
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

schema = fastavro.schema.parse_schema({
"type": "record",
"namespace": "AvroPubSubDemo",
"name": "Entity",
"fields": [
    {"name": "attr1", "type": ["float", "null"]},
    {"name": "msg", "type": ["string","null"]}
],
})


reader = DataFileReader(open("test_output.avro-00000-of-00001", "rb"), DatumReader())
for user in reader:
    print(user)
reader.close()