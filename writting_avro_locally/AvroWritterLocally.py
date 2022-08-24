import json
import avro
from google.cloud import storage
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

write_schema = {
    "namespace": "example.avro",
    "type": "record",
    "name": "User",
    "fields": [
         {"name": "name", "type": "string"},
         {"name": "favorite_number", "type": ["int", "null"]},
         {"name": "favorite_color", "type": ["string", "null"]}
     ]
}

client = storage.Client()
bucket = client.get_bucket('test-carga1')
blob = bucket.blob('gs://test-carga1/test/users.avro')
schema = avro.schema.parse(json.dumps(write_schema))
writer = DataFileWriter(blob.open(mode='w'), DatumWriter(), schema)
writer.append({"name": "Alyssa", "favorite_number": 256})
writer.append({"name": "Ben", "favorite_number": 7, "favorite_color": "red"})
writer.close()