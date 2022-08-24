import json
import avro
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

schema = avro.schema.parse(json.dumps(write_schema))
writer = DataFileWriter(open("users.avro", "wb"), DatumWriter(), schema)
writer.append({"name": "Alyssa", "favorite_number": 256})
writer.append({"name": "Ben", "favorite_number": 7, "favorite_color": "red"})
writer.close()