# source ----> https://towardsdatascience.com/data-pipelines-with-apache-beam-86cd8eb55fd8
import apache_beam as beam
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
 
from apache_beam.io import ReadFromAvro
from apache_beam.io import WriteToAvro
 
schema = fastavro.schema.parse_schema({
"type": "record",
"namespace": "AvroPubSubDemo",
"name": "Entity",
"fields": [
    {"name": "attr1", "type": "float"},
    {"name": "msg", "type": "string"}
],
})
schema = avro.schema.parse(open(“parquet_file.parqet”, “rb”).read())
 
avro_write = beam.Pipeline()
 content_4 = ( avro_write
 |beam.Create({'attr1': 2.2153305989805246, 'msg': 'Hi-2022-08-22 19:44:54.495819'})
 |beam.Map(lambda element: element)
 |beam.io.WriteToAvro(‘/content/output.avro’,schema=schema))
 
avro_write.run()

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.DEBUG)