# source ----> https://towardsdatascience.com/data-pipelines-with-apache-beam-86cd8eb55fd8



import fastavro
import argparse
import avro.schema
import apache_beam as beam
from apache_beam.io import WriteToAvro
from apache_beam.io import ReadFromAvro
from avro.io import DatumReader, DatumWriter
from avro.datafile import DataFileReader, DataFileWriter
 

 


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args()
    schema = fastavro.schema.parse_schema({
    "type": "record",
    "namespace": "AvroPubSubDemo",
    "name": "Entity",
    "fields": [
        {"name": "attr1", "type": "float"},
        {"name": "msg", "type": "string"}
    ],
    })
    options1 = PipelineOptions(
    argv=pipeline_args,
    runner='DataflowRunner',
    project='x-oxygen-360101',
    job_name='yohan-lule-dantodomingo-1',
    temp_location='gs://temp-medium1/temp1',
    region='us-east1',
    service_account_email='684034867805-compute@developer.gserviceaccount.com',
    save_main_session= True)
    #schema = avro.schema.parse(open(“parquet_file.parqet”, “rb”).read())
    logging.getLogger().setLevel(logging.DEBUG)
    avro_write = beam.Pipeline(options=options1)
    content_4 = ( avro_write
    |beam.Create({'attr1': 2.2153305989805246, 'msg': 'Hi-2022-08-22 19:44:54.495819'})
    |beam.Map(lambda element: element)
    |beam.io.WriteToAvro('gs://test-carga1/test/output.avro',schema=schema))

    avro_write.run()