# PocPubSubDataflow_v3

## Command for executing

python test_gcs.py --runner DataFlowRunner --streaming  --input_subscription projects/x-oxygen-360101/subscriptions/sub-falabella --output_table x-oxygen-360101:medium.medium_test --key "pruebatest" --output_schema "timestamp:TIMESTAMP,attr1:FLOAT,msg:STRING" --project x-oxygen-360101  --output_path gs://test-carga1/test/ --region us-east1  --temp_location gs://temp-medium1/temp1  --staging_location gs://stag-bucket2/stg1  --numWorkers=2 --maxNumWorkers=2 --requirements_file ./requirements1.txt --save_main_session=True

## Command for executing avro writter in batch

python AvroWritterInBatch.py --numWorkers=2 --maxNumWorkers=2 --requirements_file ./requirements1.txt
