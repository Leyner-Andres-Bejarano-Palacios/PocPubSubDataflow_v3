import argparse
from datetime import datetime
import logging
import random
import os
import sys
import avro
from avro import io as avroio
from avro import datafile
from avro import schema
import apache_beam as beam
from apache_beam import pvalue
from apache_beam.io import filebasedsink
from fastavro import parse_schema
from apache_beam.io.filesystem import CompressionTypes
from apache_beam import DoFn, GroupByKey, io, ParDo, Pipeline, PTransform, WindowInto, WithKeys
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows
from apache_beam.io.fileio import FileSink
from apache_beam.io.fileio import WriteToFiles
import fastavro
from apache_beam.io.fileio import FileSink
from apache_beam.io.fileio import WriteToFiles
import fastavro
from apache_beam.io import WriteToAvro
import apache_beam.transforms.window as window









BIGQUERY_SCHEMA = "attr1:FLOAT,msg:STRING"

class fn_check_schema(beam.DoFn):
    def process(self, element):
        import argparse
        from datetime import datetime
        import logging
        import random
        import os
        import sys
        import avro
        from avro import io as avroio
        from avro import datafile
        from avro import schema
        import apache_beam as beam
        from apache_beam import pvalue
        from apache_beam.io import filebasedsink
        from fastavro import parse_schema
        from apache_beam.io.filesystem import CompressionTypes
        from apache_beam import DoFn, GroupByKey, io, ParDo, Pipeline, PTransform, WindowInto, WithKeys
        from apache_beam.options.pipeline_options import PipelineOptions
        from apache_beam.transforms.window import FixedWindows
        from apache_beam.io.fileio import FileSink
        from apache_beam.io.fileio import WriteToFiles
        import fastavro
        from apache_beam.io.fileio import FileSink
        from apache_beam.io.fileio import WriteToFiles
        import fastavro
        from apache_beam.io import WriteToAvro
        import apache_beam.transforms.window as window
        parsed = json.loads(element.decode("utf-8"))
        correct = False
        if "attr1" in parsed and \
           "msg" in parsed:
            correct = True

        if correct == True:
            yield pvalue.TaggedOutput('Clean', parsed)
        else:
            yield pvalue.TaggedOutput('validationsDetected', parsed)




class OverridenClass(WriteToAvro):
    def expand(self, pcoll):
        output = (pcoll | ParDo(ProcessUnboundedRecordsFn(self._sink)))


class ProcessUnboundedRecordsFn(beam.DoFn):
    def __init__(self,sink):
        self._sink = sink    
    def process(self, record, window=DoFn.WindowParam):
        import argparse
        from datetime import datetime
        import logging
        import random
        import os
        import sys
        import avro
        from avro import io as avroio
        from avro import datafile
        from avro import schema
        from apache_beam import pvalue
        import apache_beam as beam
        from apache_beam.io import filebasedsink
        from fastavro import parse_schema
        from apache_beam.io.filesystem import CompressionTypes
        from apache_beam import DoFn, GroupByKey, io, ParDo, Pipeline, PTransform, WindowInto, WithKeys
        from apache_beam.options.pipeline_options import PipelineOptions
        from apache_beam.transforms.window import FixedWindows
        from apache_beam.io.fileio import FileSink
        from apache_beam.io.fileio import WriteToFiles
        import fastavro
        from apache_beam.io.fileio import FileSink
        from apache_beam.io.fileio import WriteToFiles
        import fastavro
        from apache_beam.io import WriteToAvro
        import apache_beam.transforms.window as window
        # ts_format = "%H:%M"
        # window_start = window.start.to_utc_datetime().strftime(ts_format)
        # window_end = window.end.to_utc_datetime().strftime(ts_format)
        # filename = "-".join([self.output_path, window_start, window_end, str(shard_id)])
        beam.io.iobase.Write(self._sink)

    


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"x-oxygen-360101-a0fc362da30c.json"
BIGQUERY_TABLE = "x-oxygen-360101:medium.medium_test"
BIGQUERY_SCHEMA = "timestamp:TIMESTAMP,attr1:FLOAT,msg:STRING"




class GroupMessagesByFixedWindows(PTransform):
    """A composite transform that groups Pub/Sub messages based on publish time
    and outputs a list of tuples, each containing a message and its publish time.
    """

    def __init__(self, window_size, num_shards=5):
        # Set window size to 60 seconds.
        self.window_size = int(window_size * 60)
        self.num_shards = num_shards

    def expand(self, pcoll):
        import argparse
        from datetime import datetime
        import logging
        import random
        import os
        import sys
        import avro
        from avro import io as avroio
        from apache_beam import pvalue
        from avro import datafile
        from avro import schema
        import apache_beam as beam
        from apache_beam.io import filebasedsink
        from fastavro import parse_schema
        from apache_beam.io.filesystem import CompressionTypes
        from apache_beam import DoFn, GroupByKey, io, ParDo, Pipeline, PTransform, WindowInto, WithKeys
        from apache_beam.options.pipeline_options import PipelineOptions
        from apache_beam.transforms.window import FixedWindows
        from apache_beam.io.fileio import FileSink
        from apache_beam.io.fileio import WriteToFiles
        import fastavro
        from apache_beam.io.fileio import FileSink
        from apache_beam.io.fileio import WriteToFiles
        import fastavro
        from apache_beam.io import WriteToAvro
        import apache_beam.transforms.window as window
        return (
            pcoll
            # Bind window info to each element using element timestamp (or publish time).
            | "Window into fixed intervals"
            >> WindowInto(FixedWindows(self.window_size))
            | "Add timestamp to windowed elements" >> ParDo(AddTimestamp())
            # Assign a random key to each windowed element based on the number of shards.
            | "Add key" >> WithKeys(lambda _: random.randint(0, self.num_shards - 1))
            # Group windowed elements by key. All the elements in the same window must fit
            # memory for this. If not, you need to use `beam.util.BatchElements`.
            | "Group by key" >> GroupByKey()
        )

class AddTimestamp(DoFn):
    def process(self, element, publish_time=DoFn.TimestampParam):
        """Processes each windowed element by extracting the message body and its
        publish time into a tuple.
        """
        import argparse
        from datetime import datetime
        import logging
        import random
        import os
        import sys
        import avro
        from avro import io as avroio
        from apache_beam import pvalue
        from avro import datafile
        from avro import schema
        import apache_beam as beam
        from apache_beam.io import filebasedsink
        from fastavro import parse_schema
        from apache_beam.io.filesystem import CompressionTypes
        from apache_beam import DoFn, GroupByKey, io, ParDo, Pipeline, PTransform, WindowInto, WithKeys
        from apache_beam.options.pipeline_options import PipelineOptions
        from apache_beam.transforms.window import FixedWindows
        from apache_beam.io.fileio import FileSink
        from apache_beam.io.fileio import WriteToFiles
        import fastavro
        from apache_beam.io.fileio import FileSink
        from apache_beam.io.fileio import WriteToFiles
        import fastavro
        from apache_beam.io import WriteToAvro
        import apache_beam.transforms.window as window
        yield (
            element.decode("utf-8"),
            datetime.utcfromtimestamp(float(publish_time)).strftime(
                "%Y-%m-%d %H:%M:%S.%f"
            ),
        )

class ExtractJsonFromKeyValuePair(DoFn):
    def process(self, key_value, window=DoFn.WindowParam):
        """Extract json from keyValue Pair generated in wwindows function."""
        import argparse
        from datetime import datetime
        import logging
        import random
        import os
        import sys
        import avro
        from avro import io as avroio
        from apache_beam import pvalue
        from avro import datafile
        from avro import schema
        import apache_beam as beam
        from apache_beam.io import filebasedsink
        from fastavro import parse_schema
        from apache_beam.io.filesystem import CompressionTypes
        from apache_beam import DoFn, GroupByKey, io, ParDo, Pipeline, PTransform, WindowInto, WithKeys
        from apache_beam.options.pipeline_options import PipelineOptions
        from apache_beam.transforms.window import FixedWindows
        from apache_beam.io.fileio import FileSink
        from apache_beam.io.fileio import WriteToFiles
        import fastavro
        from apache_beam.io.fileio import FileSink
        from apache_beam.io.fileio import WriteToFiles
        import fastavro
        from apache_beam.io import WriteToAvro
        import apache_beam.transforms.window as window
        shard_id, batch = key_value
        return [message_body for  message_body, publish_time in batch]

class FormatErrors(DoFn):
    def process(self, record, window=DoFn.WindowParam):
        """Format errors from bigQuery insert."""
        return Map(lambda record: {'value':{'attr1':str(record['attr1']),'msg':str(record['msg'])},\
                                'error':str(record['FailedRows']),\
                                'timestamp':str(record['timestamp'])})

def run(input_subscription, output_path, output_table, window_interval_sec, window_size=1.0, num_shards=5, pipeline_args=None):
    import argparse
    from datetime import datetime
    import logging
    import random
    import os
    import sys
    import avro
    from avro import io as avroio
    from apache_beam import pvalue
    from avro import datafile
    from avro import schema
    import apache_beam as beam
    from apache_beam.io import filebasedsink
    from fastavro import parse_schema
    from apache_beam.io.filesystem import CompressionTypes
    from apache_beam import DoFn, GroupByKey, io, ParDo, Pipeline, PTransform, WindowInto, WithKeys
    from apache_beam.options.pipeline_options import PipelineOptions
    from apache_beam.transforms.window import FixedWindows
    from apache_beam.io.fileio import FileSink
    from apache_beam.io.fileio import WriteToFiles
    import fastavro
    from apache_beam.io.fileio import FileSink
    from apache_beam.io.fileio import WriteToFiles
    import fastavro
    from apache_beam.io import WriteToAvro
    import apache_beam.transforms.window as window
    schema = fastavro.schema.parse_schema({
    "type": "record",
    "namespace": "AvroPubSubDemo",
    "name": "Entity",
    "fields": [
        {"name": "attr1", "type": ["float", "null"]},
        {"name": "msg", "type": ["string","null"]}
    ],
    })

    # Set `save_main_session` to True so DoFns can access globally imported modules.
    options1 = PipelineOptions(
    pipeline_args,
    runner='DataflowRunner',
    project='x-oxygen-360101',
    job_name='test-yohan-seb-13',
    temp_location='gs://temp-medium1/temp1',
    region='us-east1',
    service_account_email='684034867805-compute@developer.gserviceaccount.com',
    streaming = True)
    with Pipeline(options=options1) as pipeline:    
        results  = (
                pipeline
                # Because `timestamp_attribute` is unspecified in `ReadFromPubSub`, Beam
                # binds the publish time returned by the Pub/Sub server for each message
                # to the element's timestamp parameter, accessible via `DoFn.TimestampParam`.
                # https://beam.apache.org/releases/pydoc/current/apache_beam.io.gcp.pubsub.html#apache_beam.io.gcp.pubsub.ReadFromPubSub
                | "Read from Pub/Sub" >> io.ReadFromPubSub(subscription=input_subscription)
                # | "Window into" >> GroupMessagesByFixedWindows(window_size, num_shards)
                | "Fixed-size windows" >> beam.WindowInto(window.FixedWindows(window_interval_sec, 0))
                # | "Extract json from key value pair" >> ParDo(ExtractJsonFromKeyValuePair())
                |  beam.ParDo(fn_check_schema()).with_outputs()
            )

        errors = (results["Clean"] 
        | "Write to Big Query" >> beam.io.WriteToBigQuery(
            BIGQUERY_TABLE,
            #table_FALABELLA,
            schema=BIGQUERY_SCHEMA,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            #create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            #write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
            )
        | "Format errors" >> ParDo(FormatErrors())
        | "Write to Big Query dead letter" >> beam.io.WriteToBigQuery(
            "x-oxygen-360101:medium.medium_test",
            #table_FALABELLA,
            schema="value:STRING,error:STRING,timestamp:STRING",
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            #create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            #write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
        ))
    

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.DEBUG)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_subscription",
        help="The Cloud Pub/Sub topic to read from."
        '"projects/<PROJECT_ID>/topics/<TOPIC_ID>".',
    )
    parser.add_argument(
        "--window_size",
        type=float,
        default=1.0,
        help="Output file's window size in minutes.",
    )
    parser.add_argument(
        "--output_path",
        help="Path of the output GCS file including the prefix.",
    )
    parser.add_argument(
        "--num_shards",
        type=int,
        default=5,
        help="Number of shards to use when writing windowed elements to GCS.",
    )

    parser.add_argument(
        "--window_interval_sec",
        default=1,
        type=int,
        help="Window interval in seconds for grouping incoming messages.",
    )

    parser.add_argument(
        "--output_table",
        #default="x-oxygen-360101:falabella.Test1", 
        help="Output BigQuery table for results specified as: "
        "PROJECT:DATASET.TABLE or DATASET.TABLE.",
        default=BIGQUERY_TABLE
    )

    known_args, pipeline_args = parser.parse_known_args()

    run(
        known_args.input_subscription,
        known_args.output_path,
        known_args.output_table,
        known_args.window_interval_sec,
        known_args.window_size,
        known_args.num_shards,
        pipeline_args,
    )
