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


def _use_fastavro():
  return sys.version_info[0] >= 3

class WriteToAvro(PTransform):
  """A ``PTransform`` for writing avro files."""

  def __init__(self,
               file_path_prefix,
               schema,
               codec='deflate',
               file_name_suffix='',
               num_shards=0,
               shard_name_template=None,
               mime_type='application/x-avro',
               use_fastavro=_use_fastavro()):
    """Initialize a WriteToAvro transform.

    Args:
      file_path_prefix: The file path to write to. The files written will begin
        with this prefix, followed by a shard identifier (see num_shards), and
        end in a common extension, if given by file_name_suffix. In most cases,
        only this argument is specified and num_shards, shard_name_template, and
        file_name_suffix use default values.
      schema: The schema to use, as returned by avro.schema.Parse
      codec: The codec to use for block-level compression. Any string supported
        by the Avro specification is accepted (for example 'null').
      file_name_suffix: Suffix for the files written.
      num_shards: The number of files (shards) used for output. If not set, the
        service will decide on the optimal number of shards.
        Constraining the number of shards is likely to reduce
        the performance of a pipeline.  Setting this value is not recommended
        unless you require a specific number of output files.
      shard_name_template: A template string containing placeholders for
        the shard number and shard count. When constructing a filename for a
        particular shard number, the upper-case letters 'S' and 'N' are
        replaced with the 0-padded shard number and shard count respectively.
        This argument can be '' in which case it behaves as if num_shards was
        set to 1 and only one file will be generated. The default pattern used
        is '-SSSSS-of-NNNNN' if None is passed as the shard_name_template.
      mime_type: The MIME type to use for the produced files, if the filesystem
        supports specifying MIME types.
      use_fastavro: when set, use the `fastavro` library for IO

    Returns:
      A WriteToAvro transform usable for writing.
    """
    self._sink = \
      _create_avro_sink(
          file_path_prefix,
          schema,
          codec,
          file_name_suffix,
          num_shards,
          shard_name_template,
          mime_type,
          use_fastavro
      )

  def expand(self, pcoll):
    output = (pcoll | ParDo(ProcessUnboundedRecordsFn(self._sink)))
    # p = pcoll.pipeline
    # return pcoll | beam.io.iobase.Write(self._sink)

def _create_avro_sink(file_path_prefix,
                      schema,
                      codec,
                      file_name_suffix,
                      num_shards,
                      shard_name_template,
                      mime_type,
                      use_fastavro):
  return \
      _FastAvroSink(
          file_path_prefix,
          schema,
          codec,
          file_name_suffix,
          num_shards,
          shard_name_template,
          mime_type
      ) \
      if use_fastavro \
      else \
      _AvroSink(
          file_path_prefix,
          schema,
          codec,
          file_name_suffix,
          num_shards,
          shard_name_template,
          mime_type
      )

class _BaseAvroSink(filebasedsink.FileBasedSink):
  """A base for a sink for avro files. """
  def __init__(self,
               file_path_prefix,
               schema,
               codec,
               file_name_suffix,
               num_shards,
               shard_name_template,
               mime_type):
    super(_BaseAvroSink, self).__init__(
        file_path_prefix,
        file_name_suffix=file_name_suffix,
        num_shards=num_shards,
        shard_name_template=shard_name_template,
        coder=None,
        mime_type=mime_type,
        # Compression happens at the block level using the supplied codec, and
        # not at the file level.
        compression_type=CompressionTypes.UNCOMPRESSED)
    self._schema = schema
    self._codec = codec

  def display_data(self):
    res = super(_BaseAvroSink, self).display_data()
    res['codec'] = str(self._codec)
    res['schema'] = str(self._schema)
    return res


class _AvroSink(_BaseAvroSink):
  """A sink for avro files using Avro. """
  def open(self, temp_path):
    file_handle = super(_AvroSink, self).open(temp_path)
    return avro.datafile.DataFileWriter(
        file_handle, avro.io.DatumWriter(), self._schema, self._codec)

  def write_record(self, writer, value):
    writer.append(value)


class _FastAvroSink(_BaseAvroSink):
  """A sink for avro files using FastAvro. """
  def open(self, temp_path):
    file_handle = super(_FastAvroSink, self).open(temp_path)
    return Writer(file_handle, self._schema, self._codec)

  def write_record(self, writer, value):
    writer.write(value)

  def close(self, writer):
    writer.flush()
    writer.fo.close()


class ProcessUnboundedRecordsFn(beam.DoFn):
    def __init__(self,sink):
        self._sink = sink    
    def process(self, key_value, window=DoFn.WindowParam):
        # ts_format = "%H:%M"
        # window_start = window.start.to_utc_datetime().strftime(ts_format)
        # window_end = window.end.to_utc_datetime().strftime(ts_format)
        # shard_id, batch = key_value
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
    from datetime import datetime
    def process(self, element, publish_time=DoFn.TimestampParam):
        """Processes each windowed element by extracting the message body and its
        publish time into a tuple.
        """
        yield (
            element.decode("utf-8"),
            datetime.utcfromtimestamp(float(publish_time)).strftime(
                "%Y-%m-%d %H:%M:%S.%f"
            ),
        )

class ExtractJsonFromKeyValuePair(DoFn):
    def process(self, key_value, window=DoFn.WindowParam):
        """Extract json from keyValue Pair generated in wwindows function."""
        shard_id, batch = key_value
        return [message_body for  message_body, publish_time in batch]

def run(input_subscription, output_path, output_table, window_interval_sec, window_size=1.0, num_shards=5, pipeline_args=None):
    schema = fastavro.schema.parse_schema({
        "type": "record",
        "namespace": "AvroPubSubDemo",
        "name": "Entity",
        "fields": [
            {"name": "attr1", "type": "float"},
            {"name": "msg", "type": "string"}
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
    streaming = True,
    save_main_session= True,
    )

    with Pipeline(options=options1) as pipeline:
        (
            pipeline
            # Because `timestamp_attribute` is unspecified in `ReadFromPubSub`, Beam
            # binds the publish time returned by the Pub/Sub server for each message
            # to the element's timestamp parameter, accessible via `DoFn.TimestampParam`.
            # https://beam.apache.org/releases/pydoc/current/apache_beam.io.gcp.pubsub.html#apache_beam.io.gcp.pubsub.ReadFromPubSub
            | "Read from Pub/Sub" >> io.ReadFromPubSub(subscription=input_subscription)
            | "Window into" >> GroupMessagesByFixedWindows(window_size, num_shards)
            | "Extract json from key value pair" >> ParDo(ExtractJsonFromKeyValuePair())
            | "write avros" >> WriteToAvro(known_args.output_path, schema=schema)          

        )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

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









# import argparse
# from datetime import datetime
# import logging
# import random
# import os
# import apache_beam as beam
# from apache_beam.io import WriteToAvro
# from apache_beam import DoFn, GroupByKey, io, ParDo, Pipeline, PTransform, WindowInto, WithKeys
# from apache_beam.options.pipeline_options import PipelineOptions
# from apache_beam.transforms.window import FixedWindows
# from fastavro import parse_schema, schemaless_reader, schemaless_writer


# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"x-oxygen-360101-a0fc362da30c.json"

# BIGQUERY_TABLE = "x-oxygen-360101:medium.medium_test"
# BIGQUERY_SCHEMA = "timestamp:TIMESTAMP,attr1:FLOAT,msg:STRING"

# raw_schema = {
#         "type": "record",
#         "namespace": "AvroPubSubDemo",
#         "name": "Entity",
#         "fields": [
#             {"name": "attr1", "type": "float"},
#             {"name": "msg", "type": "string"}
#         ],
#     }

# class GroupMessagesByFixedWindows(PTransform):
#     """A composite transform that groups Pub/Sub messages based on publish time
#     and outputs a list of tuples, each containing a message and its publish time.
#     """

#     def __init__(self, window_size, num_shards=5):
#         # Set window size to 60 seconds.
#         self.window_size = int(window_size * 60)
#         self.num_shards = num_shards

#     def expand(self, pcoll):
#         return (
#             pcoll
#             # Bind window info to each element using element timestamp (or publish time).
#             | "Window into fixed intervals"
#             >> WindowInto(FixedWindows(self.window_size))
#             | "Add timestamp to windowed elements" >> ParDo(AddTimestamp())
#             # Assign a random key to each windowed element based on the number of shards.
#             | "Add key" >> WithKeys(lambda _: random.randint(0, self.num_shards - 1))
#             # Group windowed elements by key. All the elements in the same window must fit
#             # memory for this. If not, you need to use `beam.util.BatchElements`.
#             | "Group by key" >> GroupByKey()
#         )

# class AddTimestamp(DoFn):
#     def process(self, element, publish_time=DoFn.TimestampParam):
#         """Processes each windowed element by extracting the message body and its
#         publish time into a tuple.
#         """
#         yield (
#             element.decode("utf-8"),
#             datetime.utcfromtimestamp(float(publish_time)).strftime(
#                 "%Y-%m-%d %H:%M:%S.%f"
#             ),
#         )



# class ExtractJsonFromKeyValuePair(DoFn):
#     def process(self, key_value, window=DoFn.WindowParam):
#         """Write messages in a batch to Google Cloud Storage."""

#         ts_format = "%H:%M"
#         window_start = window.start.to_utc_datetime().strftime(ts_format)
#         window_end = window.end.to_utc_datetime().strftime(ts_format)        
#         shard_id, batch = key_value
#         filename = "-".join([self.output_path, window_start, window_end, str(shard_id)])
#         return [message_body for message_body, publish_time in batch]


# def run(input_subscription, output_path, output_table, window_interval_sec, window_size=1.0, num_shards=5, pipeline_args=None):
#     # Set `save_main_session` to True so DoFns can access globally imported modules.
#     options1 = PipelineOptions(
#     pipeline_args,
#     runner='DataflowRunner',
#     project='x-oxygen-360101',
#     job_name='test-yohan-seb-11',
#     temp_location='gs://temp-medium1/temp1',
#     region='us-east1',
#     service_account_email='684034867805-compute@developer.gserviceaccount.com',
#     streaming = True,
#     save_main_session= True,
#     )

#     with Pipeline(options=options1) as pipeline:
#         (
#             pipeline
#             # Because `timestamp_attribute` is unspecified in `ReadFromPubSub`, Beam
#             # binds the publish time returned by the Pub/Sub server for each message
#             # to the element's timestamp parameter, accessible via `DoFn.TimestampParam`.
#             # https://beam.apache.org/releases/pydoc/current/apache_beam.io.gcp.pubsub.html#apache_beam.io.gcp.pubsub.ReadFromPubSub
#             | "Read from Pub/Sub" >> io.ReadFromPubSub(subscription=input_subscription)
#             | "Window into" >> GroupMessagesByFixedWindows(window_size, num_shards)
#             | "Extract Json" >> ParDo(ExtractJsonFromKeyValuePair())
#             | "Write to GCS" >> WriteToAvro(known_args.output_path, schea=schema, file_name_suffix='.avro')
#         )

# if __name__ == "__main__":
#     logging.getLogger().setLevel(logging.INFO)
#     schema = parse_schema(raw_schema)
#     parser = argparse.ArgumentParser()
#     parser.add_argument(
#         "--input_subscription",
#         help="The Cloud Pub/Sub topic to read from."
#         '"projects/<PROJECT_ID>/topics/<TOPIC_ID>".',
#     )
#     parser.add_argument(
#         "--window_size",
#         type=float,
#         default=1.0,
#         help="Output file's window size in minutes.",
#     )
#     parser.add_argument(
#         "--output_path",
#         help="Path of the output GCS file including the prefix.",
#     )
#     parser.add_argument(
#         "--num_shards",
#         type=int,
#         default=5,
#         help="Number of shards to use when writing windowed elements to GCS.",
#     )

#     parser.add_argument(
#         "--window_interval_sec",
#         default=1,
#         type=int,
#         help="Window interval in seconds for grouping incoming messages.",
#     )

#     parser.add_argument(
#         "--output_table",
#         #default="x-oxygen-360101:falabella.Test1", 
#         help="Output BigQuery table for results specified as: "
#         "PROJECT:DATASET.TABLE or DATASET.TABLE.",
#         default=BIGQUERY_TABLE
#     )

#     known_args, pipeline_args = parser.parse_known_args()

#     run(
#         known_args.input_subscription,
#         known_args.output_path,
#         known_args.output_table,
#         known_args.window_interval_sec,
#         known_args.window_size,
#         known_args.num_shards,
#         pipeline_args,
#     )
