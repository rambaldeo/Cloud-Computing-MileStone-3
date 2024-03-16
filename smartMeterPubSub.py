import argparse
import json
import logging
import os

import apache_beam as beam
import mysql.connector
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

class FilterMissingMeasurements(beam.DoFn):
    def process(self, element):
        if all(val is not None for val in element.values()):
            yield element

class ConvertMeasurements(beam.DoFn):
    def process(self, element):
        element['Pressure_psi'] = element['pressure'] / 6.895
        element['Temperature_F'] = element['temperature'] * 1.8 + 32
        yield element

def write_to_mysql(element, mysql_connection):
    cursor = mysql_connection.cursor()
    insert_query = "INSERT INTO your_table (column_name) VALUES (%s)"
    cursor.execute(insert_query, (json.dumps(element),))
    mysql_connection.commit()
    cursor.close()

def run(argv=None):
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--input', dest='input', required=True, help='Input Pub/Sub topic to read from.')
    parser.add_argument('--output', dest='output', required=True, help='Output Pub/Sub topic to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    # Initialize MySQL connection
    mysql_host = '34.118.135.16'
    mysql_user = 'usr'
    mysql_password = 'sofe4630u'
    mysql_database = 'Readings'
    mysql_connection = mysql.connector.connect(
        host=mysql_host,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database
    )

    with beam.Pipeline(options=pipeline_options) as p:
        measurements = (p | 'Read from Pub/Sub' >> beam.io.ReadFromPubSub(topic=known_args.input)
                         | 'Parse JSON' >> beam.Map(lambda x: json.loads(x)))

        filtered_measurements = measurements | 'Filter missing measurements' >> beam.ParDo(FilterMissingMeasurements())

        converted_measurements = filtered_measurements | 'Convert measurements' >> beam.ParDo(ConvertMeasurements())

        converted_measurements | 'Write to MySQL' >> beam.Map(write_to_mysql, mysql_connection)

        (converted_measurements | 'Convert to JSON' >> beam.Map(lambda x: json.dumps(x))
                               | 'Write to Pub/Sub' >> beam.io.WriteToPubSub(topic=known_args.output))

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
