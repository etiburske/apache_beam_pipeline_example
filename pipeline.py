""" Apache Beam pipeline example

This pipeline gets an input transactions table from a public repository and 
performs transformations in order to get the total amout of transactions per date.

It is divided into 'task_1' and 'task_2' in order to show how the same transformations 
could be called by a Apache Beam Composite Transform.
"""

# Standard Imports
from datetime import datetime
import json

# Apache Beam Imports
import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.options.pipeline_options import PipelineOptions

# Global Variables
INPUT_TABLE = 'gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv'
OUTPUT_PATH = 'output/results'

class GetDateFromTimestamp(beam.DoFn):
    """ DoFn class to add 'date' attribute based on 'timestamp'
    """
    def process(self, element):
        """ Method to add 'date' attribute based on 'timestamp'
            Args:
                element (beam.Row): The element should have already a 'timestamp' attribute
            Yield:
                element (beam.Row): Modified element with the new attribute
        """
        # Converts timestamp from string to datetime
        element_timestamp = datetime.strptime(element.timestamp, '%Y-%m-%d %H:%M:%S %Z')
        # Get date and assign to new attribute
        new_row = {
            'timestamp': element.timestamp,
            'origin': element.origin,
            'destination': element.destination,
            'transaction_amount': element.transaction_amount,
            'date': element_timestamp.date()
            }

        yield beam.Row(**new_row)

class ExtractDateAndAmount(beam.DoFn):
    """ DoFn class to extract only 'date' and 'transaction_amount' attributes from the PCollection element
    """
    def process(self, element):
        """ Method to extract only 'date' and 'transaction_amount' attributes from the PCollection element
            Args:
                element (beam.Row): PCollection element containing attributes 'timestamp', 'origin', 'destination', 'transaction_amount', 'date'
            Yield:
                (tuple): Tupple with 'date' and 'transaction_amount'
        """
        yield (element.date.strftime('%Y-%m-%d'), float(element.transaction_amount))

class TupleToJson(beam.DoFn):
    """ DoFn class to convert Tuple to Json format for elements containing 'date' and 'total_amount'.
    """
    def process(self, element):
        """ Method to convert Tuple to Json format for elements containing 'date' and 'total_amount' data.
            Args:
                element (tuple): Tuple element with 'date' and 'total_amount'
            Yield:
                (json): Json encoded string with 'date' and 'transaction_amount' attributes
        """
        # Convert the tuple to a dictionary
        data = {'date': element[0], 'total_amount': element[1]}
        # Convert the dictionary to a JSON string
        json_string = json.dumps(data)
        # Yield the JSON string as output
        yield json_string.encode('utf-8')

class GetTransactionsAmountByDate(beam.PTransform):
    """ Composite Transform to nest multiple transforms
    """
    def __init__(self, output=OUTPUT_PATH):
        """ PTransform constructor
            Args:
                output (str): output path where results should be stored. This path does not include file format extention.
        """
        self.output = output
        
    def expand(self, pcoll):
        """ Method to apply the following transformations to PCollection:
                1. Filter transaction_amount > 20
                2. Exclude transactions before year 2010
                3. Sum the total by `date`
                4. Save the output into `output/results.jsonl.gz`

            Args:
                pcoll (PCollection): string containing 'timestamp', 'origin', 'destination', 'transaction_amount' separated by ','
            Return:
                PCollection containing filename with results after all transformations
        """
        output = (
            pcoll
            # Add Schema containing column names to the csv table and convert it into Beam Rows
            | 'AddSchemaCT' >> beam.Map(
                lambda x: beam.Row(
                    timestamp=str(x[0]),
                    origin=str(x[1]),
                    destination=str(x[2]),
                    transaction_amount=float(x[3])))
            # Add 'date' attribute to PCollection, so it is easier to make transformations based on date
            | 'GetDateFromTimestampCT' >> beam.ParDo(GetDateFromTimestamp())
            # Filter by transaction_amount larger than 20
            | 'FilterAmountCT' >> beam.Filter(lambda x: x.transaction_amount > 20)
            # Exclude transactions before year 2010
            | 'FilterYearCT' >> beam.Filter(lambda x: x.date.year > 2009)
            # Extract only 'date' and 'transaction_amount' attributes
            | 'ExtractKeyValueCT' >> beam.ParDo(ExtractDateAndAmount())
            # Convert into key/value pairs. Key: 'date', value: 'transaction_amount'
            | 'GroupByCT' >> beam.GroupByKey()
            # Sum transaction_amount for each date
            | 'CombineSumCT' >> beam.CombineValues(sum)
            # Convert from Tuple to Json string format
            | 'ConvertToJsonCT' >> beam.ParDo(TupleToJson())
            # Save output result to file
            | 'SaveToFileCT' >> WriteToText(self.output, file_name_suffix='.jsonl.gz', shard_name_template='')
        )
        return output

def parse_lines(element):
    """ Method to split csv string
        Args:
            element (PCollection str element): Input string element to be split into comma separated values
        Return:
            (list) list of strings divided by ','
    """
    return element.split(",")


def run():
    """ Method to define and run the pipeline
        Args:
            None
        Return:
            None
    """
    # Construct pipeline object
    with beam.Pipeline() as p:

        # Get input into a PCollection called 'table_input'
        table_input = (
            p
            # Read input data from external source, skipping header
            | 'ReadMyFile' >> ReadFromText(INPUT_TABLE, skip_header_lines=1)
            # Split strings
            | 'ParseLines' >> beam.Map(parse_lines)
        )

        # Task 1: Make transformations to input
        task_1 = (
            table_input
            # Add Schema containing column names to the csv table and convert it into Beam Rows
            | 'AddSchema' >> beam.Map(
                lambda x: beam.Row(
                    timestamp=str(x[0]),
                    origin=str(x[1]),
                    destination=str(x[2]),
                    transaction_amount=float(x[3])))
            # Add 'date' attribute to PCollection, so it is easier to make transformations based on date
            | 'GetDateFromTimestamp' >> beam.ParDo(GetDateFromTimestamp())
            # Filter by transaction_amount larger than 20
            | 'FilterAmount' >> beam.Filter(lambda x: x.transaction_amount > 20)
            # Exclude transactions before year 2010
            | 'FilterYear' >> beam.Filter(lambda x: x.date.year > 2009)
            # Extract only 'date' and 'transaction_amount' attributes
            | 'ExtractKeyValue' >> beam.ParDo(ExtractDateAndAmount())
            # Convert into key/value pairs. Key: 'date', value: 'transaction_amount'
            | 'GroupBy' >> beam.GroupByKey()
            # Sum transaction_amount for each date
            | 'CombineSum' >> beam.CombineValues(sum)
            # Convert from Tuple to Json string format
            | 'ConvertToJson' >> beam.ParDo(TupleToJson())
            # Save result to output file
            | 'SaveToFile' >> WriteToText(OUTPUT_PATH, file_name_suffix='.jsonl.gz', shard_name_template='')\
        )

        # Task 2: transform steps above into a Composite Transform. Both task_1 and task_2 will have the same output and 
        # same transformations, for learning purposes.
        task_2 = (
            table_input
            # Call Composite Transform
            | 'GetTransactionsAmountByDateCT' >> GetTransactionsAmountByDate()
        )

if __name__ == '__main__':
    run()