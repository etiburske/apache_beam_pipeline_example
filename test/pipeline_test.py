""" Apache Beam unittest pipeline example

This unittest example tests the pipeline.py Composite Transform and other methods
"""

# Standard Imports
import unittest
import sys
from datetime import datetime
import json

# Apache Beam Imports
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

# Local Imports
sys.path.append('../apache_beam_pipeline_example')
import pipeline

# Global Variables
TEST_FILE_OUTPUT = 'test/output/results'

class TransactionsByDateTest(unittest.TestCase):
    """ Class to unittest TransactionsByDate Composite Transform class
    """

    def test_transactions(self):
        """ Method to create Test Pipeline and test GetTransactionsAmountByDate Composite Transform
        """
        # Static input data, which will make up the initial PCollection.
        input = [
            beam.Row(
                timestamp="2022-01-01 12:00:00 UTC",
                origin="A",
                destination="B",
                transaction_amount=30.0,
            ),
            beam.Row(
                timestamp="2022-01-02 12:00:00 UTC",
                origin="B",
                destination="C",
                transaction_amount=10.0,
            ),
            beam.Row(
                timestamp="2022-01-02 12:00:00 UTC",
                origin="C",
                destination="D",
                transaction_amount=25.0,
            ),
            beam.Row(
                timestamp="2005-01-02 12:00:00 UTC",
                origin="C",
                destination="D",
                transaction_amount=25.0,
            ),
        ]
        # Expected resuts after running Composite Transform for the given input
        expected_output = [
            '{"date": "2022-01-01", "total_amount": 30.0}',
            '{"date": "2022-01-02", "total_amount": 25.0}',
        ]

        # Create a test pipeline to run the Composite Transform
        with TestPipeline() as p1:
            pcoll = (
                p1 
                # Create an input PCollection
                | 'Test:CreateInput' >> beam.Create(input)
                # Apply the Composite Transform under test
                | 'Test:RunComposite' >> pipeline.GetTransactionsAmountByDate(TEST_FILE_OUTPUT)
            )

        # Created p2 such that the output file gets created in p1 and can be read in p2
        with TestPipeline() as p2:
            out = (
                p2
                | 'Test:ReadOutput' >> ReadFromText(TEST_FILE_OUTPUT + '.jsonl.gz', compression_type='gzip')
            )
            assert_that(out, equal_to(expected_output), label="Test:CheckOutput")

class GetDateFromTimestampTest(unittest.TestCase):
    """ Class to test GetDateFromTimestamp() class
    """
    def test_process(self):
        # Create static input and expected output
        input = beam.Row(
                timestamp="2022-01-01 12:00:00 UTC",
                origin="A",
                destination="B",
                transaction_amount=30.0,
            )
        expected_output = beam.Row(
                timestamp="2022-01-01 12:00:00 UTC",
                origin="A",
                destination="B",
                transaction_amount=30.0,
                date = datetime.date(datetime(2022, 1, 1)),
            )

        # Create a test pipeline
        with TestPipeline() as p:
            # Apply the transformation to the test element
            result = (
                p
                | beam.Create([input])
                | beam.ParDo(pipeline.GetDateFromTimestamp())
            )

            # Assert the result has the expected date attribute value
            assert_that(result, equal_to([expected_output]))

class ExtractDateAndAmountTest(unittest.TestCase):
    """ Class to test ExtractDateAndAmount() class
    """
    def test_process(self):
        # Create static input and expected output
        input = beam.Row(
                timestamp="2022-01-01 12:00:00 UTC",
                origin="A",
                destination="B",
                transaction_amount=30.0,
                date=datetime.date(datetime(2022, 1, 1)),
            )
        expected_output = ('2022-01-01', 30.0)

        # Create a test pipeline
        with TestPipeline() as p:
            # Apply the transformation to the test element
            result = (
                p
                | beam.Create([input])
                | beam.ParDo(pipeline.ExtractDateAndAmount())
            )

            # Assert the result will be yield as tuple
            assert_that(result, equal_to([expected_output]))

class TupleToJsonTest(unittest.TestCase):
    """ Class to test TupleToJson() class
    """
    def test_process(self):
        # Create static input and expected output
        input = ('2022-01-01', 30.0)
        expected_output = json.dumps({'date': '2022-01-01', 'total_amount': 30.0}).encode('utf-8')

        # Create a test pipeline
        with TestPipeline() as p:
            # Apply the transformation to the test element
            result = (
                p
                | beam.Create([input])
                | beam.ParDo(pipeline.TupleToJson())
            )

            # Assert the result will be yield as the expected_output
            assert_that(result, equal_to([expected_output]))

class ParseLinesTest(unittest.TestCase):
    """ Class to test parse_lines() method
    """
    def test_process(self):
        # Create static input and expected output
        input = 'ab,bc,defgh'
        expected_output = ['ab','bc','defgh']

        # Create a test pipeline
        with TestPipeline() as p:
            # Apply the transformation to the test element
            result = (
                p
                | beam.Create([input])
                | beam.Map(pipeline.parse_lines)
            )

            # Assert the result will be returned as the expected_output
            assert_that(result, equal_to([expected_output]))

if __name__ == '__main__':
    unittest.main()
