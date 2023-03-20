""" Apache Beam unittest pipeline example

This unittest example tests the pipeline.py Composite Transform
"""

# Standard Imports
import unittest
import sys
import os
import time

# Apache Beam Imports
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

# Local Imports
sys.path.append('../apache_beam_pipeline_example')
from pipeline import GetTransactionsAmountByDate

# Global Variables
current_dir = os.getcwd()
print(current_dir)
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

        # Create a test pipeline.
        with TestPipeline() as p:
            # Create an input PCollection
            pcoll = (
                p 
                | 'Test:CreateInput' >> beam.Create(input)
                # Apply the Composite Transform under test
                | 'Test:RunComposite' >> GetTransactionsAmountByDate(TEST_FILE_OUTPUT)
                # Read test output file
                | 'Test:ReadOutput' >> ReadFromText(TEST_FILE_OUTPUT + '.jsonl.gz', validate=False)
            )

        # Assert on the results
        assert_that(pcoll, equal_to(expected_output), label="Test:CheckOutput")


if __name__ == '__main__':
    unittest.main()
