# apache_beam_pipeline_example
Data pipeline example with a purpose to learn Apache Beam in Python.

## Task 1
1. Read the input from `gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv`
2. Find all transactions have a `transaction_amount` greater than `20`
3. Exclude all transactions made before the year `2010`
4. Sum the total by `date`
5. Save the output into `output/results.jsonl.gz` and make sure all files in the `output/` directory is git ignored

## Task 2
Following up on the same Apache Beam batch job, also do the following 
1. Group all transform steps into a single `Composite Transform`
2. Add a unit test to the Composite Transform using tooling / libraries provided by Apache Beam

# Getting Started
> Dependencies: Python

Clone this Git repository locally
```
git clone https://github.com/etiburske/apache_beam_pipeline_example.git
```
## Run pipeline
You can run the pipeline with a single command line. Here are the two options, depending on your machine setup.

### All required packages are installed
If all required packages from `requirements.txt` are already installed on your local machine, use the following command on `apache_beam_pipeline_example` folder:
```
python -m pipeline
```

### Windows
If you do not have all packages installed and are using Windows machine:
```
 .\get_transactions_by_date.ps1
```

## Results
The output file containing the results will be saved as `output\results.jsonl.gz`.

## Unittest
To run unittest locally:
```
python test/pipeline_test.py
```
This unittest will test the Composite Transform and other created classes or methods. 
It will also run automatically when new commits are send to this repository.