# Donation Analytics

Code base for Insight Data Engineering Challenge - Donation Analytics. To execute the data pipeline run the
`run.sh` file.

# Dependencies
Following are the libraries required to run the pipeline
1. multiprocessing - used for running multiple processes
2. math - used for the mathematical operations
3. time - used for its sleep function
4. json - read and parse the config files

# Architecture Overview
The data pipeline is divided into two modules. The first reads the file/input line by line and puts them onto a queue, the second component reads the data from the queue and applies the logic required to generate an output that would store the repeat donor contributions to a file. The two modules are run as separate processes in parallel so as to emulate a streaming operation where one processes collects the input data from some source in this case an
input file puts it on a queue from which the second process can read and execute the required logic. This architecture can be extended very easily to accommodate taking from various different data sources at the same time.

# Execution flow
Following is the execution flow
1. `run.sh` triggers the `datapipeline.py` file in src folder
2. `datapipeline.py` reads the config file which contains information such as data processing service behaviour ,input file path, output file path, etc.
3. It then instantiates 2 processes, one for reading the input data from the file `fileWorker` and the second one for processing the data `service`.
4. The `service` process read the data from the common queue between the two process and then tests if all the constraints on data quality are satisfied or not. The tests are specified and run in the `integrityChecks.py` file
5. If all the tests are passed then the actual processing for repeat contributors is performed and the output of this process if any is stored to the output file specified.

The config file is used to modify the behavior of the service and file worker processes
Config file Description
{
  "schedule": The number of time empty queue is to be encountered before the service exists. If not present the service runs indefinitely. Optional,
  "serviceSleepTime": number of seconds the service should sleep before checking the queue for data again. Optional,
  "outFileName": output file name. Required,
  "inputFileName": input file name. Required,
  "percentileFile": percentile file name. Required,
  "contributorInfo": file to which to store or read contributor info the service processed before shutting down. This will be useful if the data is coming at long intervals where we have to store and then read previous runs variables as well. Optional,
  "campaignInfo": file to which to store or read campaign info the service processed before shutting down. This will be useful if the data is coming at long intervals where we have to store and then read previous runs variables as well. Optional
}

#Data Constraint Checks
Following are the tests done on various fields
1. Transaction Date: Check to ensure that the date is present valid and that of after 1970. The last is to ensure that we don't have really old dates such as that of 1016 as they are obviously a mistake

2. Zipcode: Check to ensure that zipcode has atleast 5 digits

3. Name: Check to ensure that name is present and not malformed. For malformation I check if all the letters of the name are alphabets and not digits

4. Transaction Amount: Check to ensure that the transaction amount is present and it is valid that it does not contain any alphabets

5. campaignID: Check to ensure that the field is present

6. Count of columns: For each entry there is a check to ensure that we have the required number of columns i.e. 21 
