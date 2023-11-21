## Building a Data Pipeline with Luigi

This project focuses on building a data pipeline that fetches data from the JSONPlaceholder API, cleans and transforms it, and stores the processed data in a database. It's using Luigi as an orchestration framework.

### Getting started

There are just a few steps to be taken to run the project.

#### Check prerequisites

The major prerequisites for your system for running this project are

* Docker engine
* a local folder that you can map to a corresponding folder in a docker container
* an ability to run bash commands (needed to build and run a docker image from a script)

#### Clone this project to your system

The script that one needs to launch at the next steps builds and runs a docker container with the project. This script is written to be run and tested with Bash. So ideally copy your project to a system that supports Bash command shell. 

#### Edit and Run start.sh script

1. At the start.sh script replace <LOCAL_DIR> with a valid path on your local system where you want to allow the Luigi workflow to save all generated results
2. Run the script by:
```& ./start.sh```

### Generated Results

During the project run, each of the Luigi tasks creates some output files in your specified folder.
Each file has a prefix pointing to a LuigiTask which created it.

   0. **data_prep_pipeline.log**: Common log file for the whole Luigi Workflow containing stdout output of the main.py program 

   1. FetchJSONdata() Task creates the following output:
      - **FetchJSONdata_uploadedData.json (luigi.LocalTarget)**: raw JSON received as a result of a GET request to https://jsonplaceholder.typicode.com/posts 

   2. CleanAndTransform() Task creates the following outputs:
      - **CleanAndTransform_readyData.csv**: already pre-processed **FetchJSONdata_uploadedData.json** with data ready to be inserted into a DB as the next step
      - **CleanAndTransform_descTable.csv**: informational table for **CleanAndTransform_readyData.csv** with some basic DQ stats useful for the creation of a table in the DB at the next step
      - **CleanAndTransform_MasterTable.txt (luigi.LocalTarget)**: stores important metadata to assess the results of the current Task at any time. The contents are as follows:
        - *1 row*: absolute path to **CleanAndTransform_readyData.csv**
        - *2 row*: absolute path to **CleanAndTransform_descTable.csv**
        - *3 row*: list of lists with suggested column combinations that can be used as a Primary Key when adding cleaned data to DB at the next step

   3. WriteDataIntoDB() Task creates the following outputs:
      - **WriteDataIntoDB_demodb.db**: SQLite database file containing a table with data from **CleanAndTransform_readyData.csv**
      - **WriteDataIntoDB_dummyOut.txt (luigi.LocalTarget)**: dummy control over the task execution (ideally should be replaced with SQLite table as luigi target)
