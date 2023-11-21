import os
import sys
import itertools
import json
import chompjs
import requests
from requests.adapters import HTTPAdapter, Retry
import luigi
import pandas as pd
import sqlite3
import logging

# logging
logging.basicConfig(filename='data/data_prep_pipeline.log', encoding='utf-8', level=logging.DEBUG)


"""
Helper functions & classes
"""
def _propose_primary_keys(_dataFrame,
                          _minPrimaryKeyLength,  # returns a list of lists with possible column combinations
                          _maxPrimaryKeyLength): # valid as primary keys for a dataframe
    _columnNames = _dataFrame.columns

    _tempPrimaryKeySuggestions = []
    for _columnsCombination in itertools.chain.from_iterable(
        itertools.combinations(_columnNames, r)
        for r in range(max(_minPrimaryKeyLength, 1), min(_maxPrimaryKeyLength, len(_columnNames)))
    ):
        try:
            _dataFrame.set_index(list(_columnsCombination), verify_integrity=True)
        except ValueError: # if impossible to use a column combination as index (=primary key) skip it
            pass
        else:
            _tempPrimaryKeySuggestions.append(list(_columnsCombination))
    
    primaryKeySuggestions = _tempPrimaryKeySuggestions

    return primaryKeySuggestions


def _get_basic_stats(_dataFrame): # creates a small descriptive table with helpful info
                                        # about a dataset (e.g. helps for writing insert, basic data q check)

    _tmpDescTable = _dataFrame.dtypes.to_frame() #get types
    _tmpDescTable = _tmpDescTable.rename(columns={_tmpDescTable.columns[0]: 'columnType'})

    _colDataChar = _dataFrame.select_dtypes(include="object") #get only char columns
    for _col in _colDataChar: # get max lengths of char columns
        _tmpDescTable.at[_col,"CharLength"] = _colDataChar[_col].str.len().max()

    _missingsNum = _dataFrame.isna().sum().to_frame() # get num of missings
    _missingsNum.rename(columns = {_missingsNum.columns[0]:'missingsNum'}, inplace = True)

    _uniqueValsNum = _dataFrame.nunique().to_frame() # get num of unique values
    _uniqueValsNum.rename(columns = {_uniqueValsNum.columns[0]:'uniqueValsNum'}, inplace = True) 

    _tmpDescTable = _tmpDescTable.join(_missingsNum).join(_uniqueValsNum) # unite stats in one desc table

    descTable = _tmpDescTable

    return descTable



class RetryWithLog(Retry): # retries to connect to URL in case of a failure + prints status to stdout
    
  def __init__(self, *args, **kwargs):
    logging.debug("Trying to connect to URL...")
    super().__init__(*args, **kwargs)


"""
Luigi Workflow
"""
class FetchJSONdata(luigi.Task):

    def output(self):
        return luigi.LocalTarget("data/FetchJSONdata_uploadedData.json")
    
    def run(self):
        pathToMain = os.path.dirname(os.path.abspath(__file__))
        
        s = requests.Session()
        retries = RetryWithLog(total=30, backoff_factor=1)
        s.mount('https://', HTTPAdapter(max_retries=retries))

        response = s.get("https://jsonplaceholder.typicode.com/posts")

        with self.output().open("w") as outfile:
            json.dump(response.text, outfile)
        
        pathToOutputData = os.path.join(pathToMain, "data/FetchJSONdata_uploadedData.json")
        sizeInB = os.stat(pathToOutputData).st_size
        logging.debug(f"The upload data has a size of {sizeInB} bytes")


class CleanAndTransform(luigi.Task):
    def requires(self):
        return FetchJSONdata()
    
    def output(self):
        return luigi.LocalTarget("data/CleanAndTransform_MasterTable.txt")
    
    def run(self):
        pathToMain = os.path.dirname(os.path.abspath(__file__))
        pathToOutputData = os.path.join(pathToMain, "data/CleanAndTransform_readyData.csv")
        pathToOutputDescTable = os.path.join(pathToMain, "data/CleanAndTransform_descTable.csv")

        with self.input().open("r") as fromfile:
            # use chompjs to ensure JSON syntax is strictly followed
            rawJSON = chompjs.parse_js_object(json.load(fromfile))

        logging.debug("\n=== Print Data Example ===")
        logging.debug("Analysed JSON contains " + repr(len(rawJSON)) + " elements")
        logging.debug("Element example is:\n" + repr(rawJSON[0]))
        logging.debug("=== Print Data Example ===")

        columnarData = pd.json_normalize(rawJSON)
        columnNames = columnarData.columns

        # improvement required: add a proc to check if in columnarData any values are still represented by nested JSONs 
        # if present - write logic to handle them unil table contains no nested JSONs
        #def _expand_nested(_pdFromJSONfull, _pdFromJSONcharColsOnly):
            #for _col in _pdFromJSONcharColsOnly:
            # ...

        descTable = _get_basic_stats(columnarData)

        logging.debug("\n=== Print statistics ===")
        logging.debug("DataFrame contains" + repr(columnarData.shape[0])+ \
                      " rows and " + repr(columnarData.shape[1]) + " columns")
        logging.debug("Column Description Table:\n" + repr(descTable))

        # get list of possible primary keys for the data
        minPrimaryKeyLength = 1
        maxPrimaryKeyLength = len(columnNames.to_list()) - 1
        possiblePrimaryKeys = _propose_primary_keys(columnarData,
                                                    minPrimaryKeyLength,
                                                    maxPrimaryKeyLength)
        logging.debug("Possible Primary Keys:\n" + repr(possiblePrimaryKeys))
        logging.debug("=== Print statistics ===\n")

        # save generated tables to files
        columnarData.to_csv(pathToOutputData,
                            sep=';', quotechar='"', index = False)
        descTable.to_csv(pathToOutputDescTable,
                         sep=';',quotechar='"',index = True)

        # create one Master table with paths to saved tables
        with self.output().open("w") as outFile: 
            outFile.write(pathToOutputData+"\n"+pathToOutputDescTable+"\n"+repr(possiblePrimaryKeys))
            


class WriteDataIntoDB(luigi.Task): # Task 3
    def requires(self):
        return CleanAndTransform()
    
    def output(self):
        return luigi.LocalTarget("data/WriteDataIntoDB_dummyOut.txt")
    
    def run(self):
        
        with self.input().open("r") as masterTable: # read location of a cleaned data from CleanAndTransform() task
            pathToCleanData = masterTable.readline().strip()

        cleanData = pd.read_csv(pathToCleanData,
                                sep=';', quotechar='"', index_col = False)

        conn = sqlite3.connect("data/WriteDataIntoDB_demodb.db")
        logging.debug ("Connected to DB")

        conn.execute("""CREATE TABLE IF NOT EXISTS CLEANED_DATA
                        (userId INT NOT NULL,
                        id      INT PRIMARY KEY NOT NULL,
                        title   CHAR(79)                ,
                        body    CHAR(225)               
                        );""")

        try:
            # improvement required: better to implement UPSERT logic here for a production pipeline
            cleanData.to_sql(name="CLEANED_DATA", con=conn, if_exists="replace")
        except:
            logging.debug("ERROR: Something went wrong when inserting into the created table:")
        else:
            conn.commit()
            rowsNum = conn.execute("SELECT count(*) from CLEANED_DATA").fetchone()[0]
            logging.debug("After the successful insert, the target table conntains " + repr(rowsNum) + " rows")
        
        conn.close()

        with self.output().open('w') as outFile:
            outFile.write('OK')


if __name__ == "__main__":
    luigi.run(["WriteDataIntoDB", "--local-scheduler"])
