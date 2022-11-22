
################################   HEADER   ####################################
# Module Name : gbqCon class
# Date of creation : sept 2021
# Name of creator of module : DE. Shoaib Nadaf
# History of modification : 17 Jan 2022
# Summary of what the module does :
# Full load framework : Version 2
################################################################################


import time
from google.cloud import bigquery
from pandas.io import gbq


class gbqCon:
    def __init__(self, destinationTable, projectID, mode, maxtimestampQuery):
        self.destinationTable = destinationTable
        self.projectID = projectID
        self.mode = mode
        self.maxtimestampQuery = maxtimestampQuery
        self.bqcountQuery = None

    def readFrombq(self):
        maxtimestamp = gbq.read_gbq(
            self.maxtimestampQuery, project_id=self.projectID)
        return maxtimestamp

    def countFrombq(self, bqcountQuery):
        self.bqcountQuery = bqcountQuery
        count = gbq.read_gbq(self.bqcountQuery, project_id=self.projectID)
        return count._get_value(0, 'f0_')

    def loadTobq(self, df, destinationTable, projectID, mode):
        print("Data started loading to BQ......\n")
        print("")
        begin = time.time()
        df.to_gbq(destination_table=destinationTable, project_id=projectID,
                  if_exists=mode)  # chunksize = 1000000 -->
        end = time.time()
        print("Time Required to store data : ", end-begin, " sec")
        print("######## Data loaded to BQ successfully !! ########\n")
