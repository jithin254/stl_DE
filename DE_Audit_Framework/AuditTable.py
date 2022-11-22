
################################   HEADER   ####################################
# Module Name : AuditTable.py
# Date of creation : sep 2022
# Name of creator of module : DE. Shoaib Nadaf
# History of modification : 6 Oct 2022
# Summary of what the module does :
# Audit table lib - extact the lib to use audit feature
# Version 1.2
# More Details : Please refer the README.md
################################################################################


import pandas as pd
from google.cloud import bigquery
from pandas.io import gbq
import json
import argparse
from DE_Audit_Framework.connector import common, gbqCon
import os
import datetime


class AuditTable:
    def __init__(self, config):
        self.config = config
        self.gbqObj = ""

    def extract_info(self):
        self.gbqObj = gbqCon.gbqCon(
            self.config["bqConnection"]["destinationTable"], self.config["bqConnection"]["projectID"], "", "")

        data = {}
        print("Extract information for audit table...")
        data["source"] = str(self.config["source"]).upper()
        data["machine_no"] = str(self.config["machine_no"]).upper()
        data["plant"] = str(self.config["plant"]).upper()
        data["load_stage"] = str(self.config["load_stage"]).upper()
        data["source_table"] = str(self.config["table"]).upper()
        data["projectID"] = str(self.config["bqConnection"]["projectID"]).upper()
        data["dataset"] = str(self.config["dataset"]).upper()
        data["destination_table"] = str(self.config["bqConnection"]["destinationTable"]).upper()
        data["view_name"] = str(self.config["bqConnection"]["view_name"]).upper()
        data["source_count"] = str(self.source_count)
        bq_count = self.gbqObj.countFrombq(self.config["bqcount"])
        bq_count = str(bq_count).replace(' ', '')
        data["destination_count"] = str(bq_count)
        view_count = self.gbqObj.countFrombq(self.config["bqcount"])
        view_count = str(view_count).replace(' ', '')
        data["view_count"] = str(view_count)
        data["records_per_load"] = str(self.records_per_load)
        data["load_type"] = str(self.config["load_type"]).upper()
        data["job_start_time"] = str(self.job_start_time)
        data["job_end_time"] = str(self.job_end_time)
        data["job_status"] = str(self.job_status).upper()

        dt_India = datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)
        Indian_time = dt_India.strftime('%d-%b-%y %H:%M:%S')
        dataframe = pd.DataFrame([data])
        return dataframe

    def audit_table(self, source_count, records_per_load, job_start_time, job_end_time, job_status):
        # read config and extract the details required to load the table
        print("------------------------------------------------------------------\n")
        self.source_count = source_count
        self.records_per_load = records_per_load
        self.job_start_time = job_start_time
        self.job_end_time = job_end_time
        self.job_status = job_status

        audit_dataframe = self.extract_info()
        print("------------------------------------------------------------------\n")
        print("Job Summary :\n", audit_dataframe.head())
        print("------------------------------------------------------------------\n")
        # Audit Table details
        self.gbqObj = gbqCon.gbqCon(
            self.config["bqConnection"]["destinationTable"], self.config["bqConnection"]["projectID"], "", "")
        print("Data loading to Audit Table....")
        print("------------------------------------------------------------------\n")
        audit_table, audit_mode, audit_projectID = common.audit_table()
        self.gbqObj.loadTobq(audit_dataframe, audit_table,
                             audit_projectID, audit_mode)
        print("------------------------------------------------------------------\n")
        print("Auditing Table populated Successfully !!!")

    
    def test_audit_table(self, source_count, records_per_load, job_start_time, job_end_time, job_status):
        # read config and extract the details required to load the table
        print("------------------------------------------------------------------\n")
        self.source_count = source_count
        self.records_per_load = records_per_load
        self.job_start_time = job_start_time
        self.job_end_time = job_end_time
        self.job_status = job_status

        audit_dataframe = self.extract_info()
        print("------------------------------------------------------------------\n")
        print("Job Summary :\n", audit_dataframe.head())
        print("------------------------------------------------------------------\n")
        # Audit Table details
        self.gbqObj = gbqCon.gbqCon(
            self.config["bqConnection"]["destinationTable"], self.config["bqConnection"]["projectID"], "", "")
        print("Data loading to Audit Table....")
        print("------------------------------------------------------------------\n")
        audit_table, audit_mode, audit_projectID = common.test_audit_table()
        self.gbqObj.loadTobq(audit_dataframe, audit_table,
                             audit_projectID, audit_mode)
        print("------------------------------------------------------------------\n")
        print("Auditing Table populated Successfully !!!")
