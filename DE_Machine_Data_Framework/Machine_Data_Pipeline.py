"""
A framework for the Machine Data Pipelines
Maintains a common strcuture for the pipelines including the 
GCS bucket structure and BQ dataset structure
"""

from google.cloud import storage, bigquery
from pandas import read_csv, to_datetime
from datetime import datetime
from io import BytesIO
from numpy import nan
from DE_Audit_Framework.AuditTable import AuditTable
from DE_Alert_Framework.alert import alert


class Machine_Data_Pipeline:
    bucket = "stl_machine_data"
    archive_bucket = "stl_machine_data_archive"
    project = "stl-staging-data"
    #dataset = "data_engineering"

    def __init__(self, plant, process, machine, dataset=None, table=None):
        self.plant = plant
        self.process = process
        self.machine = machine
        self.dataset = dataset
        self.table = table
        self.start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.errors = {}

    def get_files(self, prefix=None, file_format=None):
        """
        Scans the machine data bucket for the files
        Returns an iterator for blobs
        """
        client = storage.Client()
        print("Scanning list of files...")
        if prefix:
            blob_prefix = prefix
        elif file_format:
            blob_prefix = (
                f"{self.plant}/Staging/{self.process}/{file_format}/{self.machine}/"
            )
        else:
            blob_prefix = f"{self.plant}/Staging/{self.process}/{self.machine}/"
        blobs = client.list_blobs(self.bucket, prefix=blob_prefix, delimiter="/")
        return blobs

    def read_file_as_dataframe(self, blob, encoding=None, index_col=False, skiprows=0):
        """
        Reads a blob as pandas daatframe
        Returns a pandas dataframe
        """
        print("Reading File", blob.name, "...")
        data = blob.download_as_bytes()
        df = read_csv(
            BytesIO(data), encoding=encoding, index_col=index_col, skiprows=skiprows
        )
        return df

    def read_file_as_bytes(self, blob, skiprows=0):
        """
        Reads a blob as byte stream
        Returns a byte stream
        Comes handy if we have to skip some rows in the file
        """
        print("Reading File", blob.name, "...")
        data = blob.download_as_bytes()
        return data

    def add_error(self, file, error):
        """
        Adds error to the dictionary of errors
        """
        self.error[file] = error

    def has_errors(self):
        """
        Returns true if there were errors while processing the files
        """
        return len(self.errors) > 0

    def set_table(self, table):
        """
        Changes the table to be uploaded
        Useful for draw tower pipeline
        """
        self.table = table

    def files_not_found(self):
        """
        If files are not found, send an email and update audit table
        """
        e = f"Files Not Found for machine {self.machine}"
        print(e)
        self.update_audit_table(filename=None, records_per_load=0, job_status="Failed")
        self.send_email(e)

    def move_to_archive(self, blobname):
        """
        Moves the blob to archive bucket
        This is done after successful uploading of file to BQ
        Archive bucket has a 30 days deletion policy
        """
        client = storage.Client()
        source_bucket = client.get_bucket(self.bucket)
        archive_bucket = client.get_bucket(self.archive_bucket)
        print("Moving to Archive Bucket...")

        blobs = source_bucket.list_blobs(prefix=blobname)
        for blob in blobs:
            print(blob.name)
            archive_blob_name = blob.name.replace("Staging/", "")
            blob_copy = source_bucket.copy_blob(blob, archive_bucket, archive_blob_name)
            source_bucket.delete_blob(blob)

    def move_to_error(self, blobname):
        """
        Moves the blob to error directory
        This is done if any error occured while processing the file
        Requires manual intervention to check the error and upload back to BQ
        """
        client = storage.Client()
        source_bucket = client.get_bucket(self.bucket)
        print("Moving to Error Bucket...")
        blobs = source_bucket.list_blobs(prefix=blobname)
        for blob in blobs:
            print(blob.name)
            error_blob_name = blob.name.replace("Staging/", "Error/")
            error_blob = source_bucket.rename_blob(blob, error_blob_name)

    def schema_mapping(df, columns, datatypes):
        n = len(columns)
        df.rename(columns=columns, inplace=True)
        for i in range(n):
            col = columns[i]
            datatype = datatypes[i]
            if datatype == "BOOL":
                df[col] = df[col].astype(bool)
            elif datatype == "INTEGER":
                df[col] = df[col].astype(int)
            elif datatype == "FLOAT":
                df[col] = df[col].astype(float)
            elif datatype == "STRING":
                df[col] = df[col].astype(str)
            elif datatype == "TIMESTAMP":
                if "AM" in str(df[col][0]):
                    df[col] = to_datetime(
                        df[col].str.strip(), format="%d/%m/%Y %I:%M:%S %p"
                    ).dt.strftime("%Y-%m-%d %H:%M:%S")
                    df[col] = to_datetime(df[col])
                if "PM" in str(df[col][0]):
                    df[col] = to_datetime(
                        df[col].str.strip(), format="%d/%m/%Y %I:%M:%S %p"
                    ).dt.strftime("%Y-%m-%d %H:%M:%S")
                    df[col] = to_datetime(df[col])
                else:
                    df[col] = to_datetime(
                        df[col], format="%d/%m/%Y %H:%M:%S"
                    ).dt.strftime("%Y-%m-%d %H:%M:%S")
                    df[col] = to_datetime(df[col])
        return df

    def upload_to_bq(self, df, dataset=None, table=None):
        """
        Gets the table schema from BQ
        Adds extra columns if present in BQ
        Maps the schema of dataframe to that of the table
        Uploads a dataframe to BQ
        """
        # Getting table schema
        client = bigquery.Client()
        job_config = bigquery.LoadJobConfig()

        table_id = f"{self.dataset}.{self.table}"

        table = client.get_table(table_id)
        columns = [c.name for c in table.schema]
        datatypes = [c.field_type for c in table.schema]

        # Adds extra columns if present in BQ
        diff = list(set(columns) - set(df.columns))
        if len(diff) != 0:
            print("Extra Columns :", diff, "\n")
            print("Adding extra columns and setting datatypes..\n")
            for i in diff:
                df[i] = nan

        # Maps the schema of dataframe to that of the table
        df = self.schema_mapping(df, columns, datatypes)

        # Uploading a dataframe to BQ
        print("Uploading to BQ")

        job_config.schema = table.schema
        job_config.write_disposition = "WRITE_APPEND"

        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()

    def update_audit_table(self, filename, records_per_load, job_status):
        """
        Updates the audit table
        """
        job_start_time = self.start_time
        job_end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        config = {
            "source": f"{self.plant}_{self.process}",
            "machine_no": f"{self.machine}",
            "plant": f"{self.plant}",
            "load_stage": "gcs to bq",
            "load_type": "move",
            "table": f"{self.table}",
            "dataset": f"{self.dataset}",
            "maxTquery": "",
            "sourcecount": f"{filename}",
            "bqcount": f"SELECT COUNT(*) FROM {self.project}.{self.dataset}.{self.table}",
            "view_count": f"SELECT COUNT(*) FROM {self.project}.{self.dataset}.{self.table}_V",
            "query": "",
            "schema": ["id", "Email", "Year"],
            "bqConnection": {
                "destinationTable": f"{self.dataset}.{self.table}",
                "view_name": f"{self.table}_V",
                "projectID": f"{self.project}",
                "mode": "append",
            },
            "filename": "lmd_afpo.txt",
        }

        audit = AuditTable(config)
        audit.audit_table(
            filename, records_per_load, job_start_time, job_end_time, job_status
        )

    def send_email(self, exception=None):
        """
        Send an alert email in case any error occured while processing the file
        """
        s = ""
        if self.errors:
            for key, value in self.errors.items():
                s += f"{key}\t: {value}\n"

        if not exception:
            exception = s
        domain = f"{self.plant.upper()}_MACHINE_DATA"
        email_alert = alert()
        email_alert.send_email(
            domain,
            table=None,
            alert_domain=None,
            Date=datetime.strftime(datetime.now(), "%d-%m-%Y"),
            exception=exception,
        )
