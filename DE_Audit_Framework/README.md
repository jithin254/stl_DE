
################################   HEADER   ####################################
# Module Name : AuditTable.py 
# Date of creation : sep 2022
# Name of creator of module : DE. Shoaib Nadaf
# History of modification : 6 Oct 2022
# Summary of what the module does : 
# Audit table lib - extact the lib to use audit feature 
################################################################################

-- We have added 3 Different Config File(SFDC,SAP,MACHINE DATA) for your understanding.


1. Developer should create audit table object 
> auditObject = AuditTable(config)
   >> 'config' which developer reading while executing the job

2. Developer should call audit_table() method and pass mandetory parameters.


> auditObject.audit_table(source_count, records_per_load, job_start_time, job_end_time, job_status)

If Developer is not handling job start time ,job end time ,job status (try-except (true/false))
Recommeded to handle these parameters in FW itself.

>> Job start time, end time should be in INDIAN TIME STANDERD 

refer ,

        dt_India = datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)
        Indian_time = dt_India.strftime('%d-%b-%y %H:%M:%S')
    
auditobject.audit_table(source_count, records_per_load, job_start_time,job_end_time,job_status)   


Machine Data : (shendra to gcs, gcs to bq) source_count --> No. of files uploaded/moved/copied count
record_per_load : (shendra to gcs : same as source_count
                  (gcs to bq) : df count expected 


## Note : convert all the paramenters in String before passing to Audit table method

1. Test first in Test Environment
2. Implement in the original Frameworks


Please refer properties/saphana.json

## Modified properties file as per Audit table requirement this [properties] file will be same for all the modules

## SAMPLE CONFIG FILE 
`json`
{
    "source": "SAP_HANA",
    "machine_no": "",
    "plant": "",
    "load_stage": "",
    "load_type": "FULL",
    "table": "AFPO",
    "dataset": "sap_data",
    "maxTquery": "",
    "sourcecount": "SELECT COUNT(*) from SAPPST.AFPO",
    "bqcount": "SELECT COUNT(*) from stl-staging-data.sap_data.afpo",
    "view_count": "SELECT COUNT(*) from stl-staging-data.sap_data.AFPO_V",
    "query": "select * from SAPPST.AFPO",
    "schema": [
        "id",
        "Email",
        "Year"
    ],
    "bqConnection": {
        "destinationTable": "sap_data.afpo",
        "view_name": "AFPO_V",
        "projectID": "stl-staging-data",
        "mode": "replace"
    },
    "filename": "lmd_afpo.txt"
}




### Testing the Audit table 
path = open("properties/config_table.json",)
config = json.load(path)
auditobject = AuditTable(config) 
auditobject.audit_table("10440","1300","2020-09-29 10:45:45","2020-09-29 10:45:50","TRUE")   
