

def audit_table():
    auditTable =  "ssd_de.AUDIT"
    auditMode = "append"
    auditprojectID = "stl-staging-data"
    return auditTable,auditMode,auditprojectID

def test_audit_table():                                                                                                                                                                                                                                                                                                                                                                                      
    auditTable =  "data_engineering.AUDIT"
    auditMode = "append"
    auditprojectID = "stl-staging-data"
    return auditTable,auditMode,auditprojectID