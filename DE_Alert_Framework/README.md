
## Requirement 

check requirement.txt file and install package accordingly

## How to import [package]

from alert import alert 
Create single Object 
call send_email() method

While calling send_email() new parameteres need to be passed


## Below are few examples you can refer,

mail_alert = alert()  --> Object creation  

##SAP HANA
mail_alert.send_email("SAP_HANA",table="SAP.BSEG",exception=Exception) 

##nagios
mail_alert.send_email("Nagios_Logs",table=None,alert_domain=None,Date=Yesterday,exception=None) 
 
##SFDC
mail_alert.send_email("SFDC",table="SFDC.Account",exception=None) 

##HR Data
mail_alert.send_email("HR",table="HR.Personal",exception=None) 

##Signavio
mail_alert.send_email("SIGNAVIO",table="SIGNAVIO.TASKS",exception=None) 


'''
mail_alert.send_email(
    domain_name="Nagios_Logs", ## mandetory field 
    alert_domain="empty_file", ## If not selected empty_file dont pass anything
    Date=None, ## Current day execution/failure --> dont pass date , If want to pass yesterday or selective date use this
    exception=None ## No exception --> dont pass anything , If execption catched --> pass in the form of string 
    )
''' 



