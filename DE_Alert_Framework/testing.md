
##Scenario 1: 
#mail_alert.send_email("Nagios_Logs")   

---------------------------------------------------------------------------------------
##Scenario 2: 
Yesterday=datetime.strftime(datetime.now() - timedelta(1), '%d%m%Y')
#mail_alert.send_email("Nagios_Logs",alert_domain=None,Date=Yesterday,exception=Exc) 

---------------------------------------------------------------------------------------
##Scenario 3: 
#mail_alert.send_email("Nagios_Logs",Yesterday,"empty_file")   
mail_alert.send_email("Nagios_Logs",alert_domain="empty_file",Date="2022-09-21",exception=None) 

---------------------------------------------------------------------------------------
##Scenario 4: 
#mail_alert.send_email("Nagios_Logs",alert_domain="empty_file",Date=Yesterday,exception="Exception in loading to BQ !!!") 

---------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------


