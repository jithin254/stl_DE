
################################   HEADER   ####################################
# Module Name : main file
# Date of creation : sep 2022
# Name of creator of module : DE.Kalyani , DE. Shoaib Nadaf
# History of modification : 8 Sept 2022
# Summary of what the module does :
# Version 1.3
################################################################################


import smtplib
from datetime import date, timedelta, datetime
from email.message import EmailMessage
import json
import os
import logging
from google.cloud import storage,bigquery


Yesterday = datetime.strftime(datetime.now() - timedelta(1), '%d%m%Y')


class alert:

    def __init__(self):
        global_config = self.read_global_config()
        self.port = global_config["port"]
        self.sender = global_config["sender"]
        self.password = global_config["password"]
        self.mail_config = None
        self.Date = None
        self.exception = None
        self.table = None
        self.absolute_path = os.getcwd()
        self.end_msg_body = "\n\n\n\n-Data Engineering Team\n(dataengg.team@stl.tech)\nThanks" + \
            "\n\n\nDisclaimer : Responses to this email id are not monitored."

    def read_global_config(self):
        ## read the gobal config
        storage_client = storage.Client("stl-staging-data")
        bucket_name = "stl_data_engineering"
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob("_cred_/cred.json")
        cred_string = blob.download_as_string()
        global_config = json.loads(cred_string)
        return global_config

    def read_mail_config(self):
        #storage_client = storage.Client("stl-staging-data")
        #bucket_name = "stl_data_engineering"
        #bucket = storage_client.bucket(bucket_name)
        #blob = bucket.blob("alert_common_config/common_config.json")
        #mail_config_string = blob.download_as_string()
        #mail_config = json.loads(mail_config_string)
        absolute_path = str(os.getcwd())
        config_dir = absolute_path+"/DE_Alert_Framework/properties/"
        mail_config_file = "common_config.json"
        mail_config = json.load(open(config_dir+mail_config_file))
        return mail_config

    def send_alert(self, domain_name, alert_domain):
        mail_config = self.read_mail_config()
        self.From = mail_config[domain_name]["From"]
        self.To = mail_config[domain_name]["To"]
        if alert_domain == None:
            #print("Alert Domain------->" + alert_domain)
            self.Subject = mail_config[domain_name]["Subject_1"].format(
                Yesterday)
            self.message_body = mail_config[domain_name]['message_body_1'].format(
                Yesterday)+"\n"+self.exception

        else:
            #print("Alert Domain------->" + alert_domain)
            mail_config = self.read_mail_config()
            self.From = mail_config[domain_name]["From"]
            self.To = mail_config[domain_name]["To"]
            self.Subject = mail_config[domain_name]["Subject_2"].format(
                Yesterday)
            self.message_body = mail_config[domain_name]['message_body_2'].format(
                Yesterday)

        message = EmailMessage()
        message["Subject"] = self.Subject
        message["From"] = self.From
        message["To"] = self.To
        message.set_content(self.message_body)
        with smtplib.SMTP_SSL("smtp.gmail.com", self.port) as server:
            server.login(self.sender, self.password)
            server.send_message(message)
            server.quit()

    def job_fail(self, domain_name):
        self.Subject = self.mail_config[domain_name]["Subject"].format(
            self.Date)
        self.message_body = self.mail_config[domain_name]['message_body'].format(
            self.Date)+"\nException : \n"+self.exception+self.end_msg_body

    def file_empty(self, domain_name):
        self.Subject = self.mail_config[domain_name]["Subject_2"].format(
            self.Date)
        self.message_body = self.mail_config[domain_name]['message_body_2'].format(
            self.Date)+"\nException : \n"+self.exception+self.end_msg_body

    def email(self):
        message = EmailMessage()
        if(self.table == None):
            message["Subject"] = "DET Alert - "+self.Subject
        else:
            self.Subject = "DET Alert - "+str(self.table)+self.Subject
            message["Subject"] = self.Subject
        message["From"] = self.From
        message["To"] = self.To
        message.set_content(self.message_body)
        with smtplib.SMTP_SSL("smtp.gmail.com", self.port) as server:
            server.login(self.sender, self.password)
            server.send_message(message)
            server.quit()

    def send_email(self, domain_name, table=None, alert_domain=None, Date=datetime.strftime(datetime.now(), '%d-%m-%Y'), exception=None):
        self.mail_config = self.read_mail_config()
        self.From = self.mail_config[domain_name]["From"]
        self.To = self.mail_config[domain_name]["To"]
        self.Date = Date
        self.table = table
        self.exception = str(exception)
  

        if alert_domain == None:
            #print("Alert Domain------->" + alert_domain)
            self.job_fail(domain_name)
            self.email()
            print("mail alert sent !!!!")
        elif alert_domain == "empty_file":
            self.file_empty(domain_name)
            self.email()

        else:
            print("You have NOT entered right Domain !!!!!")
