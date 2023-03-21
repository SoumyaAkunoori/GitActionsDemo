# Databricks notebook source
import datetime
import os.path as op
import requests
import json

# COMMAND ----------

class Logger:

    """
    Logger class that helps log data into Log analyitcs workspace

    Ref. :  https://learn.microsoft.com/en-us/azure/azure-monitor/logs/tutorial-logs-ingestion-api
    """

    def __init__(self, log_config):
        """
        Config data are being loaded
        """
        self.appId = log_config["app"]["appId"]
        self.dcrImmutableId = log_config["dcr"]["dcrImmutableId"]
        self.dceEndpoint = log_config["dce"]["dceEndpoint"]
        self.authentication_url = log_config["authentication"]["url"]
        self.authentication_scope = log_config["authentication"]["scope"]
        self.logingestionurl = log_config["logingestionurl"]["url"]
        self.appSecret = "tMP8Q~vwwoTYMdYdSE~PQvIX7zU8LX0lwJotGboN"
        # Sensitive

    def __get_bearer_token(self):
        """
        Gets bearer token
        """

        url = self.authentication_url
        request_body = f"client_id={self.appId}&scope={self.authentication_scope}&client_secret={self.appSecret}&grant_type=client_credentials"
        x = requests.post(url, data=request_body)
        response = json.loads(x.text)
        return response["access_token"]

    def log(self, message, log_level):
        """
        Logs message to Workspace

        Parameters:
        ----------
        message : String
            Message to be logged
        log_level : String
            Severity of the log
            "INFO", "WARN", "ERROR"

        Returns:
        -------
        status code depicting the request result
        204 : Success
        """

        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer " + self.__get_bearer_token(),
        }
        time_now = (
            datetime.datetime.now(datetime.timezone.utc)
            .replace(tzinfo=None)
            .isoformat()
            + "Z"
        )
        data = [{"Time": time_now, "Message": message, "Level": log_level}]

        response = requests.post(self.logingestionurl, headers=headers, json=data)

        return response.status_code
