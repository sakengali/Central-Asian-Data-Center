import os.path
import base64
from email.message import EmailMessage
import pandas as pd
from typing import List
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders

from helpers import get_sensors_info, date_folder_name, cwd

import socket
timeout_in_sec = 60*5 # 3 minutes timeout limit
socket.setdefaulttimeout(timeout_in_sec)

message_recepients = "dhawal.shah@nu.edu.kz, michael.bergin@duke.edu, trb50@duke.edu, kuanysh.moldabekov@nu.edu.kz, sakengali.kazhiyev@nu.edu.kz, baglan.zhubatkanov@nu.edu.kz, rauan.arstangaliyev@nu.edu.kz"

def get_credentials():
    """Shows basic usage of the Gmail API.
    Lists the user's Gmail labels.
    """
    # If modifying these SCOPES, delete the file token.pickle.
    SCOPES = ['https://www.googleapis.com/auth/gmail.send']

    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.

    token_path = f"{cwd}/credentials/token_gmail.json"
    cred_path = f"{cwd}/credentials/AQsensor_Google_Gmail_API_Credentials.json"

    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                cred_path, SCOPES
            )
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(token_path, "w") as token:
            token.write(creds.to_json())

    return creds

def send_email(message_text : str):
    """Create and send an email message
    Print the returned  message id
    Returns: Message object, including message id

    Load pre-authorized user credentials from the environment.
    TODO(developer) - See https://developers.google.com/identity
    for guides on implementing OAuth2 for the application.
    """

    try:
        creds = get_credentials()
        service = build("gmail", "v1", credentials=creds)
        message = EmailMessage()

        message.add_header('Content-Type','text/html')
        message.set_payload(message_text)

        message["To"] = message_recepients
        message["From"] = "aqsensor@nu.edu.kz"
        message["Subject"] = "Data Upload Message"
        

        # encoded message
        encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()

        create_message = {"raw": encoded_message}
        # pylint: disable=E1101
        send_message = (
            service.users()
            .messages()
            .send(userId="me", body=create_message)
            .execute()
        )
        print(f'Confirmation message sent (Id: {send_message["id"]})')
    except HttpError as error:
        print(f"An error occurred: {error}")
        send_message = None
    return send_message

def send_email_with_attachment(message_text : str, file_path_list : str):
    """Create and send an email message
    Print the returned  message id
    Returns: Message object, including message id
    """

    try:
        creds = get_credentials()
        service = build("gmail", "v1", credentials=creds)
        
        # Create the email
        message = MIMEMultipart()
        message["To"] = message_recepients
        message["From"] = "aqsensor@nu.edu.kz"
        message["Subject"] = "Data Upload Confirmation"
        
        # Add the email body
        message.attach(MIMEText(message_text, "html"))
        
        # Attach the files
        for file_path in file_path_list:
            with open(file_path, "rb") as f:
                mime_base = MIMEBase("application", "octet-stream")
                mime_base.set_payload(f.read())
                encoders.encode_base64(mime_base)
                mime_base.add_header("Content-Disposition", f"attachment; filename={file_path.split('/')[-1]}")
                message.attach(mime_base)
        
        # Encode the message
        encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()

        create_message = {"raw": encoded_message}
        
        # Send the message
        send_message = (
            service.users()
            .messages()
            .send(userId="me", body=create_message)
            .execute()
        )
        
        print(f'Confirmation message sent (Id: {send_message["id"]})')
    except HttpError as error:
        print(f"An error occurred: {error}")
        send_message = None

    return send_message

def make_readable_list(str_list : List[str]) -> str:

    if len(str_list) == 2:
        return str_list[0] + " and " + str_list[1]
    else:
        str_list[1] = str_list[0] + ", " + str_list[1]
        str_list = str_list[1:] 
        return make_readable_list(str_list)

def get_list_off_sensors(country):
    sensors = get_sensors_info(country)

    off_sensors = [sensor.name for sensor in sensors.values() if sensor.is_turned_off()]

    return make_readable_list(off_sensors)



def send_email_main(is_successful : bool = True, error : str = ''):
    """Send the email to the recipients"""

    print("Sending confirmation email...")

    day1 = pd.Timestamp.today() - pd.Timedelta(days=16); day1 = day1.strftime("%d-%b-%Y")
    day2 = pd.Timestamp.today().strftime("%d-%b-%Y")

    if is_successful:
        
        level_folder = "Level 0"
        file_path_list = []

        for cnt in ['KZ', 'KG', 'UZ']:
            file_path_list.append(f"{cwd}/Central Asian Data/{cnt}/{level_folder}/{date_folder_name}/{cnt.lower()}_info.txt")
            #file_path_list.append(f"{cwd}/Central Asian Data/{cnt}/{level_folder}/{date_folder_name}/{cnt.lower()}_summary.pdf")

        message_text = f"""
            <p>Dear Members of the NU Air Quality Project</p>

            <p>Please be informed that the data for Air Quality Sensors in Kazakhstan, Kyrgyzstan and Uzbekistan were downloaded for the period of {day1} and {day2}. All the data are available at the <a href="https://drive.google.com/drive/folders/12YDIO1ya_bIxyFifYfnBq7dU-64WoqjX?usp=sharing"> NU Data Center</a>.</p>
            
            <p></p>

            <p>WARNING: There are some sensors that are deployed but not sending any data. <br>For Kazakhstan: {get_list_off_sensors('kz')} </p>
            
            <p>Best,</p>
        """    
        send_email_with_attachment(message_text=message_text, file_path_list=file_path_list)
    else:
        message_text = f"""
            <p>Dear Members of the NU Air Quality Project</p>

            <p>Please be informed that the data for Air Quality Sensors in Kazakhstan, Kyrgyzstan and Uzbekistan <b> were failed to download </b> for the period of {day1} and {day2}.<br> The error is <i>{error}</i>.</p>

            <p>Best,</p>
        """

        send_email(message_text=message_text)

    socket.setdefaulttimeout(None)





""" if __name__ == "__main__":
    send_confirmation_email() """