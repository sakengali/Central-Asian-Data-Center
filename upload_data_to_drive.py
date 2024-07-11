import os
from typing import List
import pandas as pd
from tqdm import tqdm

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload

"""
Uploads newly downloaded data of KZ, KG and UZ to corresponding folders in Google Drive.
"""

#set the path of the correct folder
cwd = os.getcwd()

def get_credentials():
    """ 
    Process: obtains credentials from the saved token file
    Returns: credentials
    """

    # If modifying these scopes, delete the file token.json.
    SCOPES = ["https://www.googleapis.com/auth/drive"]

    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    token_path = f"{cwd}/credentials/token_drive.json"
    cred_path = f"{cwd}/credentials/AQsensor_Google_Drive_API_Credentials.json"
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


def create_folder_in_folder(creds, folder_name : str, parent_folder_ids : List[str]):
    """
    Creates a folder inside a parent's folder
    Returns: folder_id
    """
    
    service = build("drive", "v3", credentials=creds)

    file_metadata = {
        'name' : folder_name,
        'parents' : parent_folder_ids,
        'mimeType' : 'application/vnd.google-apps.folder'
    }

    file = (service.files().create(body=file_metadata,
                                    fields='id').execute())
    
    #print ('Folder ID: %s' % file.get('id'))

    return file.get('id')



def upload_file_to_folder(creds, file_name : str, parent_folder_ids : List[str]):
    """
    Creates a folder inside a parent's folder
    Returns: folder_id
    """
    
    service = build("drive", "v3", credentials=creds)

    file_metadata = {
        'name' : file_name,
        'parents' : parent_folder_ids,
        'mimeType' : 'text/csv',
    }

    file = (service.files().create(body=file_metadata,
                                   media_body=file_name,
                                    fields='id').execute())

    return file.get('id')




def get_date_folder_name() -> str:
    """ returns date folder name"""

    this_month = pd.Timestamp.today().strftime("%b-%Y")
    month_part = '1' if pd.Timestamp.today().day <= 16 else '2'
    return f"{this_month}-{month_part}"




def create_folders(creds):
    """
    create folders in the following structure:

    country folder (KZ, KG or UZ)
        - level 0
            - date folder
                - Indoor Sensors
                - Outdoor Sensors

    returns: a dict containg tuple of folder ids for each country
    """
    print("creating folders ... ")

    kz_level0_folder_id = "1tK95cgkDNwCKVN_6XVlypwI0J61Oxogh"
    kg_level0_folder_id = "1rH60ZFRlwkC_1gR1a6iTg43fZzWBylC_"
    uz_level0_folder_id = "1emKDUabDiUCEoqTRG0J8yrUnSsA3UlzB"

    # create date folder
    date_folder_name = get_date_folder_name()

    kz_date_folder_id = create_folder_in_folder(creds, date_folder_name, [kz_level0_folder_id])
    kg_date_folder_id = create_folder_in_folder(creds, date_folder_name, [kg_level0_folder_id])
    uz_date_folder_id = create_folder_in_folder(creds, date_folder_name, [uz_level0_folder_id])
    
    print(f"created date folder for KZ, id: {kz_date_folder_id}")
    print(f"created date folder for KG, id: {kg_date_folder_id}")
    print(f"created date folder for UZ, id: {uz_date_folder_id}")


    # create Indoor and Outdoor sensors folders
    indoor_folder_name = "Indoor Sensors"
    outdoor_folder_name = "Outdoor Sensors"

    kz_indoor_folder_id = create_folder_in_folder(creds, indoor_folder_name, [kz_date_folder_id])
    kz_outdoor_folder_id = create_folder_in_folder(creds, outdoor_folder_name, [kz_date_folder_id])
    kg_indoor_folder_id = create_folder_in_folder(creds, indoor_folder_name, [kg_date_folder_id])
    kg_outdoor_folder_id = create_folder_in_folder(creds, outdoor_folder_name, [kg_date_folder_id])
    uz_indoor_folder_id = create_folder_in_folder(creds, indoor_folder_name, [uz_date_folder_id])
    uz_outdoor_folder_id = create_folder_in_folder(creds, outdoor_folder_name, [uz_date_folder_id])

    print()
    print(f"created Indoor Sensor folder for KZ, id: {kz_indoor_folder_id}")
    print(f"created Outdoor Sensor folder for KZ, id: {kz_outdoor_folder_id}")
    print(f"created Indoor Sensor folder for KG, id: {kg_indoor_folder_id}")
    print(f"created Outdoor Sensor folder for KG, id: {kg_outdoor_folder_id}")
    print(f"created Indoor Sensor folder for UZ, id: {uz_indoor_folder_id}")
    print(f"created Outdoor Sensor folder for UZ, id: {uz_outdoor_folder_id}")

    print("created all folders.")
    
    folder_ids = {
        'KZ' : (kz_indoor_folder_id, kz_outdoor_folder_id),
        'KG' : (kg_indoor_folder_id, kg_outdoor_folder_id),
        'UZ' : (uz_indoor_folder_id, uz_outdoor_folder_id),    
    }

    return folder_ids

def upload_data_for(creds, country : str, cwd : str, indoor_folder_id : str, outdoor_folder_id : str):
    """
    
    """

    level_folder = "Level 0"
    date_folder_name = get_date_folder_name()
    indoor_folder_name = 'Indoor Sensors'
    outdoor_folder_name = 'Outdoor Sensors'

    # uploading data for indoor sensors
    os.chdir(f"{cwd}/Central Asian Data/{country.upper()}/{level_folder}/{date_folder_name}/{indoor_folder_name}")
    print('Uploading indoor data')
    with tqdm(os.listdir()) as t:
        for file_name in t:
            t.set_description(f"Uploading {file_name}")
            upload_file_to_folder(creds, file_name, parent_folder_ids=[indoor_folder_id])

    # uplodaing data for outdoor sensors
    os.chdir(f"{cwd}/Central Asian Data/{country.upper()}/{level_folder}/{date_folder_name}/{outdoor_folder_name}")
    print('Uploading outdoor data')
    with tqdm(os.listdir()) as t:
        for file_name in t:
            t.set_description(f"Uploading {file_name}")
            upload_file_to_folder(creds, file_name, parent_folder_ids=[indoor_folder_id])

    return None


def main_upload():
    creds = get_credentials()

    # create date and indoor/outdoor folders
    folder_ids = create_folders(creds)
    
    # upload data
    for country in ['KZ', 'KG', 'UZ']:
        print(f'\nUploading data for {country}')
        upload_data_for(creds, country, cwd, *folder_ids[country])


if __name__ == "__main__":
    main_upload()