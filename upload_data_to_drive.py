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

from helpers import cwd, date_folder_name, get_level_0_folder, get_level_1_folder

"""
Uploads newly downloaded data of KZ, KG and UZ to corresponding folders in Google Drive.
"""

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
    Upload a file to the parent folderS
    Returns: file id
    """
    
    service = build("drive", "v3", credentials=creds)

    if file_name[-3:] == 'csv':
        mime_type = 'text/csv'
    elif file_name[-3:] == 'txt':
        mime_type = 'text/plain'
    elif file_name[-3:] == 'pdf':
        mime_type = 'application/pdf'

    file_metadata = {
        'name' : file_name,
        'parents' : parent_folder_ids,
        'mimeType' : mime_type,
    }

    file = (service.files().create(body=file_metadata,
                                   media_body=file_name,
                                    fields='id').execute())

    return file.get('id')

def create_folders(creds, level: str = '0'):
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

    if level == "0":
        kz_level_folder_id = "1tK95cgkDNwCKVN_6XVlypwI0J61Oxogh"
        kg_level_folder_id = "1rH60ZFRlwkC_1gR1a6iTg43fZzWBylC_"
        uz_level_folder_id = "1emKDUabDiUCEoqTRG0J8yrUnSsA3UlzB"
    if level == "1":
        kz_level_folder_id = "1x-tbN3OhQmNN_34XVmoYMFZFDnuX1poR"
        kg_level_folder_id = "1AhAv3lkCT1HKwVVYlWCW31RrBLzGL4zq"
        uz_level_folder_id = "1P_M0ZYAnZjbq9ycYW4TGJw7sy45HQnzR"

    kz_date_folder_id = create_folder_in_folder(creds, date_folder_name, [kz_level_folder_id])
    kg_date_folder_id = create_folder_in_folder(creds, date_folder_name, [kg_level_folder_id])
    uz_date_folder_id = create_folder_in_folder(creds, date_folder_name, [uz_level_folder_id])

    data_folder_ids = {
        'KZ' : kz_date_folder_id,
        'KG' : kg_date_folder_id,
        'UZ' : uz_date_folder_id,
    }
    
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



    return data_folder_ids, folder_ids

def upload_data_for(creds, country : str, indoor_folder_id : str, outdoor_folder_id : str, level: str):
    """
    Upload data of sensors in .csv files
    """

    level_folder = get_level_0_folder(country) if level == '0' else get_level_1_folder(country)
    indoor_folder_name = 'Indoor Sensors'
    outdoor_folder_name = 'Outdoor Sensors'

    # uploading data for indoor sensors
    os.chdir(f"{cwd}/Central Asian Data/{country.upper()}/{level_folder}/{date_folder_name}/{indoor_folder_name}")
    print(f'Uploading indoor data (level {level})')
    with tqdm(os.listdir()) as t:
        for file_name in t:
            t.set_description(f"Uploading {file_name}")
            upload_file_to_folder(creds, file_name, parent_folder_ids=[indoor_folder_id])

    # uplodaing data for outdoor sensors
    os.chdir(f"{cwd}/Central Asian Data/{country.upper()}/{level_folder}/{date_folder_name}/{outdoor_folder_name}")
    print(f'Uploading outdoor data (level {level})')
    with tqdm(os.listdir()) as t:
        for file_name in t:
            t.set_description(f"Uploading {file_name}")
            upload_file_to_folder(creds, file_name, parent_folder_ids=[outdoor_folder_id])

    return None

def upload_info_file(creds, country : str, date_folder_id : str):
    """
    uploads info.txt file to the date folder
    """

    level_folder = get_level_0_folder(country)

    os.chdir(f"{cwd}/Central Asian Data/{country}/{level_folder}/{date_folder_name}")
    upload_file_to_folder(creds, f"{country.lower()}_info.txt", parent_folder_ids=[date_folder_id])

    return None

def upload_summary_file(creds, country : str, date_folder_id : str):
    """
    uploads summary_{country}.pdf file to the date folder
    """

    level_folder = get_level_1_folder(country)

    os.chdir(f"{cwd}/Central Asian Data/{country}/{level_folder}/{date_folder_name}")
    upload_file_to_folder(creds, f"{country.lower()}_summary.pdf", parent_folder_ids=[date_folder_id])

    return None

def upload_uptime_file(creds, country : str, date_folder_id : str):
    """
    uploads {country}_uptime.pdf file to the date folder
    """

    level_folder = get_level_0_folder(country)

    os.chdir(f"{cwd}/Central Asian Data/{country}/{level_folder}/{date_folder_name}")
    upload_file_to_folder(creds, f"{country.lower()}_uptime.pdf", parent_folder_ids=[date_folder_id])

    return None

def upload_status_db_file(creds, base_folder_id : str):
    """
    uploads sensor_status_db.csv file to the base folder
    """

    os.chdir(f"{cwd}/Central Asian Data/")
    upload_file_to_folder(creds, "sensor_status_db.csv", parent_folder_ids=[base_folder_id])

    return None

def main_upload():
    creds = get_credentials()

    # create date and indoor/outdoor folders
    level_0_date_folder_ids, level_0_folder_ids = create_folders(creds, level='0')
    level_1_date_folder_ids, level_1_folder_ids = create_folders(creds, level='1')

    base_folder_id = "12YDIO1ya_bIxyFifYfnBq7dU-64WoqjX"

    # upload
    for country in ['KZ', 'KG', 'UZ']:

        try:
            # upload data
            print(f'\nUploading data for {country}')
            upload_data_for(creds, country, *level_0_folder_ids[country], level='0')
            upload_data_for(creds, country, *level_1_folder_ids[country], level='1')

            # upload info.txt file
            print(f'Uploading {country.lower()}_info.txt file')
            upload_info_file(creds, country, level_1_date_folder_ids[country])

            # upload uptime.pdf
            print(f'Uploading {country.lower()}_uptime.pdf file')
            upload_uptime_file(creds, country, level_1_date_folder_ids[country])

            # upload summary.pdf
            print(f'Uploading {country.lower()}_summary.pdf file')
            upload_summary_file(creds, country, level_1_date_folder_ids[country])

            print(f'Finished uploading data for {country}')
            
        except FileNotFoundError as e:
            print(f"Couldn't upload data for {country}. Error: {e}")

if __name__ == "__main__":
    main_upload()