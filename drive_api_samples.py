import os.path
from typing import List

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload

#TODO: upload folders and files inside them to specific folders in drive




def get_credentials():
  # If modifying these scopes, delete the file token.json.
  SCOPES = ["https://www.googleapis.com/auth/drive"]

  creds = None
  # The file token.json stores the user's access and refresh tokens, and is
  # created automatically when the authorization flow completes for the first
  # time.
  if os.path.exists("credentials/token.json"):
    creds = Credentials.from_authorized_user_file("credentials/token.json", SCOPES)
  # If there are no (valid) credentials available, let the user log in.
  if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
      creds.refresh(Request())
    else:
      flow = InstalledAppFlow.from_client_secrets_file(
          "credentials/AQsensor_GoogleAPI_Credentials.json", SCOPES
      )
      creds = flow.run_local_server(port=0)
    # Save the credentials for the next run
    with open("credentials/token.json", "w") as token:
      token.write(creds.to_json())

  return creds

def get_file_list(creds):
  try:
    service = build("drive", "v3", credentials=creds)

    # Call the Drive v3 API
    results = (
        service.files()
        .list(pageSize=50, fields="nextPageToken, files(id, name)")
        .execute()
    )
    items = results.get("files", [])
    
    if not items:
      print("No files found.")
      return
    print("Files:")
    for item in items:
      print(f"{item['name']} ({item['id']})")
  except HttpError as error:
    # TODO
    print(f"An error occurred: {error}")

def upload_basic(creds):
  """Insert new file.
  Returns : Id's of the file uploaded
  """

  #creds, _ = google.auth.default()
  try:
    # create drive api client
    service = build("drive", "v3", credentials=creds)

    file_metadata = {
      "name": "test.py",
      "parents": ["12YDIO1ya_bIxyFifYfnBq7dU-64WoqjX"]}
    media = MediaFileUpload("test.py", mimetype="text/py")
    # pylint: disable=maybe-no-member
    file = (
        service.files()
        .create(body=file_metadata, media_body=media, fields="id")
        .execute()
    )
    print(f'File ID: {file.get("id")}')

  except HttpError as error:
    print(f"An error occurred: {error}")
    file = None

  return file.get("id")

def create_folder_in_folder(creds, folder_name : str, parent_folder_id : List[str]):
  
  service = build("drive", "v3", credentials=creds)

  file_metadata = {
  'name' : folder_name,
  'parents' : parent_folder_id,
  'mimeType' : 'application/vnd.google-apps.folder'
  }

  file = (service.files().create(body=file_metadata,
                                  fields='id').execute())
  
  print ('Folder ID: %s' % file.get('id'))

def main():
  creds = get_credentials()

  #kz_folder_id = "1bQY2LN9qpb0dRx_BpV6AAKWZUzBoyzb5"
  #create_folder_in_folder(creds, 'Level 0', [kz_folder_id])

  get_file_list(creds)

  #upload_basic(creds)

if __name__ == "__main__":
  main()



#create folders for three countries and get their ids

#upload files to those folders