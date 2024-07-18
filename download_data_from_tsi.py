import requests
import pandas as pd
import numpy as np
import os
import sys
from typing import Dict
import json
from tqdm import tqdm

# TODO: 
# set age to 16
# set client_id and client_secret for kg and uz; preferebly using environmmental variables

"""
    Script to download the data of Air Quality sensors for the last 16 days, using the TSI API.

    Usage : python3 download_data_from_tsi.py {country_code}
    country_code can be only 'kz', 'kg', and 'uz'       

"""

#set the path of the correct folder
cwd = "/home/dhawal/Air Quality Analysis Central Asia/Central-Asian-Data-Center"

def get_access_token(country : str, config : Dict[str, str]) -> str:

    """ Input: country code, config dictionary with client ids and secrets of countries\
        Returns : access token for the country's TSI account """

    client_id = config[f'{country.lower()}_client_id']
    client_secret = config[f'{country.lower()}_client_secret']

    #getting credentials (access token)
    url='https://api-prd.tsilink.com/api/v3/external/oauth/client_credential/accesstoken?grant_type=client_credentials'
    params = {'grant_type':'client_credentials'}
    headers = {'Content-Type':'application/x-www-form-urlencoded'}
    response = requests.post(url, params=params, headers=headers, auth=(client_id, client_secret))

    access_token = response.json()['access_token']

    return access_token


def get_date() -> tuple:
    this_month = pd.Timestamp.today().strftime("%b-%Y")
    month_part = '1' if pd.Timestamp.today().day <= 16 else '2'

    return this_month, month_part 


def get_device_list(is_indoor: bool = True, headers : Dict[str, str] = {}):
    """ return the list of devices """

    url='https://api-prd.tsilink.com/api/v3/external/devices'

    params = {
        'is_indoor' : is_indoor
    }

    response = requests.get(url, headers=headers, params=params)

    devices = response.json() #list of dict-s, one dict of info for each sensor
    device_list = {device['metadata']['friendlyName'] : device['device_id'] for device in devices}

    return device_list




def get_sensor_data(device_id : str, device_name : str, is_indoor=True, headers : Dict[str, str] = {}):
    """ 
    fetch sensor data and copy it to a csv file 
    returns: None
    """

    assert device_id and device_name, 'device_id and device_name are required'

    #getting the date information
    this_month, month_part = get_date()

    url =  'https://api-prd.tsilink.com/api/v3/external/telemetry/flat-format'

    telemetry = ['serial', 'location', 'mcpm2x5', 'mcpm2x5_aqi', 'co2_ppm', 'voc_mgm3', 'temperature', 'rh']

    params = {
            'device_id' : device_id,
            'age' : 16,
            'telem[]' : telemetry,
        }
    
    response = requests.get(url, headers=headers, params=params)
    data_json = response.json() 

    if is_indoor:
        columns = ['Timestamp', 'Serial', 'Latitude', 'Longitude', 'PM 2.5', 'PM 2.5 AQI', 'CO2', 'VOC', 'Temperature', 'Relative Humidity']

        sensor_dataframe = pd.DataFrame(columns = columns)

        for data_point in data_json:
            sensor_dataframe.loc[len(sensor_dataframe)] = {
                'Timestamp' : data_point['timestamp'],
                'Serial' : data_point['serial'],
                'Latitude' : data_point['latitude'],
                'Longitude' : data_point['longitude'],
                'PM 2.5' : data_point['mcpm2x5'],
                'PM 2.5 AQI' : data_point['mcpm2x5_aqi'],
                'CO2' : data_point['co2_ppm'],
                'VOC' : data_point['voc_mgm3'],
                'Temperature' : data_point['temperature'],
                'Relative Humidity' : data_point['rh']
            }

        sensor_dataframe = sensor_dataframe.set_index('Timestamp')
        sensor_dataframe.to_csv(f'{this_month}-{month_part}/Indoor Sensors/{device_name}-{this_month}.csv')
        #print(f"Downloaded data for {device_name}")
    else:
        columns = ['Timestamp', 'Serial', 'Latitude', 'Longitude', 'PM 2.5', 'PM 2.5 AQI', 'Temperature', 'Relative Humidity']

        sensor_dataframe = pd.DataFrame(columns = columns)

        for data_point in data_json:
            sensor_dataframe.loc[len(sensor_dataframe)] = {
                'Timestamp' : data_point['timestamp'],
                'Serial' : data_point['serial'],
                'Latitude' : data_point['latitude'],
                'Longitude' : data_point['longitude'],
                'PM 2.5' : data_point['mcpm2x5'],
                'PM 2.5 AQI' : data_point['mcpm2x5_aqi'],
                'Temperature' : data_point['temperature'],
                'Relative Humidity' : data_point['rh']
            }

        sensor_dataframe = sensor_dataframe.set_index('Timestamp')
        sensor_dataframe.to_csv(f'{this_month}-{month_part}/Outdoor Sensors/{device_name}-{this_month}.csv')
        #print(f"Downloaded data for {device_name}")




def save_sensors_data(device_list : dict, is_indoor : bool = True, headers : Dict[str, str] = {}) -> None:
    """ save data of the sensors to csv in a folder """

    if is_indoor:
        print("Downloading data for Indoor sensors")
        with tqdm(device_list['Indoor'].items()) as t:
            for device_name, device_id in t:
                t.set_description(device_name)
                get_sensor_data(device_name=device_name, device_id=device_id, is_indoor=True, headers=headers)
    else:
        print("Downloading data for Outdoor sensors")
        with tqdm(device_list['Outdoor'].items()) as t:
            for device_name, device_id in t:
                t.set_description(device_name)
                get_sensor_data(device_name=device_name, device_id=device_id, is_indoor=False, headers=headers)



def create_folders(cwd, country) -> None:
    """ create necessary folders to store the data of this month """

    # getting the date information
    this_month, month_part = get_date()

    #create the main folder for data storage
    if not os.path.isdir(f'{cwd}/Central Asian Data/{country}/Level 0/{this_month}-{month_part}'):
        print(f"Creating the necessary folders for data storage")

        os.makedirs(f'{cwd}/Central Asian Data/{country}/Level 0/{this_month}-{month_part}/Indoor Sensors')
        os.makedirs(f'{cwd}/Central Asian Data/{country}/Level 0/{this_month}-{month_part}/Outdoor Sensors')

    else:
        print("Folders for data storage were found.")





def main_download(country : str = None):

    #checks
    assert country, "country code must be provided"
    assert country.lower() in ['kz', 'kg', 'uz'], "country code should be either 'kz', 'kg', or 'uz' "

    # getting the date information
    this_month, month_part = get_date()
    print()
    print(f'Downloading {country.upper()} data for {this_month}-{month_part}')

    #getting the client id and secret
    with open(f'{cwd}/config.json') as config_file:
        config = json.load(config_file)

    access_token = get_access_token(country, config)

    headers = {
            "Accept": 'application/json',
            "Authorization": 'Bearer ' + access_token,
        }

    #level information
    level_folder = "Level 0"

    create_folders(cwd, country)

    #change directory to the country's
    os.chdir(f"{cwd}/Central Asian Data/{country.upper()}/{level_folder}")

    # save data for indoor sensors 
    save_sensors_data(
        device_list={
            'Indoor' : dict(sorted(get_device_list(is_indoor=True, headers=headers).items())),
            'Outdoor' : dict(sorted(get_device_list(is_indoor=False, headers=headers).items())),
        },
        is_indoor=True,
        headers=headers)
    
    # save data for outdoor sensors
    save_sensors_data(
        device_list={
            'Indoor' : dict(sorted(get_device_list(is_indoor=True, headers=headers).items())),
            'Outdoor' : dict(sorted(get_device_list(is_indoor=False, headers=headers).items())),
        },
        is_indoor=False,
        headers=headers)
    
#if __name__ == "__main__":
    #main_download(country = 'KZ')