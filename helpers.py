import pandas as pd
import os
from datetime import datetime
from typing import NamedTuple, List, Dict
import csv
import gspread
import json

cwd : str = "/home/dhawal/Air Quality Analysis Central Asia/Central-Asian-Data-Center" if "dhawal" in os.getcwd() else os.getcwd()

#service account to obtain information from google spreadsheets
gc = gspread.service_account(filename=f'{cwd}/cosmic-talent-416001-3c711f8ccf2e.json')

country_names = {
    'KZ': 'Kazakhstan',
    'KG': 'Kyrgyzstan',
    'UZ': 'Uzbekistan'
}

get_level_0_folder = lambda country : "Level 0h" if country in ["KZ", "KG"] else "Level 0"

get_level_1_folder = lambda country : "Level 2" if country in ["KZ", "KG"] else "Level 1"

def get_date_folder_name() -> str:
    """ returns date folder name """

    this_month = pd.Timestamp.today().strftime("%b-%Y")
    month_part = '1' if pd.Timestamp.today().day <= 15 else '2'
    return f"{this_month}-{month_part}" if "dhawal" in os.getcwd() else "Nov-2024-2"

date_folder_name : str = get_date_folder_name()

class Sensor(NamedTuple):
    name : str
    sensor_type : str
    country : str
    is_deployed : bool
    location : str
    city : str
    did_change_location : str
    calibration_factor : float
    updates : List

    def get_level_0_folder(self):
        return "Level 0h" if self.country in ["KZ", "KG"] else "Level 0"

    def is_responding(self):
        sensor_file_name = f"{self.name}-{date_folder_name[:8]}.csv"
        df = pd.read_csv(f"{cwd}/Central Asian Data/{self.country}/{self.get_level_0_folder()}/{date_folder_name}/{self.sensor_type}/{sensor_file_name}")

        return not df.empty

    def is_turned_off(self):
        return self.is_deployed and not self.is_responding()


def get_sensors_info(country : str) -> List[Sensor]:

    """ 
        inputs: country 
        returns a Dict[sensor_name : sensor_info] for country's sensors
    """

    sensors : Dict[str, Sensor] = {}
    
    with open(f"{cwd}/config.json", "r") as f:
        config = json.load(f)
        sheet_key = config[f"{str.lower(country)}_client_spreadsheet"]
    print(f"Retrieving data for {country}")
    sh = gc.open_by_key(sheet_key)
    
    for w in sh.worksheets()[:2]:
        data = w.get_all_records()
        #print(data)
        sensors.update(
            {row['Sensor Name'] : Sensor(
                name = row['Sensor Name'],
                sensor_type = row['Sensor Type'],
                country = country,
                is_deployed = row['Is Deployed'],
                location = row['Location'],
                city = row['City'],
                did_change_location = row['Did Change Location'],
                calibration_factor = row['Calibration Factor'],
                updates = [upd for upd in row['Updates'].split(';')]
            ) for row in data}
        )
    
    return sensors


def sensor_line_v1(sensor : Sensor) -> str:
    sensor_name = sensor.name
    status = "Deployed    " if sensor.is_deployed else "Not Deployed"
    response = "Responding" if sensor.is_responding() else "Not Responding"
    location = sensor.location if sensor.is_deployed else "None"
    location_length = len(location)
    
    if location_length < 8:
        location_str = f"{location}"
    elif location_length < 16:
        location_str = f"{location}"
    else:
        location_str = f"{location}"
    
    # Format the output with fixed width for each column
    return f"{sensor_name:<8}{location_str:<24}{status:<16}{response}"

def sensor_line_v0(sensor_name, status) -> str:
    if len(sensor_name) < 8:
        return f"{sensor_name}\t" + "\t"*(2) + f"{status}"
    elif len(sensor_name) < 16:
        return f"{sensor_name}\t" + "\t"*(1) + f"{status}"
    else:
        return f"{sensor_name}\t" + f"{status}"


def create_info_file():

    """ 
        creates the {country}_info.txt file 
        level 0 info is used here, because info file is needed only to obtain data about sensor status, not the data itself
    """

    for country in ['KZ', 'KG', 'UZ']:

        level_folder = get_level_0_folder(country)

        try:
            with open(f"{cwd}/Central Asian Data/{country}/{level_folder}/{date_folder_name}/{country.lower()}_info.txt", 'w') as f:
                f.write(f"The data of {country} sensors was downloaded on {datetime.now()}\n\n")
                f.write(f"Date Folder Name: {date_folder_name}\n\n")
                f.write(f"Level Folder: {level_folder}\n\n")
                f.write("Sensor\t\tLocation\t\t\tStatus\t\t\tResponse\n")
            
            #information for updated info file version is available only for KZ
            if country == 'KZ': 
                sensors = get_sensors_info(country)
                for sensor_type in ['Indoor Sensors', 'Outdoor Sensors']:
                    with open(f"{cwd}/Central Asian Data/{country}/{level_folder}/{date_folder_name}/{country.lower()}_info.txt", 'a') as f:
                        f.write(f"\n({sensor_type}):\n")
                    for sensor in sorted(os.listdir(f"{cwd}/Central Asian Data/{country}/{level_folder}/{date_folder_name}/{sensor_type}")):
                        sensor_name : str = sensor.split('-')[0]
                        sensor : Sensor = sensors[sensor_name]

                        with open(f"{cwd}/Central Asian Data/{country}/{level_folder}/{date_folder_name}/{country.lower()}_info.txt", 'a') as f:
                            f.write(sensor_line_v1(sensor) + "\n")

            #KG and UZ will get the old verison of the info files
            else:
                for sensor_type in ['Indoor Sensors', 'Outdoor Sensors']:
                    with open(f"{cwd}/Central Asian Data/{country}/{level_folder}/{date_folder_name}/{country.lower()}_info.txt", 'a') as f:
                        f.write(f"\n({sensor_type}):\n")
                    for sensor in sorted(os.listdir(f"{cwd}/Central Asian Data/{country}/{level_folder}/{date_folder_name}/{sensor_type}")):
                        if country == 'UZ':
                            if sensor_type == 'Indoor Sensors':
                                sensor_name = sensor.split('-')[:4]
                                sensor_name = sensor_name[0] + '-' + sensor_name[1] + '-' + sensor_name[2] + '-' + sensor_name[3]
                            else:
                                sensor_name = sensor.split('-')[:3]
                                sensor_name = sensor_name[0] + '-' + sensor_name[1] + '-' + sensor_name[2]
                        else:
                            sensor_name = sensor.split('-')[0]

                        df = pd.read_csv(f"{cwd}/Central Asian Data/{country}/{level_folder}/{date_folder_name}/{sensor_type}/{sensor}")
                        if df.empty:
                            status = "Not Responding (Empty Data)"
                            with open(f"{cwd}/Central Asian Data/{country}/{level_folder}/{date_folder_name}/{country.lower()}_info.txt", 'a') as f:
                                f.write(sensor_line_v0(sensor_name, status) + "\n")
                        else:
                            status = "Responding (Data Available)"
                            with open(f"{cwd}/Central Asian Data/{country}/{level_folder}/{date_folder_name}/{country.lower()}_info.txt", 'a') as f:
                                f.write(sensor_line_v0(sensor_name, status) + "\n")

        except FileNotFoundError as e:
            print(f"Couldn't create log file for {country} {level_folder}. Error: {e}")

    return
