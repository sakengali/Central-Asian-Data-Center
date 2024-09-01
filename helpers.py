import pandas as pd
import os
from datetime import datetime
from typing import NamedTuple, List, Dict
import csv

cwd : str = "/home/dhawal/Air Quality Analysis Central Asia/Central-Asian-Data-Center"
level_folder : str = "Level 0"

country_names = {
    'KZ': 'Kazakhstan',
    'KG': 'Kyrgyzstan',
    'UZ': 'Uzbekistan'
}


def get_date_folder_name() -> str:
    """ returns date folder name"""

    this_month = pd.Timestamp.today().strftime("%b-%Y")
    month_part = '1' if pd.Timestamp.today().day <= 16 else '2'
    #return f"{this_month}-{month_part}"
    return "Jul-2024-2"

date_folder_name : str = get_date_folder_name()

class Sensor(NamedTuple):
    name : str
    sensor_type : str
    country : str
    is_deployed : bool
    location : str
    owner : str
    
    def is_responding(self):
        sensor_file_name = f"{self.name}-{date_folder_name[:8]}.csv"
        df = pd.read_csv(f"{cwd}/Central Asian Data/{self.country}/{level_folder}/{date_folder_name}/{self.sensor_type}/{sensor_file_name}")

        return not df.empty

    def is_turned_off(self):
        return self.is_deployed and not self.is_responding()

def get_sensors_info(country : str) -> List[Sensor]:

    """ 
        inputs: country 
        returns a Dict[sensor_name : sensor_info] for country's sensors
    """

    sensors : Dict[str, Sensor] = {}
    
    with open(f"{cwd}/sensors_info/{country.lower()}_deployed_sensors.csv", mode='r', newline='') as file:
        reader = csv.DictReader(file)
        
        for row in reader:
            sensor = Sensor(
                name = row['sensor_name'],
                sensor_type=row['sensor_type'],
                country = row['country'].upper(),
                is_deployed=bool(int((row['is_deployed']))),
                location=row['location'],
                owner=row['owner'],
            )
            
            sensors[sensor.name] = sensor
    
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

    """ creates the {country}_info.txt file """

    try:
        for country in ['KZ', 'KG', 'UZ']:

            with open(f"{cwd}/Central Asian Data/{country}/{level_folder}/{date_folder_name}/{country.lower()}_info.txt", 'w') as f:
                f.write(f"The data of {country} sensors was downloaded on {datetime.now()}\n\n")
                f.write(f"Date Folder Name: {date_folder_name}\n\n")
                f.write(f"Level Folder: {level_folder}\n\n")
                f.write("Sensor\t\tLocation\t\t\tStatus\t\t\tResponse\n")

            try:
                sensors = get_sensors_info(country)
                for sensor_type in ['Indoor Sensors', 'Outdoor Sensors']:
                    with open(f"{cwd}/Central Asian Data/{country}/{level_folder}/{date_folder_name}/{country.lower()}_info.txt", 'a') as f:
                        f.write(f"\n({sensor_type}):\n")
                    for sensor in sorted(os.listdir(f"{cwd}/Central Asian Data/{country}/{level_folder}/{date_folder_name}/{sensor_type}")):
                        sensor_name : str = sensor.split('-')[0]
                        sensor : Sensor = sensors[sensor_name]

                        with open(f"{cwd}/Central Asian Data/{country}/{level_folder}/{date_folder_name}/{country.lower()}_info.txt", 'a') as f:
                            f.write(sensor_line_v1(sensor) + "\n")

            except FileNotFoundError as e:
                print(e)
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
        print(f"Couldn't create log file for {country}. Error: {e}")

    return

#TODO:
# request kg_deployed_sensors.csv and kz_deployed_sensors.csv files, and remove reduntant part of the code for old info_txt file.