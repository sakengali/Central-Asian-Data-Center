import pandas as pd
import os
from upload_data_to_drive import get_date_folder_name
from datetime import datetime
from typing import NamedTuple, List
import csv

cwd = "/home/dhawal/Air Quality Analysis Central Asia/Central-Asian-Data-Center"
level_folder : str = "Level 0"
date_folder_name = get_date_folder_name()

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
        return self.is_deployed and (not self.is_responding)

def get_sensors_info(country : str) -> List[Sensor]:
    sensors : List[Sensor] = []
    
    with open(f"sensors_info/{country.lower()}_deployed_sensors.csv", mode='r', newline='') as file:
        reader = csv.DictReader(file)
        
        for row in reader:
            person = Sensor(
                name = row['sensor_name'],
                sensor_type=row['sensor_type'],
                country = row['country'].upper(),
                is_deployed=bool(int((row['is_deployed']))),
                location=row['location'],
                owner=row['owner'],
            )
            sensors.append(person)
    
    return sensors


def sensor_line(sensor_name, status) -> str:
    if len(sensor_name) < 8:
        return f"{sensor_name}\t" + "\t"*(2) + f"{status}"
    elif len(sensor_name) < 16:
        return f"{sensor_name}\t" + "\t"*(1) + f"{status}"
    else:
        return f"{sensor_name}\t" + f"{status}"    


def create_info_file():

    try:
        for country in ['KZ', 'KG', 'UZ']:

            with open(f"{cwd}/Central Asian Data/{country}/{level_folder}/{date_folder_name}/info.txt", 'w') as f:
                f.write(f"The data of {country} sensors was downloaded on {datetime.now()}\n\n")
                f.write(f"Date Folder Name: {date_folder_name}\n\n")
                f.write(f"Level Folder: {level_folder}\n\n")
                f.write("Sensor\t\tStatus\n")

            for sensor_type in ['Indoor Sensors', 'Outdoor Sensors']:
                with open(f"{cwd}/Central Asian Data/{country}/{level_folder}/{date_folder_name}/info.txt", 'a') as f:
                    f.write(f"\n({sensor_type}):\n")
                for sensor in os.listdir(f"{cwd}/Central Asian Data/{country}/{level_folder}/{date_folder_name}/{sensor_type}"):
                    sensor_name = sensor.split('-')[0]

                    df = pd.read_csv(f"{cwd}/Central Asian Data/{country}/{level_folder}/{date_folder_name}/{sensor_type}/{sensor}")
                    if df.empty:
                        status = "Not Responding (Empty Data)"
                        with open(f"{cwd}/Central Asian Data/{country}/{level_folder}/{date_folder_name}/info.txt", 'a') as f:
                            f.write(sensor_line(sensor_name, status) + "\n")
                    else:
                        status = "Responding (Data Available)"
                        with open(f"{cwd}/Central Asian Data/{country}/{level_folder}/{date_folder_name}/info.txt", 'a') as f:
                            f.write(sensor_line(sensor_name, status) + "\n")
    except FileNotFoundError as e:
        print(f"Couldn't create log file for {country}. Error: {e}")

    return
