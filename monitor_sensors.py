# monitor_sensors.py

import pandas as pd
import os
from datetime import datetime
from typing import List, Dict, Optional, Set
from helpers import Sensor, get_sensors_info, get_date_folder_name, get_level_0_folder, sensors_info, cwd

def update_responding_status(sensors: List[Sensor], date: str, database_path: str) -> None:
    """
    Input: result of get_sensors_info transformed into a list of Sensor objects, date, path where .csv file is stored
    Updates the sensors status database with current session data
    """

    data = []
    for sensor in sensors:
        data.append({
            'Sensor': f"{sensor.name} ({sensor.country})",
            date : int(sensor.get_status())
        })
    df = pd.DataFrame(data)
 
    if not os.path.exists(database_path):
        df.to_csv(database_path, index=False)
    else:
        db = pd.read_csv(database_path)

        #append new column
        if date in db.columns:
            db = db.drop(date, axis=1)

        db = pd.concat([db, df[date]], axis=1)
        db.to_csv(database_path, index=False)

        #df.to_csv(database_path, mode='a', header=False, index=False)
    

def get_sensors_off_twice(database_path: str, date: str) -> Optional[Set[str]]:
    """
    Input: path where .csv file is stored, and some date
    Return: list of sensors that were off in both current and previous time when sensros were checked
    
    """
    try:
        df = pd.read_csv(database_path)
    except (FileNotFoundError, pd.errors.ParserError):
        print("sensor_status_db.csv not found")
        return None

    columns = df.columns.tolist()
    session_dates = columns[1:]
    sensor_names = df['Sensor'].tolist()
    
    if len(session_dates) < 2:
        return None
    
    current_session = df[date].values
    current_session = pd.DataFrame({'Sensor': sensor_names, 'Status': current_session})
    previous_session_date = session_dates[-2]
    previous_session = df[previous_session_date].values
    previous_session = pd.DataFrame({'Sensor': sensor_names, 'Status': previous_session})
    #print(current_session); print(previous_session)

    merged = pd.merge(current_session, previous_session, on='Sensor', suffixes=('_current', '_previous'))
    off_twice = merged[
        (merged['Status_current'] == False) &
        (merged['Status_previous'] == False)
    ]
    return set(off_twice['Sensor'].tolist())

def monitor(write_to_info : bool = True) -> None:
    """
    Main calling function
    """

    print("updating sensor status monitoring database")
    
    database_path : str = f"{cwd}/sensor_status_db.csv"
    current_date : str = get_date_folder_name()
    off_twice = set()
    line : str = ""

    for country in ['KZ']: #for now only KZ, KG and UZ will need to fill the tables
        sensors_dict = sensors_info[country]
        sensors = list(sensors_dict.values())
        sensors = sorted(sensors, key=lambda x: x.name)

        update_responding_status(sensors, current_date, database_path)
        
        temp_set = get_sensors_off_twice(database_path, current_date)
        if temp_set:
            off_twice.update(temp_set)

    if off_twice:
        line = "Sensors that are off for two consecutive sessions: " + ", ".join(off_twice) + "."
    else:
        line = "No sensors found off for two consecutive sessions"

    if write_to_info:
        with open(f"{cwd}/Central Asian Data/{country}/{get_level_0_folder(country)}/{current_date}/{country.lower()}_info.txt", 'a') as f:
            f.write(f"\n\n{line}\n")

    return line