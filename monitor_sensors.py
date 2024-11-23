# monitor_sensors.py

import pandas as pd
import os
from datetime import datetime
from typing import List, Dict, Optional, Set
from helpers import Sensor, get_sensors_info

def update_responding_status(sensors: List[Sensor], date: datetime, database_path: str) -> None:
    """
    Input: result of get_sensors_info transformed into a list of Sensor objects, date, path where .csv file is stored
    Updates the sensors status database with current session data
    """
    data = []
    for sensor in sensors:
        data.append({
            'Sensor Name': sensor.name,
            'Date': date,
            'Country': sensor.country,
            'Responding Status': sensor.is_responding()
        })
    df = pd.DataFrame(data)
 
    if not os.path.exists(database_path):
        df.to_csv(database_path, index=False)
    else:
        df.to_csv(database_path, mode='a', header=False, index=False)
    
 

def get_sensors_off_twice(database_path: str, date: str) -> Optional[Set[str]]:
    """
    Input: path where .csv file is stored, and some date
    Return: list of sensors that were off in both current and previous time when sensros were checked
    
    """
    try:
        df = pd.read_csv(database_path)
    except (FileNotFoundError, pd.errors.ParserError):
        return None

    session_dates = sorted(df['Date'].unique())
    
    if len(session_dates) < 2:
        return None
    
    
    current_session = df[df['Date'] == date]
    previous_session_date = df['Date'].unique()[-2]
    previous_session = df[df['Date'] == previous_session_date]
    merged = pd.merge(current_session, previous_session, on='Sensor Name', suffixes=('_current', '_previous'))
    off_twice = merged[
        (merged['Responding Status_current'] == False) &
        (merged['Responding Status_previous'] == False)
    ]
    return set(off_twice['Sensor Name'].tolist())

def monitor() -> None:
    """
    Main calling function
    """
    
    database_path : str = "sensor_status_db.csv"
    current_date : datetime = datetime.now().strftime("%Y-%m-%d")

    for country in ['KZ']:
        sensors_dict = get_sensors_info(country)
        sensors = list(sensors_dict.values())

        update_responding_status(sensors, current_date, database_path)
        
        off_twice = get_sensors_off_twice(database_path, current_date)
        if off_twice:
            print("Sensors off for two consecutive sessions:", off_twice)
        else:
            print("No sensors found off for two consecutive sessions")

