import os
import pandas as pd
from helpers import get_date_folder_name
from helpers import country_names, cwd

BASE_DIR = cwd
date_folder_name = get_date_folder_name()

def clean_main()-> None:
    for country in country_names.keys():
        try:
            if country == "KZ":
                clean_level = "Level 2"
            else:
                clean_level = "Level 1"

            print(f"Applying {clean_level} cleaning for {country}...")

            for sensor_type in ['Indoor Sensors', 'Outdoor Sensors']:
                clean_level_path = f"{BASE_DIR}/Central Asian Data/{country}/{clean_level}/{date_folder_name}/{sensor_type}"
                if not os.path.exists(clean_level_path):
                    os.makedirs(clean_level_path)

            for sensor_type in ['Indoor Sensors', 'Outdoor Sensors']:
                for sensor in os.listdir(f"{BASE_DIR}/Central Asian Data/{country}/Level 0/{date_folder_name}/{sensor_type}"):
                    df: pd.DataFrame = pd.read_csv(f"{BASE_DIR}/Central Asian Data/{country}/Level 0/{date_folder_name}/{sensor_type}/{sensor}")
                    if df.empty:
                        continue
                    else:
                        filter_table(df, clean_level, country, sensor_type, sensor)
            print(f"{clean_level} cleaning for {country} done.")
        except Exception as e:
            print(f"Cleaning for {country} failed. Error: {e}")
            continue


def filter_row(obj):
    check_cols = {"PM 2.5":[int, 0,1000], "CO2":[int, 400,10000], "VOC tVOC measurement":[float, 0,1885],
               "Relative Humidity":[int, 0,100]}
    for col in check_cols.keys():
        if col=="Device Status":
            if obj[col]!=0 and obj[col]!=4:
                return False
        elif col in obj.keys():
            if check_cols[col][0](obj[col]) < check_cols[col][1] or check_cols[col][0](obj[col]) > check_cols[col][2]:
                return False
    return True


def filter_table(df: pd.DataFrame, clean_level: str, country: str, sensor_type: str, sensor_name: str) -> None:
    df = df.drop([0], axis=0)
    df["Timestamp"] = pd.to_datetime(df['Timestamp'], format='%Y-%m-%dT%H:%M:%SZ')
    first_hour = df.loc[1, "Timestamp"].hour

    i = 2
    while True:
        if df.loc[i, "Timestamp"].hour != first_hour:
            df = df.drop(range(1, i), axis=0)
            break
        i += 1

    start_time = df.loc[df.index[0], 'Timestamp']
    second_time = df.loc[df.index[1], 'Timestamp']
    interval = (second_time - start_time).total_seconds()

    if interval <= 60:
        threshold = 100
    elif interval <= 900:
        threshold = 1000
    elif interval <= 3600:
        threshold = 5000 
    else:
        threshold = float('inf') 

    exclude_indices = []
    for ind in df.index:
        if not filter_row(df.loc[ind]) or df.loc[ind, "PM 2.5"] > threshold:
            exclude_indices.append(ind)

    # Filter rows that pass the preprocess_row check and PM 2.5 threshold
    filtered_df = df.drop(exclude_indices)

    cleaned_path = f"{BASE_DIR}/Central Asian Data/{country}/{clean_level}/{date_folder_name}/{sensor_type}/{sensor_name}"
    filtered_df.to_csv(cleaned_path, index=False)