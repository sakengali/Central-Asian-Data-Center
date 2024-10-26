import os
import re
import base64
from io import BytesIO
import pdfkit
import pandas as pd
import matplotlib.pyplot as plt
from typing import List, Tuple, Dict
from datetime import datetime, timedelta
from helpers import get_date_folder_name
from helpers import country_names, cwd, get_level_0_folder

BASE_DIR: str = cwd

date_folder_name: str = get_date_folder_name()

def get_period() -> str:
    today = datetime.today()
    today_str = today.strftime("%m-%d-%Y")
    sixteen_days_ago = (today - timedelta(days=16)).strftime("%m-%d-%Y")
    return f"{sixteen_days_ago} -- {today_str}"

def split_list(input_list, chunk_size=10):
    return [input_list[i:i + chunk_size] for i in range(0, len(input_list), chunk_size)]

def preprocess_row(obj):
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

def preprocess(df : pd.DataFrame) -> int:
    df = df.drop([0], axis=0)
    df["Timestamp"] = pd.to_datetime(df['Timestamp'], format='%Y-%m-%dT%H:%M:%SZ')
    first_hour = df.loc[1,"Timestamp"].hour

    i = 2
    while True:
        if df.loc[i,"Timestamp"].hour!=first_hour:
            df = df.drop(range(1,i), axis=0)
            break
        i+=1
    start_time = df.loc[df.index[0],['Timestamp']].item()
    second_time = df.loc[df.index[1],['Timestamp']].item()
    interval = (second_time-start_time).total_seconds()
    end_time = df.loc[df.index[-1],['Timestamp']].item()
    total_hours = (end_time-start_time).total_seconds()//3600
    
    exclude_indices = []
    for ind in df.index:
        if not preprocess_row(df.loc[ind]):
            exclude_indices.append(ind)

    ind = df.index[0]
    cur_ind = -1
    cur_hour = -1
    hour_list = []

    count = 0
    while True:
        if cur_ind==-1:
            cur_ind=ind
            cur_hour=df.loc[ind].Timestamp.hour
            hour_list = [ind]
            ind+=1
        elif df.loc[ind].Timestamp.hour!=cur_hour or ind==df.index[-1]:
            if ind==df.index[-1]:
                hour_list.append(ind)
            hour_list = [x for x in hour_list if x not in exclude_indices]
            
            if len(hour_list)>=3600/interval*0.75:
                count+=1

            cur_ind=-1
            hour_list=[]
        else:
            hour_list.append(ind)
            ind+=1
        if ind==df.index[-1]:
            break
    uptime = round(count/total_hours*100)
    return uptime

def calculate_uptime(country : str) -> Dict[str, float]:

    level_folder = get_level_0_folder(country)

    uptimes: Dict = {}
    try:
        for sensor_type in ['Indoor Sensors', 'Outdoor Sensors']:
            for sensor in os.listdir(f"{BASE_DIR}/Central Asian Data/{country}/{level_folder}/{date_folder_name}/{sensor_type}"):
                match = re.match(r'([A-Za-z0-9-]+)-\w+-\d{4}', sensor)
                if match:
                    sensor_name = match.group(1)
                else:
                    sensor_name = sensor.split('-')[0]
                df: pd.DataFrame = pd.read_csv(f"{BASE_DIR}/Central Asian Data/{country}/{level_folder}/{date_folder_name}/{sensor_type}/{sensor}")
                if df.empty:
                    continue
                else:
                    uptimes[sensor_name] = preprocess(df)

        return uptimes
    except Exception as e:
        print(f"Uptime calculations for {country} failed. Error: {e}")
        return


def create_uptime_graph() -> None:
    
    for country in ['KZ', 'KG', 'UZ']:

        level_folder = get_level_0_folder(country)

        uptimes: Dict = {}
        print(f"Creating uptime graphs pdf for {country} ...")
        try:
            for sensor_type in ['Indoor Sensors', 'Outdoor Sensors']:
                for sensor in os.listdir(f"{BASE_DIR}/Central Asian Data/{country}/{level_folder}/{date_folder_name}/{sensor_type}"):
                    match = re.match(r'([A-Za-z0-9-]+)-\w+-\d{4}', sensor)
                    if match:
                        sensor_name = match.group(1)
                    else:
                        sensor_name = sensor.split('-')[0]
                    df: pd.DataFrame = pd.read_csv(f"{BASE_DIR}/Central Asian Data/{country}/{level_folder}/{date_folder_name}/{sensor_type}/{sensor}")
                    if df.empty:
                        continue
                    else:
                        uptimes[sensor_name] = preprocess(df)

            imgs : List = []

            for chunk in split_list(sorted(list(uptimes.keys()), key=lambda x: (len(x[0]), x[0])), 15):
                plt.figure(figsize=(18, 6))
                for sensor in chunk:
                    bar = plt.bar(sensor, uptimes[sensor], label=sensor)
                    
                    # Add Y value annotation
                    plt.text(bar[0].get_x() + bar[0].get_width() / 2, bar[0].get_height(), 
                            f'{uptimes[sensor]:.2f}', ha='center', va='bottom')

                plt.xlabel('Sensor')
                plt.ylabel('Uptime %')
                plt.xticks(rotation=45)
                #plt.legend()
                plt.tight_layout()
                # plt.savefig('uptime_barchart1.png', dpi=300)
                buf = BytesIO()
                plt.savefig(buf, format='png')
                plt.close()
                buf.seek(0)
                img_base64 = base64.b64encode(buf.read()).decode('utf-8')
                img: str = f"data:image/png;base64,{img_base64}"
                imgs.append(img)

            html_content: str = """
                <head>
                <style>
                    body { background-color: white; font-family: 'Times New Roman', Times, serif; }
                    .page-break { page-break-after: always; }
                </style>
                </head>
                <body>
            """
            html_content += f'<div style="font-size: 24px; font-weight: bold; text-align: center; margin-left: 20%; margin-right: 20%;">Uptime of all sensors in {country_names[country]} for the period of {get_period()}</div><br><br>'

            for img in imgs:
                html_content += f"""
                            <div><img src="{img}" width="950"></div>
                """
            html_content += "</body></html>"
            
            output_pdf_path: str = f"{BASE_DIR}/Central Asian Data/{country}/{level_folder}/{date_folder_name}/{country.lower()}_uptime.pdf"
            pdfkit.from_string(html_content, output_pdf_path)
            print(f"Uptime pdf created successfully for {country}")
        
        except Exception as e:
            try:
                html_content: str = """
                <html>
                <head>
                    <style>
                        body { background-color: white; font-family: 'Times New Roman', Times, serif; font-size: 24px; margin-left: 20%; margin-right: 20%; margin-top: 30%; margin-bottom: 30%; text-align: center;}
                    </style>
                </head>
                <body>
                """
                html_content += f"""
                <p> Could not retrieve data for the sensors in {country_names[country]}. </p>
                """
                html_content += "</body></html>"
                output_pdf_path: str = f"{BASE_DIR}/Central Asian Data/{country}/{level_folder}/{date_folder_name}/{country.lower()}_uptime.pdf"
                pdfkit.from_string(html_content, output_pdf_path)
                print(f"Error processing data for {country}: {e}. An empty pdf was created.")
            except:
                pass
            print(f"Error processing data for {country}: {e}")
            continue