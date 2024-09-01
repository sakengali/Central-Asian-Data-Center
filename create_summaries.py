import os
import pandas as pd
import numpy as np
import pdfkit
import gspread
import json
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter,DayLocator
from matplotlib.ticker import LogFormatter, NullFormatter, FixedLocator, FuncFormatter
import base64
from io import BytesIO
from typing import List, Tuple, Dict
from datetime import datetime, timedelta
from upload_data_to_drive import get_date_folder_name
from helpers import country_names, cwd

#getting the correct path
BASE_DIR: str = cwd

gc = gspread.service_account(filename='./cosmic-talent-416001-3c711f8ccf2e.json')

level_folder: str = "Level 0"
date_folder_name: str = get_date_folder_name()


def get_period() -> str:
    today = datetime.today()
    today_str = today.strftime("%m-%d-%Y")
    sixteen_days_ago = (today - timedelta(days=16)).strftime("%m-%d-%Y")
    return f"{sixteen_days_ago} -- {today_str}"


def create_graphs(df: pd.DataFrame, sensor: str, measuring: str) -> str:
    df = df.copy()
    plt.figure(figsize=(18, 6))
    
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    df.set_index('Timestamp', inplace=True)
    df_resampled = df.resample("H").mean().reset_index()

    plt.plot(df_resampled['Timestamp'], df_resampled[measuring])
    
    plt.ylabel(measuring, fontsize=18)
    plt.title(f'{measuring}', fontsize=20, fontweight='bold')
    plt.grid(True)

    plt.gca().xaxis.set_major_locator(DayLocator())
    date_format = DateFormatter("%d")
    plt.gca().xaxis.set_major_formatter(date_format)
    
    if measuring in ['PM 2.5', 'CO2'] and df_resampled[measuring].max() > df_resampled[measuring].mean() * 2:
        plt.yscale('log', base=2)
        plt.gca().yaxis.set_major_formatter(LogFormatter(base=2, labelOnlyBase=False))

    plt.xticks(fontsize=16)
    plt.yticks(fontsize=16)
    plt.tight_layout(pad=3)
    
    buf = BytesIO()
    plt.savefig(buf, format='png')
    plt.close()
    buf.seek(0)
    
    img_base64 = base64.b64encode(buf.read()).decode('utf-8')
    return f"data:image/png;base64,{img_base64}"


def summary(data: List[Dict[str, pd.DataFrame]], measuring: str, freq: str = 'H') -> str:
    plt.figure(figsize=(18, 6))

    for i, d in enumerate(data):
        for sensor_name, df in d.items():
            # print(sensor_name)
            df = df.copy()
            if df.empty:
                continue
            
            df['Timestamp'] = pd.to_datetime(df['Timestamp'])
            df.set_index('Timestamp', inplace=True)
            df_resampled = df.resample(freq).mean().reset_index()

            plt.plot(df_resampled['Timestamp'], df_resampled[measuring], label=f'{sensor_name}')
    
    plt.ylabel(measuring, fontsize=18)
    plt.title(f'{measuring}', fontsize=20, fontweight='bold')
    plt.grid(True)
    plt.gca().xaxis.set_major_locator(DayLocator())
    date_format = DateFormatter("%d")
    plt.gca().xaxis.set_major_formatter(date_format)

    if measuring in ['PM 2.5', 'CO2'] and df_resampled[measuring].max() > df_resampled[measuring].mean() * 2:
        plt.yscale('log', base=2)
        plt.gca().yaxis.set_major_formatter(LogFormatter(base=2, labelOnlyBase=False))

    plt.xticks(fontsize=16)
    plt.yticks(fontsize=16)
    plt.legend(fontsize=16, loc='center left', bbox_to_anchor=(1, 0.5))
    plt.tight_layout()

    buf = BytesIO()
    plt.savefig(buf, format='png')
    plt.close()
    buf.seek(0)
    
    img_base64 = base64.b64encode(buf.read()).decode('utf-8')
    return f"data:image/png;base64,{img_base64}"


def get_data(country: str) -> Tuple[
                            List[Tuple[str, str, str, str, bool, Tuple[str, str, str, str]]], 
                            List[Tuple[str, str, str, str, bool, Tuple[str, str, str, str]]],
                            Dict[str, str], 
                            Dict[str, str]
                            ]:
    data_indoor: List[Tuple[str, str, bool, str]] = []
    data_outdoor: List[Tuple[str, str, bool, str]] = []
    summary_indoor_data: List[pd.DataFrame] = []
    summary_outdoor_data: List[pd.DataFrame] = []
    status: bool

    for sensor_type in ['Indoor Sensors', 'Outdoor Sensors']:
        for sensor in os.listdir(f"{BASE_DIR}/Central Asian Data/{country}/{level_folder}/{date_folder_name}/{sensor_type}"):
            sensor_name: str = sensor.split('-')[0]
            df: pd.DataFrame = pd.read_csv(f"{BASE_DIR}/Central Asian Data/{country}/{level_folder}/{date_folder_name}/{sensor_type}/{sensor}")
            if df.empty:
                status = False
                latitude: str = "None"
                longitude: str = "None"
                plot_path_pm25: str = ""
                plot_path_rh: str = ""
                plot_path_temp: str = ""
                plot_path_co2: str = ""
            else:
                status = True
                latitude: str = str(df['Latitude'][0])
                longitude: str = str(df['Longitude'][0])
                if sensor_type == 'Indoor Sensors':
                    summary_indoor_data.append({sensor_name: df})
                elif sensor_type == 'Outdoor Sensors':
                    summary_outdoor_data.append({sensor_name: df})
                plot_path_pm25: str = create_graphs(df, sensor_name, 'PM 2.5')
                plot_path_rh: str = create_graphs(df, sensor_name, 'Relative Humidity')
                plot_path_temp: str = create_graphs(df, sensor_name, 'Temperature')
                if sensor_type == 'Indoor Sensors':
                    plot_path_co2: str = create_graphs(df, sensor_name, 'CO2')
                elif sensor_type == 'Outdoor Sensors':
                    plot_path_co2: str = ""

            if sensor_type == 'Indoor Sensors':
                data_indoor.append((sensor_name, latitude, longitude, sensor_type, status, (plot_path_pm25, plot_path_rh, plot_path_temp, plot_path_co2)))
            elif sensor_type == 'Outdoor Sensors':
                data_outdoor.append((sensor_name, latitude, longitude, sensor_type, status, (plot_path_pm25, plot_path_rh, plot_path_temp, plot_path_co2)))
        data_indoor_sorted = sorted(data_indoor, key=lambda x: (len(x[0]), x[0]))
        data_outdoor_sorted = sorted(data_outdoor, key=lambda x: (len(x[0]), x[0]))
        
    summary_indoor_data_sorted = sorted(summary_indoor_data, key=lambda x: (len(list(x.keys())[0]), list(x.keys())[0]))
    summary_outdoor_data_sorted = sorted(summary_outdoor_data, key=lambda x: (len(list(x.keys())[0]), list(x.keys())[0]))

    summary_indoor : Dict[str, str] = {
        'PM 2.5': summary(summary_indoor_data_sorted, 'PM 2.5'),
        'RH': summary(summary_indoor_data_sorted, 'Relative Humidity'),
        'Temperture': summary(summary_indoor_data_sorted, 'Temperature'),
        'CO2': summary(summary_indoor_data_sorted, 'CO2')
    }
    summary_outdoor : Dict[str, str] = {
        'PM 2.5': summary(summary_outdoor_data_sorted, 'PM 2.5'),
        'RH': summary(summary_outdoor_data_sorted, 'Relative Humidity'),
        'Temperture': summary(summary_outdoor_data_sorted, 'Temperature')
    }
    
    return data_indoor_sorted, data_outdoor_sorted, summary_indoor, summary_outdoor


def create_pdf() -> None:
    for country in ['KZ', 'KG', 'UZ']:
        print(f"Creating summary pdf for {country} ...")
        try:
            data_indoor, data_outdoor, summary_indoor, summary_outdoor = get_data(country)

            with open("config.json", "r") as f:
                config = json.load(f)
                sheet_key = config[f"{str.lower(country)}_client_spreadsheet"]
            sh = gc.open_by_key(sheet_key)
            
            sensors_locs: Dict = {}
            for w in sh.worksheets():
                tmp_name: List = w.col_values(1)[1:]
                tmp_loc: List = w.col_values(6)[1:]
                sensors_locs.update(dict(zip(tmp_name, tmp_loc)))            
            html_content: str = """
            <html>
            <head>
                <style>
                    body { background-color: white; font-family: 'Times New Roman', Times, serif; }
                    .page-break { page-break-after: always; }
                    .sensor-type { font-size: 24px; font-weight: bold; }
                    .sensors-name { font-size: 20px; }
                    .sensor-location { font-size: 18px; }
                    .intro { font-size: 20px;}
                </style>
            </head>
            <body>
            """
            html_content += f"""
            <div class='intro'>
                <div style="font-size: 24px; font-weight: bold; text-align: center; margin-left: 20%; margin-right: 20%;">Summary of all sensors in {country_names[country]} for the period of {get_period()}</div>
                Date: {datetime.today().strftime("%m-%d-%Y")}<br><br>
                Summary of Indoor Sensors:<br>
                <div><img src="{summary_indoor['PM 2.5']}" width="950"></div>
                <div><img src="{summary_indoor['RH']}" width="950"></div>
                <div><img src="{summary_indoor['Temperture']}" width="950"></div>
                <div><img src="{summary_indoor['CO2']}" width="950"></div>
            </div>
            <div class="page-break"></div>
            <div class='intro'>
                <p>Summary of Outdoor Sensors:</p>
                <div><img src="{summary_outdoor['PM 2.5']}" width="950"></div>
                <div><img src="{summary_outdoor['RH']}" width="950"></div>
                <div><img src="{summary_outdoor['Temperture']}" width="950"></div
            </div>
            <div class="page-break"></div>
            """
            html_content += "<p class='sensor-type'>Indoor sensors</p>"
            for sensor_name, latitude, longitude, sensor_type, status, plot_path in data_indoor:
                if not status:
                    continue
                plot_img_tag_pm25: str = f'<img src="{plot_path[0]}" width="950">' if plot_path[0] else "No Data"
                plot_img_tag_rh: str = f'<img src="{plot_path[1]}" width="950">' if plot_path[1] else ""
                plot_img_tag_temp: str = f'<img src="{plot_path[2]}" width="950">' if plot_path[2] else ""
                plot_img_tag_co2: str = f'<img src="{plot_path[3]}" width="950">' if plot_path[3] else ""
                html_content += f"""
                <div>
                    <p class='sensors-name'>Sensor <b>{sensor_name}</b></p>
                    <p class='sensor-location' style='font-size:18px;'> <b>{sensors_locs[sensor_name]} ({latitude}, {longitude})</b></p>
                    <div>{plot_img_tag_pm25}</div>
                    <div>{plot_img_tag_rh}</div>
                    <div>{plot_img_tag_temp}</div>
                    <div>{plot_img_tag_co2}</div>
                </div>
                <div class="page-break"></div>
                """
            html_content += "<p class='sensor-type'>Outdoor sensors</p>"
            for sensor_name, latitude, longitude, sensor_type, status, plot_path in data_outdoor:
                if not status:
                    continue
                status_text: str = "Responding (Data Available)" if status else "Not Responding (Empty Data)"
                plot_img_tag_pm25: str = f'<img src="{plot_path[0]}" width="950">' if plot_path[0] else "No Data"
                plot_img_tag_rh: str = f'<img src="{plot_path[1]}" width="950">' if plot_path[1] else ""
                plot_img_tag_temp: str = f'<img src="{plot_path[2]}" width="950">' if plot_path[2] else ""
                plot_img_tag_co2: str = f'<img src="{plot_path[3]}" width="950">' if plot_path[3] else ""
                html_content += f"""
                <div>
                    <p class='sensors-name'>Sensor <b>{sensor_name}</b></p>
                    <p class='sensor-location' style='font-size:18px;'><b>{sensors_locs[sensor_name]} ({latitude}, {longitude})</b></p>
                    <div>{plot_img_tag_pm25}</div>
                    <div>{plot_img_tag_rh}</div>
                    <div>{plot_img_tag_temp}</div>
                    <div>{plot_img_tag_co2}</div>
                </div>
                <div class="page-break"></div>
                """
            html_content += "</body></html>"

            output_pdf_path: str = f"{BASE_DIR}/Central Asian Data/{country}/Level 0/{date_folder_name}/{country.lower()}_summary.pdf"
            pdfkit.from_string(html_content, output_pdf_path)
            print(f"Summary pdf created successfully for {country}")
        except Exception as e:
            print(f"Error processing data for {country}: {e}")
            continue

#just ckecking