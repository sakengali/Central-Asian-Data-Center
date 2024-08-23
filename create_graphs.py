import os
import pandas as pd
import pdfkit
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter,DayLocator
import base64
from io import BytesIO
from typing import List, Tuple, Dict
from datetime import datetime, timedelta
from upload_data_to_drive import get_date_folder_name

BASE_DIR: str = os.path.dirname(os.path.abspath(__file__))
# with open(f"{BASE_DIR}/current_time.txt", "r") as f:
#     current_time: str = f.read()


level_folder: str = "Level 0"
date_folder_name: str = get_date_folder_name()


def get_period() -> str:
    today = datetime.today()
    today_str = today.strftime("%b-%d-%Y")
    sixteen_days_ago = (today - timedelta(days=16)).strftime("%b-%d-%Y")
    return f"{sixteen_days_ago} -- {today_str}"


def create_graphs(df: pd.DataFrame, sensor: str, measuring: str) -> str:
    df = df.copy()
    plt.figure(figsize=(18, 6))
    
    # Ensure Timestamp is datetime and sorted
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    df = df.sort_values('Timestamp')
    
    # Create a complete range of timestamps with the same frequency as the data
    full_range = pd.date_range(start=df['Timestamp'].min(), end=df['Timestamp'].max(), freq='H')
    
    # Reindex the DataFrame to include the full range, introducing NaNs where data is missing
    df = df.set_index('Timestamp').reindex(full_range).reset_index().rename(columns={'index': 'Timestamp'})
    
    plt.plot(df['Timestamp'], df[measuring], linestyle='-')
    plt.ylabel(measuring, fontsize=18)
    plt.title(f'{measuring}', fontsize=20, fontweight='bold')
    plt.grid(True)

    plt.gca().xaxis.set_major_locator(DayLocator())
    date_format = DateFormatter("%d")
    plt.gca().xaxis.set_major_formatter(date_format)

    plt.xticks(fontsize=16)
    plt.yticks(fontsize=16)
    plt.tight_layout()
    
    buf = BytesIO()
    plt.savefig(buf, format='png')
    plt.close()
    buf.seek(0)
    
    img_base64 = base64.b64encode(buf.read()).decode('utf-8')
    return f"data:image/png;base64,{img_base64}"


def summary(data: List[pd.DataFrame], measuring: str, freq: str = 'H') -> str:
    combined_df = pd.DataFrame()
    for df in data:
        df = df.copy()
        if df.empty:
            continue

        df['Timestamp'] = pd.to_datetime(df['Timestamp'])
        df.set_index('Timestamp', inplace=True)
        df_resampled = df.resample(freq).mean().reset_index()

        if combined_df.empty:
            combined_df = df_resampled[['Timestamp', measuring]].copy()
        else:
            combined_df = combined_df.merge(df_resampled[['Timestamp', measuring]], on='Timestamp', how='outer', suffixes=('', '_dup'))

    # Calculate the mean of the measuring column across all dataframes
    combined_df[measuring] = combined_df.filter(like=measuring).mean(axis=1)

    plt.figure(figsize=(18, 6))
    plt.plot(combined_df['Timestamp'], combined_df[measuring], label=f'Combined {measuring}')

    plt.ylabel(measuring, fontsize=18)
    plt.title(f'{measuring}', fontsize=20, fontweight='bold')

    plt.grid(True)
    plt.gca().xaxis.set_major_locator(DayLocator())
    date_format = DateFormatter("%d")
    plt.gca().xaxis.set_major_formatter(date_format)

    plt.xticks(fontsize=16)
    plt.yticks(fontsize=16)
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
                    summary_indoor_data.append(df)
                elif sensor_type == 'Outdoor Sensors':
                    summary_outdoor_data.append(df)
                plot_path_pm25 = create_graphs(df, sensor_name, 'PM 2.5')
                plot_path_rh = create_graphs(df, sensor_name, 'Relative Humidity')
                plot_path_temp = create_graphs(df, sensor_name, 'Temperature')
                if sensor_type == 'Indoor Sensors':
                    plot_path_co2 = create_graphs(df, sensor_name, 'CO2')
                elif sensor_type == 'Outdoor Sensors':
                    plot_path_co2 = ""

            if sensor_type == 'Indoor Sensors':
                data_indoor.append((sensor_name, latitude, longitude, sensor_type, status, (plot_path_pm25, plot_path_rh, plot_path_temp, plot_path_co2)))
            elif sensor_type == 'Outdoor Sensors':
                data_outdoor.append((sensor_name, latitude, longitude, sensor_type, status, (plot_path_pm25, plot_path_rh, plot_path_temp, plot_path_co2)))

    summary_indoor : dict[str, str] = {
        'PM 2.5': summary(summary_indoor_data, 'PM 2.5'),
        'RH': summary(summary_indoor_data, 'Relative Humidity'),
        'Temperture': summary(summary_indoor_data, 'Temperature'),
        'CO2': summary(summary_indoor_data, 'CO2') 
    }
    summary_outdoor : dict[str, str] = {
        'PM 2.5': summary(summary_outdoor_data, 'PM 2.5'),
        'RH': summary(summary_outdoor_data, 'Relative Humidity'),  
        'Temperture': summary(summary_outdoor_data, 'Temperature')
    }
    
    return data_indoor, data_outdoor, summary_indoor, summary_outdoor


def create_pdf() -> None:
    for country in ['KZ', 'KG', 'UZ']:
        try:
            data_indoor, data_outdoor, summary_indoor, summary_outdoor = get_data(country)
            html_content: str = """
            <html>
            <head>
                <style>
                    body { background-color: white; }
                    .page-break { page-break-after: always; }
                </style>
            </head>
            <body>
            """
            html_content += f"""
            <div>
                <p>Date: {datetime.today().strftime("%b-%d-%Y")}</p>
                <p>Summary of all sensors data in {country} for the period of {get_period()}</p>
                <p>Summary of Indoor Sensors:</p>
                <div><img src="{summary_indoor['PM 2.5']}" width="950"></div>
                <div><img src="{summary_indoor['RH']}" width="950"></div>
                <div><img src="{summary_indoor['Temperture']}" width="950"></div>
                <div><img src="{summary_indoor['CO2']}" width="950"></div>
            </div>
            <div class="page-break"></div>
            <div>
                <p>Summary of Outdoor Sensors:</p>
                <div><img src="{summary_outdoor['PM 2.5']}" width="950"></div>
                <div><img src="{summary_outdoor['RH']}" width="950"></div>
                <div><img src="{summary_outdoor['Temperture']}" width="950"></div
            </div>
            <div class="page-break"></div>
            """
            html_content += "<p>Indoor sensors</p>"
            for sensor_name, latitude, longitude, sensor_type, status, plot_path in data_indoor:
                if not status:
                    continue
                plot_img_tag_pm25: str = f'<img src="{plot_path[0]}" width="950">' if plot_path[0] else "No Data"
                plot_img_tag_rh: str = f'<img src="{plot_path[1]}" width="950">' if plot_path[1] else ""
                plot_img_tag_temp: str = f'<img src="{plot_path[2]}" width="950">' if plot_path[2] else ""
                plot_img_tag_co2: str = f'<img src="{plot_path[3]}" width="950">' if plot_path[3] else ""
                html_content += f"""
                <div>
                    <p>Sensor Name: {sensor_name}</p>
                    <p>Location: NIS ({latitude}, {longitude})</p>
                    <div>{plot_img_tag_pm25}</div>
                    <div>{plot_img_tag_rh}</div>
                    <div>{plot_img_tag_temp}</div>
                    <div>{plot_img_tag_co2}</div>
                </div>
                <div class="page-break"></div>
                """
            html_content += "<p>Outdoor sensors</p>"
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
                    <p>Sensor Name: {sensor_name}</p>
                    <p>Location: NIS ({latitude}, {longitude})</p>
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
        except Exception as e:
            print(f"Error processing data for {country}: {e}")
            continue