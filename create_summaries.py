import os
import re
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
from helpers import get_date_folder_name, get_sensors_info
from helpers import country_names, cwd, get_level_0_folder, get_level_1_folder, get_sensors_info, Sensor, sensors_info
from create_uptime_pdf import calculate_uptime
import seaborn as sns
import sys
from collections import defaultdict

#getting the correct path to this directory
BASE_DIR: str = cwd

colors = sns.color_palette(palette="deep", n_colors=10)

# gc = gspread.service_account(filename=f'{cwd}/cosmic-talent-416001-3c711f8ccf2e.json')

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
    df_resampled = df.resample("h").mean().reset_index()

    plt.plot(df_resampled['Timestamp'], df_resampled[measuring])
    
    plt.ylabel(measuring, fontsize=18)
    plt.title(f'{measuring}', fontsize=20, fontweight='bold')
    plt.grid(True, axis='x', linestyle='--', linewidth=0.5)

    plt.gca().xaxis.set_major_locator(DayLocator())
    date_format = DateFormatter("%d")
    plt.gca().xaxis.set_major_formatter(date_format)
    
    if measuring in ['PM 2.5', 'CO2'] and df_resampled[measuring].max() > df_resampled[measuring].mean() * 2:
        if measuring == 'PM 2.5':
            plt.ylim(bottom=1)
        # elif measuring == 'CO2':
        #     plt.ylim(top=400)
        def custom_yscale(y, pos):
            if y > 1:
                return f'{y:.0f}'
            else:
                return f'{y:.2f}'
        plt.yscale('log', base=2)
        plt.gca().yaxis.set_major_formatter(FuncFormatter(custom_yscale))


    plt.xticks(fontsize=16)
    plt.yticks(fontsize=16)
    plt.tight_layout(pad=3)
    
    buf = BytesIO()
    plt.savefig(buf, format='png')
    plt.close()
    buf.seek(0)
    
    img_base64 = base64.b64encode(buf.read()).decode('utf-8')
    return f"data:image/png;base64,{img_base64}"


def summary(data: List[Dict[str, pd.DataFrame]], measuring: str, country : str, freq: str = 'h') -> str:

    cities = []
    for _, d in enumerate(data):
        for sensor_name, _ in d.items():
            cities.append(sensors_info[country][sensor_name].city)
    cities = list(set(cities))
    if len(cities) > 1 and cities[0] == '':
        del cities[0]
        cities.append('')
    data_city = {city: pd.DataFrame() for city in cities}
    city_colors = {city: colors[i] for i, city in enumerate(cities)}

    #create an empty df
    #df_all = pd.DataFrame()
    #print(cities)
    for city in cities:
        for _, d in enumerate(data):
            for sensor_name, df in d.items():
                
                if sensors_info[country][sensor_name].city == city:
                    df = df.copy()
                    if df.empty:
                        continue
                    
                    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
                    df.set_index('Timestamp', inplace=True)
                    df_resampled = df.resample(freq).mean().reset_index()
                    df_resampled['Timestamp'] = df_resampled['Timestamp'].dt.tz_localize(None)

                    data_city[city] = pd.concat([data_city[city], df_resampled], axis=0, ignore_index=True)
                    #if not os.path.exists(f"{BASE_DIR}/test"):
                    #    os.makedirs(f"{BASE_DIR}/test")
                    #data_city[city].to_csv(f"{BASE_DIR}/test/df_all_test.csv")

                #plt.plot(df_resampled['Timestamp'], df_resampled[measuring], label=f'{sensor_name}')
    
    imgs_of_plts = {}
    for city, df in data_city.items():
        plt.figure(figsize=(18, 6))

        foo = False
        if city == '':
            foo = True
            if len(cities) == 1:
                city = 'all'
            else:
                city = 'other'

        sns.lineplot(data=df, x='Timestamp', y=measuring, errorbar='sd', marker='.', markersize=10, label = city, color=city_colors[''] if foo else city_colors[city])

        plt.xlabel('Date', fontsize=18)
        plt.ylabel(measuring, fontsize=18)
        plt.title(f'{measuring}', fontsize=20, fontweight='bold')
        plt.grid(True, axis='x', linestyle='--', linewidth=0.5)
        plt.gca().xaxis.set_major_locator(DayLocator())
        date_format = DateFormatter("%d")
        plt.gca().xaxis.set_major_formatter(date_format)

        if measuring in ['PM 2.5', 'CO2'] and df_resampled[measuring].max() > df_resampled[measuring].mean() * 2:
            if measuring == 'PM 2.5':
                plt.ylim(bottom=1)
            elif measuring == 'CO2':
                plt.ylim(bottom=400)
            def custom_yscale(y, pos):
                if y > 1:
                    return f'{y:.0f}'
                else:
                    return f'{y:.2f}'
            plt.yscale('log', base=2)
            plt.gca().yaxis.set_major_formatter(FuncFormatter(custom_yscale))


        plt.xticks(fontsize=16)
        plt.yticks(fontsize=16)
        plt.legend(fontsize=16, loc='center left', bbox_to_anchor=(1, 0.5))
        plt.tight_layout()

        buf = BytesIO()
        plt.savefig(buf, format='png')
        plt.close()
        buf.seek(0)
    
        img_base64 = base64.b64encode(buf.read()).decode('utf-8')

        imgs_of_plts[city] = f"data:image/png;base64,{img_base64}"
    return imgs_of_plts

def summary_in(data: List[Dict[str, pd.DataFrame]], measuring: str, country: str, freq: str = 'h') -> str:
    country = country.lower()
    plt.figure(figsize=(18, 6))
    combined_df = pd.DataFrame()

    for _, d in enumerate(data):
        for sensor_name, df in d.items():
            if df.empty:
                continue
            df = df.copy()
            df['Timestamp'] = pd.to_datetime(df['Timestamp'])
            df.set_index('Timestamp', inplace=True)
            df_resampled = df.resample(freq).mean().reset_index()
            combined_df = pd.concat([combined_df, df_resampled], axis=0, ignore_index=True)

    if combined_df.empty:
        return ""

    sns.lineplot(
        data=combined_df,
        x='Timestamp',
        y=measuring,
        errorbar='sd',
        estimator='mean',
        marker='.',
        markersize=10
    )

    plt.xlabel('Date', fontsize=18)
    plt.ylabel(measuring, fontsize=18)
    plt.title(f'{measuring}', fontsize=20, fontweight='bold')
    plt.grid(True, axis='x', linestyle='--', linewidth=0.5)
    plt.gca().xaxis.set_major_locator(DayLocator())
    date_format = DateFormatter("%d")
    plt.gca().xaxis.set_major_formatter(date_format)

    if measuring in ['PM 2.5', 'CO2'] and combined_df[measuring].max() > combined_df[measuring].mean() * 2:
        if measuring == 'PM 2.5':
            plt.ylim(bottom=1)
        elif measuring == 'CO2':
            plt.ylim(bottom=400)

        def custom_yscale(y, pos):
            return f'{y:.0f}' if y > 1 else f'{y:.2f}'

        plt.yscale('log', base=2)
        plt.gca().yaxis.set_major_formatter(FuncFormatter(custom_yscale))

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

    #use the cleaned data and save the pdf to level 1 folder
    level_folder = get_level_1_folder(country)

    for sensor_type in ['Indoor Sensors', 'Outdoor Sensors']:
        for sensor in os.listdir(f"{BASE_DIR}/Central Asian Data/{country}/{level_folder}/{date_folder_name}/{sensor_type}"):
            match = re.match(r'([A-Za-z0-9-]+)-\w+-\d{4}', sensor)
            if match:
                sensor_name: str = match.group(1)
            else:
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
        'PM 2.5': summary_in(summary_indoor_data_sorted, 'PM 2.5', country),
        'RH': summary_in(summary_indoor_data_sorted, 'Relative Humidity', country),
        'Temperture': summary_in(summary_indoor_data_sorted, 'Temperature', country),
        'CO2': summary_in(summary_indoor_data_sorted, 'CO2', country)
    }
    summary_outdoor : Dict[str, str] = {
        'PM 2.5': summary(summary_outdoor_data_sorted, 'PM 2.5', country),
        'RH': summary(summary_outdoor_data_sorted, 'Relative Humidity', country),
        'Temperture': summary(summary_outdoor_data_sorted, 'Temperature', country)
    }
    
    return data_indoor_sorted, data_outdoor_sorted, summary_indoor, summary_outdoor


def create_summary_pdf() -> None:

    for country in ['KZ', 'KG', 'UZ']:

        level_folder = get_level_1_folder(country)

        print(f"Creating summary pdf for {country} ...")
        try:
            #getting uptime data of sensors
            uptimes : Dict[str, float] = calculate_uptime(country)

            #getting data for sensors
            data_indoor, data_outdoor, summary_indoor, summary_outdoor = get_data(country)
            
            #getting info of sensors of this country
            sensors_info = get_sensors_info(country)
            
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
                <div><img src="{summary_indoor['CO2']}" width="950"></div>
            </div>
            <div class="page-break"></div>
            <div class='intro'>
                <p>Summary of Outdoor Sensors:</p>"""
            for city, plt in summary_outdoor['PM 2.5'].items():
                html_content += f"""
                    <div>
                        <p><b>{city}</b></p>
                        <img src="{plt}" width="950">
                    </div>
                """
            html_content += f"""
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
                    <p class='sensor-location' style='font-size:18px;'><b>{sensors_info[sensor_name].location} ({latitude}, {longitude})</b></p>
                    <p class='sensor-location' style='font-size:18px;'> City: {sensors_info[sensor_name].city} </p>
                    <p class='sensor-location' style='font-size:18px;'> Uptime value: {uptimes.get(sensor_name)}% </p>
                    <p class='sensor-location' style='font-size:18px;'> <b>Updates</b>:
                """
                for upd in sensors_info[sensor_name].updates:
                    html_content += f"<br> {upd}"
                
                html_content +=f"""
                    </p>
                    <p><b>Graphs</b></p>
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
                    <p class='sensor-location' style='font-size:18px;'><b>{sensors_info[sensor_name].location} ({latitude}, {longitude})</b></p>
                    <p class='sensor-location' style='font-size:18px;'> City: {sensors_info[sensor_name].city} </p>
                    <p class='sensor-location' style='font-size:18px;'> Uptime value: {uptimes.get(sensor_name)}% </p>
                    <p class='sensor-location' style='font-size:18px;'> <b>Updates</b>:
                """
                for upd in sensors_info[sensor_name].updates:
                    html_content += f"<br> {upd}"
                
                html_content +=f"""
                    </p>
                    <p><b>Graphs</b></p>
                    <div>{plot_img_tag_pm25}</div>
                    <div>{plot_img_tag_rh}</div>
                    <div>{plot_img_tag_temp}</div>
                    <div>{plot_img_tag_co2}</div>
                </div>
                <div class="page-break"></div>
                """
            html_content += "</body></html>"

            output_pdf_path: str = f"{BASE_DIR}/Central Asian Data/{country}/{level_folder}/{date_folder_name}/{country.lower()}_summary.pdf"
            pdfkit.from_string(html_content, output_pdf_path)
            print(f"Summary pdf created successfully for {country}")
        except Exception as e:
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
            output_pdf_path: str = f"{BASE_DIR}/Central Asian Data/{country}/{level_folder}/{date_folder_name}/{country.lower()}_summary.pdf"
            pdfkit.from_string(html_content, output_pdf_path)
            print(f"Error processing data for {country}: {e}. An empty pdf was created.")
            continue