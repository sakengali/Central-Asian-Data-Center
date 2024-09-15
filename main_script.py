from download_data_from_tsi import main_download
from upload_data_to_drive import main_upload
from send_confirmation_email import send_email_main
from helpers import create_info_file
from create_summaries import create_pdf
import pandas as pd

# today's day and month
month = pd.Timestamp.today().month
today = pd.Timestamp.today().day

def download_data():
    """ downloads and saves the data for the three countries"""
    
    countries = ['kz', 'kg', 'uz']

    for country in countries:
        try:
            main_download(country.upper())
        except KeyError as e:
            print(f"Couldn't download data for {country}. Error: {e}")

def upload_data():
    """ uploads data to drive """

    main_upload()

def main():
    if month == 1: # february
        if today == 15 or today == 28:
            try:
                download_data()
                create_info_file()
                create_pdf()
                upload_data()
                send_email_main()
            except Exception as error:
                send_email_main(is_successful=False, error=error)
    else:           # all other months
        if today == 15 or today == 30:
            try:
                download_data()
                create_info_file()
                create_pdf()
                upload_data()
                send_email_main()
            except Exception as error:
                send_email_main(is_successful=False, error=error)

if __name__ == "__main__":
    main()