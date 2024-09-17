"""Energy Data Updater

This program appends the energy usage data for the current day to the energy usage file.

This program runs indefinitely until manually stopped.

This program requires the following libraries be installed within the Python environment it is run in:
    * pandas
    * schedule

This program requires that a file named 'constants.py' be available in the same
directory as the program, which contains the following constants:
    * CITATION_40_MULT - Multiplier for the 40 Citation data to match up with previous energy data
    * ENERGY_USAGE_PATH - The absolute file path of the energy usage data .csv file
    * MEASUREMENTS_PATH - The absolute file path of the folder containing the most recent
                          power measurement data .csv files, program will detect automatically
    * STAFFERN_37_MULT - Multiplier for the 37 Staffern data to match up with previous energy data
"""
# Standard library imports
from pathlib import Path
import time
import datetime
import os
import sys
import warnings

# Third-party library imports
import pandas as pd
import schedule

# Local application/library specific imports
sys.path.append("C Ryan Zhou/Energy_Tracking")
import constants

# Prevent pandas warnings from displaying
warnings.simplefilter(action='ignore', category=FutureWarning)


def update_date() -> None:
    """Update dates in constants.py to current month."""
    today = datetime.datetime.today()
    curr_date = today.strftime('%Y-%m')
    constants.MEASUREMENTS_PATH = constants.MEASUREMENTS_PATH[:-7] + curr_date


def update_energy_data() -> None:
    """Update the energy data for the current day."""
    print("Updating energy data, start time is " + datetime.datetime.now().strftime('%m-%d-%y %H:%M:%S'))
    start_time = time.time()
    update_date()
    # Get the most recent power measurement data files
    files = list(Path(constants.MEASUREMENTS_PATH).glob('*.csv'))
    files.sort(key=lambda f: f.stat().st_mtime)

    # Iterate through the files and sum the energy usage for the previous day
    prev_day = datetime.datetime.today().date() - datetime.timedelta(days=1)
    total_37_staffern = 0
    total_40_citation = 0
    index = 1
    while datetime.datetime.fromtimestamp(os.path.getmtime(files[-index])).date() > prev_day:
        index += 1
    while datetime.datetime.fromtimestamp(os.path.getmtime(files[-index])).date() == prev_day:
        print("Checking file with time: " + str(datetime.datetime.fromtimestamp(os.path.getmtime(files[-index]))))
        measurements_df = pd.read_csv(files[-index])
        total_37_staffern += measurements_df[measurements_df[' site_name'] == 'Plant 2 - 37 Staffern Dr.'][' energy(Wh)'].sum()
        total_40_citation += measurements_df[measurements_df[' site_name'] == 'Plant 1 - 40 Citation Dr.'][' energy(Wh)'].sum()
        index += 1

    # Convert the energy usage to kWh, multiply by the constants, and round to 2 decimal places
    total_37_staffern = round(total_37_staffern * constants.STAFFERN_37_MULT / 1000, 2)
    total_40_citation = round(total_40_citation * constants.CITATION_40_MULT / 1000, 2)
    print("37 Total: " + str(total_37_staffern) + " kWh")
    print("40 Total: " + str(total_40_citation) + " kWh")

    # Append the data to the energy usage file
    with open(constants.ENERGY_USAGE_PATH, 'a', encoding='utf-8') as file:
        file.write(prev_day.strftime('%#m/%#d/%Y') + ",37 Staffern," + str(total_37_staffern) + '\n')
        file.write(prev_day.strftime('%#m/%#d/%Y') + ",40 Citation," + str(total_40_citation) + '\n')

    print("Update completed in " + str(time.time() - start_time) + " seconds!")
    time.sleep(43200)


def main():
    """Main function that runs the program indefinitely."""
    print("Starting program...")
    schedule.every().day.at("00:01").do(update_energy_data)
    while True:
        schedule.run_pending()
        os.system('cls')
        print("Waiting, time is " + datetime.datetime.now().strftime('%m-%d-%y %H:%M:%S'))
        time.sleep(30)


if __name__ == "__main__":
    # Printing file documentation for help command
    if len(sys.argv) == 2 and sys.argv[1] == '--help':
        print(__doc__)
        exit()
    main()
