"""Machine Data Updater

This program automatically updates a file that stores machine status data,
based on the most recent power measurements and limits data.

This program runs indefinitely until manually stopped.

This program uses CSV files for input and output.

This program requires that the 'pandas' library be installed within the Python 
environment it is run in.

This program requires that a file named 'constants.py' be available in the same
directory as the program, which contains the following constants:
    * MEASUREMENTS_PATH - The absolute file path of the folder containing
                          the most recent power measurement data CSV files.
    * NEXT_MEASUREMENTS_PATH - The absolute file path of the folder that will
                               contain the next month's power measurement data
                               CSV files.
    * LIMITS_PATH - The absolute file path of the CSV file containing the
                    each machine's power limits, id, and machine category.
    * STATUS_PATH - The absolute file path of where the new machine status data
                    CSV file should be placed, including the file name.
    * DEVICE_NAME_COL - The column number (zero-indexed) of the device name 
                        in the measurements file.
    * DEVICE_POWER_COL - The column number (zero-indexed) of the current (A)
                         in the measurements file.
    * WORK_CENTER_COL - The column number (zero-indexed) of the work center 
                        (device name) in the limits file.
    * POWER_LIMIT_COL - The column number (zero-indexed) of the power limit
                        in the limits file.
    * MACHINE_CATEGORY_COL - The column number (zero-indexed) of the machine
                             category in the limits file.
    * RUN_DELAY - The time (in seconds) to wait before running the script again.
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
import dateutil

# Local application/library specific imports
sys.path.append("C Ryan Zhou/Machine_Monitoring")
import constants

# Prevent pandas warnings from displaying
warnings.simplefilter(action='ignore', category=FutureWarning)


def get_device_name(limits_df, key) -> str:
    """Return the device name attributed to the key from the limits file.
    
    Args:
        limits_df (DataFrame): The DataFrame containing the limits data.
        key (int): The key of the machine.
    Returns:
        str: The device name of the machine attributed to the key.
    """
    return limits_df.iloc[key - 1][constants.WORK_CENTER_COL]


def get_key(device_name) -> str:
    """Return the key in the device name from the measurements file,
    or '-1' if the device name does not contain a key in square brackets.

    Args:
        device_name (str): The device name to extract the key from.
    Returns:
        str: The key in the device name.
    """
    # No key
    if ('[' not in device_name) or (']' not in device_name):
        return "-1"

    # Find key by looking for square brackets
    i = 1
    key = ""
    while device_name[-i] != '[':
        if device_name[-i].isnumeric():
            key += device_name[-i]
        i += 1

    # Return key by reversing it
    return key[::-1]


def get_limit(limits_df, key) -> str:
    """Return the power limit attributed to the key from the limits file.
    
    Args:
        limits_df (DataFrame): The DataFrame containing the limits data.
        key (int): The key of the machine.
    Returns:
        str: The power limit of the machine attributed to the key.
    """
    return limits_df.iloc[key - 1][constants.POWER_LIMIT_COL]


def get_machine_category(limits_df, key) -> str:
    """Return the machine category attributed to the key from the limits file.

    Args:
        limits_df (DataFrame): The DataFrame containing the limits data.
        key (int): The key of the machine.
    Returns:
        str: The machine category of the machine attributed to the key.
    """
    return limits_df.iloc[key - 1][constants.MACHINE_CATEGORY_COL]


def get_machine_current_state(limits_df, measurements_df, key) -> int:
    """Return the current state of the machine attributed to the key.

    Args:
        limits_df (DataFrame): The DataFrame containing the limits data.
        measurements_df (DataFrame): The DataFrame containing the measurements data.
        key (int): The key of the machine.
    Returns:
        int: The current state of the machine (0 - off, 1 - idle, 2 - on)
    """
    found = False
    current_row = 0
    limit = float(get_limit(limits_df, key))
    # Loop through rows of measurements DataFrame and find most recent entry
    for i in range(current_row, measurements_df.shape[0]):
        if int(get_key(measurements_df.iloc[i][constants.DEVICE_NAME_COL])) == int(key):
            current_row = i
            found = True
        elif found:
            break

    # Determine status
    if found:
        current = float(measurements_df
                        .iloc[current_row][constants.DEVICE_POWER_COL]) / 1000
        if current >= limit:
            return 2
        else:
            return 1
    else:
        return 0


def init_uptimes(i, files, curr_file_time, limits_df, machine_uptime_dict) -> None:
    """Update the machine_uptime_dict to represent the last 24 hours' uptime.

    Args:
        i (int): The negative index of the current file (set to 1 for most recent file)
        files (list[file]): A list of all CSV files in the measurements folder.
        curr_file_time (int): The time of the current file.
        limits_df (DataFrame): The DataFrame containing the limits data.
        machine_uptime_dict (dict): The dictionary containing the uptimes of each machine.
    """
    # Looping through files less than 24 hours before current time
    while curr_file_time - os.path.getmtime(files[-i]) < 86099:
        # Change the measurement directory to accomodate for month changes
        if i >= len(files):
            i = 1
            files = list(Path(constants.MEASUREMENTS_PATH).glob('*.csv'))
            files.sort(key=lambda f: f.stat().st_mtime)
        current_df = pd.read_csv(files[-i], engine="pyarrow")
        # Looping through keys to update the uptime for each machine
        for k in range(1, limits_df.shape[0] + 1):
            (machine_uptime_dict
             .update({k: machine_uptime_dict[k]
                      + get_df_uptime(k, current_df, limits_df)}))
        i += 1
        # Print progress every 20 files
        if i % 20 == 0:
            print(str(round((curr_file_time - os.path.getmtime(files[-i]))
                            / 861, 1)) + "% complete")
    print("Last file time: "
          + str(datetime.datetime.fromtimestamp(os.path.getmtime(files[-i]))))


def get_df_uptime(key, current_df, limits_df) -> int:
    """Return the uptime of the machine attributed to the key in current_df.

    Args:
        key (int): The key of the machine.
        current_df (DataFrame): The DataFrame to search in.
        limits_df (DataFrame): The DataFrame containing the limits data.

    Returns:
        int: The total uptime of the machine in the DataFrame.
    """
    # Filter DataFrame to only have current key entries
    filtered_df = current_df[current_df[' device_name']
                        .str.contains("[" + str(key) + "]", regex=False)]

    limit = float(get_limit(limits_df, key))
    uptime = 0
    # Looping through entries to sum uptime
    for i in range(0, filtered_df.shape[0]):
        current = float(filtered_df.iloc[i][constants.DEVICE_POWER_COL]) / 1000
        if current >= limit:
            uptime += 1
    return uptime


def update_date(has_date) -> None:
    """Update dates in constants.py to current month if necessary.

    Args:
        has_date (bool): A boolean indicating if the date has been set.    
    """
    today = datetime.datetime.today()
    curr_date = today.strftime('%Y-%m')

    # Set date when program is run for the first time
    if not has_date:
        if int(today.strftime('%d')) >= 3:
            constants.MEASUREMENTS_PATH = constants.MEASUREMENTS_PATH + curr_date
            next_month = (today + dateutil.relativedelta.relativedelta(months=1)).strftime('%Y-%m')
            constants.NEXT_MEASUREMENTS_PATH = constants.NEXT_MEASUREMENTS_PATH + next_month
        else:
            prev_date = (today - dateutil.relativedelta.relativedelta(months=1)).strftime('%Y-%m')
            constants.MEASUREMENTS_PATH = constants.MEASUREMENTS_PATH + prev_date
            constants.NEXT_MEASUREMENTS_PATH = constants.NEXT_MEASUREMENTS_PATH + curr_date

    # Check if month changed
    if curr_date == constants.MEASUREMENTS_PATH[-7:]:
        return
    elif int(today.strftime('%d')) >= 3:
        # Increment by one month
        constants.MEASUREMENTS_PATH = constants.MEASUREMENTS_PATH[:-7] + curr_date
        next_month = (today + dateutil.relativedelta.relativedelta(months=1)).strftime('%Y-%m')
        constants.NEXT_MEASUREMENTS_PATH = constants.NEXT_MEASUREMENTS_PATH[:-7] + next_month
    elif int(today.strftime('%m')) - int(constants.MEASUREMENTS_PATH[-2:]) > 1:
        prev_date = (today - dateutil.relativedelta.relativedelta(months=1)).strftime('%Y-%m')
        constants.MEASUREMENTS_PATH = constants.MEASUREMENTS_PATH[:-7] + prev_date
        constants.NEXT_MEASUREMENTS_PATH = constants.NEXT_MEASUREMENTS_PATH[:-7] + curr_date


def update_data(limits_df, measurements_df, machine_uptime_dict, curr_file_time) -> None:
    """Save a CSV file with statuses and output its contents in the console.
    
    Args:
        limits_df (DataFrame): The DataFrame containing the limits data.
        measurements_df (DataFrame): The DataFrame containing the measurements data.
        machine_uptime_dict (dict): The dictionary containing the uptimes of each
                                    machine.
        curr_file_time (int): The time of the current file.
    """
    machine_status_df = pd.DataFrame(columns=[
        'device_name',
        'power_status',
        'machine_category',
        'uptime_percent',
        'timestamp'
    ])
    # Loop through limits DataFrame to determine statuses
    for key in range(1, limits_df.shape[0] + 1):
        if machine_uptime_dict[key] > 1440:
            print("Rounded to 100% for key: " + str(key))
            machine_uptime_dict.update({key: 1440})
        elif machine_uptime_dict[key] < 0:
            print("Rounded to 0% for key: " + str(key))
            machine_uptime_dict.update({key: 0})
        machine_uptime_dict.update(
            {key: round(machine_uptime_dict[key] / 14.4)})
        machine_status_df.loc[len(machine_status_df)] = [
            get_device_name(limits_df, key),
            get_machine_current_state(limits_df, measurements_df, key),
            get_machine_category(limits_df, key),
            machine_uptime_dict[key],
            str(datetime.datetime.fromtimestamp(curr_file_time))[:-3]
        ]

    # Convert DataFrame to CSV file
    machine_status_df.to_csv(constants.STATUS_PATH, encoding='utf-8', index=False)
    print(machine_status_df)


def main():
    """Main function that runs the program indefinitely."""
    print("Start time: " + str(datetime.datetime.now()))
    update_date(False)
    print("Loading CSV files...")
    files = []
    # Looping indefinitely to check for new files
    while True:
        os.system('cls')
        print("Awaiting changes, time is " + datetime.datetime.now().strftime('%m-%d-%y %H:%M:%S'))
        try:
            # Update dates in constants
            update_date(True)
            # Set measurements path to current or next month
            curr_measurements_path = constants.MEASUREMENTS_PATH
            if len(list(Path(constants.NEXT_MEASUREMENTS_PATH).glob('*.csv'))) != 0:
                curr_measurements_path = constants.NEXT_MEASUREMENTS_PATH

            if len(list(Path(curr_measurements_path).glob('*.csv'))) != len(files):
                print("Change detected, running...")
                start_time = time.time()
                # Find the path of most recent measurement file
                files = list(Path(curr_measurements_path).glob('*.csv'))
                files.sort(key=lambda f: f.stat().st_mtime)

                # Read the CSV files into DataFrames
                measurements_df = pd.read_csv(files[-1], engine="pyarrow")
                curr_file_time = os.path.getmtime(files[-1])
                print("File time: " + str(datetime.datetime.fromtimestamp(curr_file_time)))
                print("Measurements CSV file loaded succesfully! (" + files[-1].name + ")")

                limits_df = pd.read_csv(constants.LIMITS_PATH, engine="pyarrow")
                print("Limits CSV file loaded succesfully!")

                print("Compressing...")

                # Initializing uptime dictionary
                machine_uptime_dict = {}
                for key in range(1, limits_df.shape[0] + 1):
                    machine_uptime_dict.update({key: 0})

                print("Getting uptimes...")
                # Initialize uptimes for all machines
                init_uptimes(1, files, curr_file_time, limits_df, machine_uptime_dict)
                print("Uptimes gotten succesfully!")

                # Update the machine status data
                update_data(limits_df, measurements_df, machine_uptime_dict, curr_file_time)

                print("File time: "
                    + str(datetime.datetime.fromtimestamp(curr_file_time)))
                print("Completed in %s seconds" % (time.time() - start_time))
                time.sleep(10)
        except Exception as e:
            print("Caught error, still running loop")
            print("Error time: " + str(datetime.datetime.now()))
            print("Error is:\n" + str(e))

        # Wait before looping again
        time.sleep(constants.RUN_DELAY)


if __name__ == "__main__":
    # Printing file documentation for help command
    if len(sys.argv) == 2 and sys.argv[1] == '--help':
        print(__doc__)
        exit()
    main()
