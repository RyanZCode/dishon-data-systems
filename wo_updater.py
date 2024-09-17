"""Work Order Updater

This program automatically updates a file that stores work order data for the 
Quality Queue and MRB Tracker, and a file that stores yield data for the 
Yield Notifications.

This program runs indefinitely until manually stopped.

This program uses the Abas REST API for input and comma separated value 
(.csv) files for output.

This program requires that the following libraries be installed
within the Python environment it is run in:
    * aiohttp
    * pandas
    * requests

This program requires that a file name 'constants.py' be available in the same
directory as the program, which contains the following constants:
    * WO_URL - The open work orders Infosystem URL.
    * PROD_CENTER_URL - The production center Infosystem URL.
    * CC_URL - The completion confirmations Infosystem URL.
    * WO_DB_URL - The work order database URL.
    * WO_PAYLOAD - The work order post request body.
    * POST_HEADERS - The POST request headers for the Abas REST API.
    * AUTH_HEADER - The authorization header for the Abas API.
    * CONNECTION_LIMIT - The maximum number of concurrent connections to the
                         production center.
    * RESULT_PATH - The absolute file path of the where the quality data .csv
                    file should be placed/currently is located.
    * YIELD_PATH - The absolute file path of the where the yield data .csv file 
                   should be/currently is located.
    * STARTING_STRINGS - The strings that the product should start with to be 
                         considered.
    * RUN_DELAY - The time (in seconds) to wait before running the script again.
"""
# Standard library imports
import asyncio
import datetime
import json
import locale
import os
import sys
import time
import warnings

# Third-party library imports
import aiohttp
import pandas as pd
import requests

# Local application/library specific imports
sys.path.append("C Ryan Zhou\\Work_Order_Tracking")
import constants

# Prevent pandas warnings from displaying
warnings.simplefilter(action='ignore', category=FutureWarning)


async def get_wo_response(session, wo_num) -> list[dict]:
    """Get a response asynchronously from the production center Infosystem.
    
    Args:
        session (ClientSession): The aiohttp.ClientSession object to use 
          for the request.
        wo_num (int): The work order number to get a response for.
    Returns:
        The table data from the production center Infosystem response.
    """
    # Create the POST request body with the work order number
    payload = json.dumps({
        "actions": [
            {
                "_type": "SetFieldValue",
                "fieldName": "yclosedwo",
                "value": wo_num
            },
            {
                "_type": "SetFieldValue",
                "fieldName": "klgruppe",
                "value": "INTWHG"
            },
            {
                "_type": "SetFieldValue",
                "fieldName": "bstart"
            }
        ],
        "headFields": "-",
        "tableFields": "order,art,artbez,frgmge,ycomplete,mrbqty,rverlust,twterm,vorgang,netmge"
    })
    # Send the POST request to the production center Infosystem
    async with session.post(constants.PROD_CENTER_URL,
                            headers=constants.POST_HEADERS, data=payload) as resp:
        wo_response = await resp.json()
        print("Got response from work order " + wo_response["content"]["data"]
              ["table"][0]["fields"]["order"]["text"])
        return wo_response["content"]["data"]["table"]


async def get_all_prod_tables(wo_list) -> list[list[dict]]:
    """Get the production center Infosystem response for all work orders.
    
    Args:
        wo_list (list[str]): A list of work order numbers to get responses for.
    Returns:
        A list of tables from the production center Infosystem responses.
    """
    # Create the aiohttp.ClientSession object with specified connection limit
    # and no timeouts
    connector = aiohttp.TCPConnector(limit=constants.CONNECTION_LIMIT)
    timeout = aiohttp.ClientTimeout(total=0)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = []
        # Creating a list of tasks to get the production center Infosystem response
        for wo_num in wo_list:
            task = asyncio.ensure_future(get_wo_response(session, wo_num))
            tasks.append(task)
        print("Total open work orders: " + str(len(wo_list)))
        # Run all the tasks asynchronously and return the results
        return await asyncio.gather(*tasks, return_exceptions=True)


def get_wo_list() -> list[str]:
    """Get a list of open work orders from the open work orders Infosystem."""
    # Get POST request response from the open work orders Infosystem
    wo_response = requests.post(constants.WO_URL,
                                headers=constants.POST_HEADERS,
                                data=constants.WO_PAYLOAD,
                                timeout=None)
    wo_table = json.loads(wo_response.text)["content"]["data"]["table"]
    # Create a list of work order numbers from the response
    wo_list = list()
    for i in range(len(wo_table)):
        wo_list.append(wo_table[i]["fields"]["banummer"]["text"])
    return wo_list


def get_cc_date(database_id, work_slip) -> str:
    """Get the completion confirmation date for a work slip."""
    # Create the POST request body with the database ID
    payload = json.dumps({
        "actions": [
            {
                "_type": "SetFieldValue",
                "fieldName": "nows",
                "value": database_id
            },
            {
                "_type": "SetFieldValue",
                "fieldName": "bstart"
            }
        ],
        "headFields": "-",
        "tableFields": "twonum,tdate"
    })
    # Send the POST request to the completion confirmations Infosystem
    cc_response = requests.post(
        constants.CC_URL, headers=constants.POST_HEADERS, data=payload, timeout=None)
    cc_table = json.loads(cc_response.text)["content"]["data"]["table"]
    # Iterate through the completion confirmations table to find the latest date
    found = False
    for i in range(len(cc_table)):
        if cc_table[i]["fields"]["twonum"]["value"] == work_slip:
            date = cc_table[i]["fields"]["tdate"]["value"]
            found = True
        elif found:
            break
    if found:
        return date
    else:
        return "No Completion Confirmation"


def check_mrb_qty(database_id) -> tuple[bool, str]:
    """Check if there is any MRB quantity in the completion confirmation.
    
    Args:
        database_id (str): The database ID of the completion confirmation.
    Returns:
        A tuple containing a boolean indicating if there is MRB quantity and
        the date of the MRB completion confirmation.
    """
    # Get the completion confirmation data from the database
    cc_response = requests.get(
        constants.WO_DB_URL + "/" + database_id, headers=constants.AUTH_HEADER, timeout=None)
    cc_data = json.loads(cc_response.text)["content"]["data"]
    mrb_qty = cc_data["table"][0]["fields"]["ymrbqty"]["value"]
    if mrb_qty > 0:
        return (True, cc_data["head"]["fields"]["stand"]["text"])
    else:
        return (False, "N/A")


def get_mrb_date(work_slip) -> str:
    """Get the date of the MRB completion confirmation for a work slip.
    
    Args:
        work_slip (str): The work slip number to get the MRB date for.
    Returns:
        The date of the most recent MRB completion confirmation.
    """
    # Get the completion confirmations list for the work slip
    cc_response = requests.get(constants.WO_DB_URL
                               + "?criteria=@filingmode=(Filed);nummer=" + work_slip
                               + "&limit=1000",
                               headers=constants.AUTH_HEADER,
                               timeout=None)
    cc_metalist = json.loads(cc_response.text)["content"]["data"]["erpDataObjects"]
    # Iterate through the completion confirmations list to find the MRB date
    for i in range(len(cc_metalist) - 1, -1, -1):
        database_id = cc_metalist[i]["head"]["fields"]["id"]["text"]
        mrb_tuple = check_mrb_qty(database_id)
        if mrb_tuple[0]:
            return mrb_tuple[1]
    return "No Completion Confirmation"


def in_quality(prod_table) -> tuple[int, float, str, str, bool, float, str]:
    """Check if a work order is in quality and return its data.
    
    Args:
        prod_table (list[dict]): The production center Infosystem table data.
    Returns:
        A tuple containing an integer indicating the quality status, the 
        quantity of the work order in quality, the date of the work order in 
        quality, the progress of the work order in quality, a boolean 
        indicating if there is MRB quantity, the quantity of MRB quantity, and 
        the date of the MRB completion confirmation.
    """
    in_qual = False
    qty = "N/A"
    date = "N/A"
    progress = "N/A"
    tentative = False
    qual_found = False
    mrb = False
    mrb_qty = 0
    mrb_date = "N/A"
    # Iterate through the table to find inspections and final inspection
    for i in range(1, len(prod_table)):
        # Get the description of the current item
        description = prod_table[i]["fields"]["artbez"]["text"]
        # If there is any MRB quantity, set the mrb flag and get the
        # MRB quantity and date of its completion confirmation
        if float(locale.atof(prod_table[i]["fields"]["mrbqty"]["text"])) > 0:
            mrb = True
            mrb_qty += float(locale.atof(prod_table[i]
                             ["fields"]["mrbqty"]["text"]))
            database_id = prod_table[0]["fields"]["vorgang"]["value"]
            try:
                mrb_date = get_mrb_date(prod_table[i]["fields"]["order"]["text"])
            except Exception as e:
                print("Error caught:", str(e))
                mrb_date = "No Date"
            break

        # If the description is inspection or final inspection and the work
        # order is not yet in quality, determine if it is in quality
        if (description == "Inspection" or description == "Final Inspection") and not qual_found:
            # Calculate the sum of the yield, MRB quantity, and scrap quantity
            curr_yield = float(locale.atof(
                prod_table[i]["fields"]["ycomplete"]["text"]))
            curr_mrb = float(locale.atof(
                prod_table[i]["fields"]["mrbqty"]["text"]))
            curr_scrap = float(locale.atof(
                prod_table[i]["fields"]["rverlust"]["text"]))
            curr_sum = curr_yield + curr_mrb + curr_scrap

            # Find the previous yield value by iterating backwards
            j = i - 1
            while j >= 1:
                this_yield = float(locale.atof(
                    prod_table[j]["fields"]["ycomplete"]["text"]))
                product = prod_table[j]["fields"]["art"]["text"]
                prod_description = prod_table[j]["fields"]["artbez"]["text"]

                # Do not consider first off yields, CMM yields, or assembly yields
                if (prod_description != "1st Off" and "CMM" not in prod_description
                        and product.startswith(constants.STARTING_STRINGS)):
                    if this_yield <= curr_sum: # Not in quality
                        break
                    if this_yield > curr_sum: # In quality
                        qty = this_yield - curr_sum
                        progress = str(i) + "/" + str(len(prod_table) - 1)
                        in_qual = True
                        qual_found = True
                        database_id = prod_table[0]["fields"]["vorgang"]["value"]
                        # Get the date of the completion confirmation, if any
                        date = get_cc_date(
                            database_id, prod_table[i]["fields"]["order"]["text"])
                j -= 1

            if in_qual or curr_yield == 0:
                # Iterate through the rest of the table to find discrepancies
                j = i + 1
                while j < len(prod_table):
                    this_yield = float(locale.atof(
                        prod_table[j]["fields"]["ycomplete"]["text"]))
                    product = prod_table[j]["fields"]["art"]["text"]
                    # Do not consider first off yields, CMM yields, or assembly yields
                    if (prod_table[j]["fields"]["artbez"]["text"] != "1st Off"
                        and "CMM" not in prod_description
                            and product.startswith(constants.STARTING_STRINGS)):
                        if this_yield > curr_yield:
                            tentative = True
                            break
                    j += 1

    if tentative and in_qual:
        qual_status = 1
    elif in_qual:
        qual_status = 2
    else:
        qual_status = 0
    return (qual_status, qty, date, progress, mrb, mrb_qty, mrb_date)


def get_yield_data(database_id) -> tuple[float, str, str]:
    """Get data from the yield database based on the database id.
    
    Args:
        database_id (str): The database ID of the yield data.
    Returns:
        A tuple containing the total quantity, the latest date, and the timestamp.
    """
    # Get the yield data
    cc_response = requests.get(
        constants.WO_DB_URL + "/" + database_id, headers=constants.AUTH_HEADER, timeout=None)
    cc_data = json.loads(cc_response.text)["content"]["data"]
    total_qty = (float(locale.atof(cc_data["table"][0]["fields"]["bumge"]["text"]))
                   + float(locale.atof(cc_data["table"]
                           [0]["fields"]["ymrbqty"]["text"]))
                   + float(locale.atof(cc_data["table"][0]["fields"]["verlust"]["text"])))
    latest_date = cc_data["head"]["fields"]["abldat"]["text"]
    timestamp = cc_data["head"]["fields"]["stand"]["text"][-8:-3]
    return (total_qty, latest_date, timestamp)


def get_yielded_date(work_slip, sum_qty) -> tuple[str, str]:
    """Get the latest date and time of a work slip's completion confirmation.
    
    Args:
        work_slip (str): The work slip number to get the completion date for.
        sum_qty (float): The sum of the yield, MRB quantity, and scrap quantity.
    Returns:
        A tuple containing the latest date and time of the completion confirmation.
    """
    if work_slip == "":  # If the work slip is empty, return an error
        return "ERROR"
    # Get the completion confirmations list for the work slip
    cc_response = requests.get(constants.WO_DB_URL + "?criteria=@filingmode=(Filed);nummer=" +
                               work_slip + "&limit=1000", headers=constants.AUTH_HEADER, timeout=None)
    cc_metalist = json.loads(cc_response.text)[
        "content"]["data"]["erpDataObjects"]
    total_qty = 0
    latest_date = ""
    timestamp = ""
    # Iterate through the completion confirmations list backwards
    # to find the latest date
    for i in range(len(cc_metalist) - 1, -1, -1):
        database_id = cc_metalist[i]["head"]["fields"]["id"]["text"]
        yield_tuple = get_yield_data(database_id)
        total_qty += yield_tuple[0]
        if latest_date == "" and yield_tuple[0] != 0: # Save the latest date with any quantity
            latest_date = yield_tuple[1]
            timestamp = yield_tuple[2]
        if total_qty == sum_qty: # If the total quantity matches the sum, return the date
            return (latest_date, timestamp)
    return ("ERROR", "N/A")


def get_yield(prod_table, date) -> tuple[bool, str, str, str, str, str]:
    """Get all necessary yield data for a work order.
    
    Args:
        prod_table (list[dict]): The production center Infosystem table data.
        date (str): The date to get the yield data for.
    Returns:
        A tuple containing a boolean indicating if there is yield data, the 
        work slip number, the process, the quantity, the next process, the 
        progress, and the completion time.
    """
    # Iterate through the table backwards to find the yield data
    for i in range(len(prod_table) - 1, 0, -1):
        # If the qty to be released is less than or equal to 0, get the yield data
        if float(locale.atof(prod_table[i]["fields"]["frgmge"]["text"])) <= 0:
            work_slip = prod_table[i]["fields"]["order"]["text"]
            row_sum = (float(locale.atof(prod_table[i]["fields"]["ycomplete"]["text"]))
                       + float(locale.atof(prod_table[i]["fields"]["mrbqty"]["text"]))
                       + float(locale.atof(prod_table[i]["fields"]["rverlust"]["text"])))
            # Get the yield's latest date and time for the work slip
            yield_tuple = get_yielded_date(work_slip, row_sum)
            if yield_tuple[0] == date:
                process = prod_table[i]["fields"]["artbez"]["text"]
                qty = prod_table[i]["fields"]["ycomplete"]["text"]
                # Get the next process in the work order
                if i == len(prod_table) - 1:  # Check if this is the last process
                    next_op = "None"
                elif prod_table[i + 1]["fields"]["artbez"]["text"] == "1st Off":  # Skip first off
                    next_op = prod_table[i + 2]["fields"]["artbez"]["text"]
                else:
                    next_op = prod_table[i + 1]["fields"]["artbez"]["text"]
                timestamp = yield_tuple[1]
                return (True, work_slip, process, qty, next_op, timestamp)
            else:
                return (False, "N/A", "N/A", "N/A", "N/A",  "N/A")
    return (False, "N/A", "N/A", "N/A", "N/A", "N/A")


def get_customer(database_id) -> str:
    """Get the customer name from the product database.
    
    Returns:
        str: The customer name.
    """
    customer_response = requests.get(constants.PRODUCT_DB_URL + "/" + database_id,
                                     headers=constants.AUTH_HEADER,
                                     timeout=None)
    return json.loads(customer_response.text)["content"]["data"]["head"]["fields"]["ycustomername"]["text"]


def get_wip_data(prod_table) -> tuple[str, str, str, str, str, str, str, str, str, str, str]:
    """Get the WIP data for a work order.
    
    Args:
        prod_table (list[dict]): The production center Infosystem table data.
    Returns:
        A tuple containing the work order number, the part number, the 
        description, the customer, the quantity to be released, the yield, the 
        MRB quantity, the scrap quantity, the due quantity, and the process.
    """
    wo_num = prod_table[0]["fields"]["order"]["text"]
    part_num = prod_table[0]["fields"]["art"]["text"]
    description = prod_table[0]["fields"]["artbez"]["text"]
    customer = get_customer(prod_table[0]["fields"]["art"]["value"])

    process = prod_table[0]["fields"]["artbez"]["text"]
    qty_tbr = prod_table[0]["fields"]["frgmge"]["text"]
    yield_qty = prod_table[0]["fields"]["ycomplete"]["text"]
    mrb = prod_table[0]["fields"]["mrbqty"]["text"]
    scrap = prod_table[0]["fields"]["rverlust"]["text"]

    due = prod_table[0]["fields"]["netmge"]["text"]

    return (wo_num, part_num, description, customer, qty_tbr, yield_qty, mrb, scrap, due, process)


def update_data() -> None:
    """Update the quality data and yield data files if necessary."""
    print("Starting update...")
    print("Getting open work orders...")
    start_time = time.time()
    # Get the list of all open work orders
    wo_list = get_wo_list()
    print("Got wo_list in %s seconds!" % (time.time() - start_time))

    print("Getting prod tables...")
    start_time = time.time()
    # Get the production center Infosystem response for all work orders
    prod_tables = asyncio.run(get_all_prod_tables(wo_list))
    print("Got prod tables in %s seconds!" % (time.time() - start_time))

    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    print("Analyzing prod tables...")
    start_time = time.time()

    quality_status_df = pd.DataFrame(columns=[
        'wo_num', 'in_quality', 'part_num', 'description',
        'qty', 'date', 'progress', 'mrb', 'mrb_qty', 'mrb_date', 'timestamp'
    ])
    # Iterate through the production tables to get all quality data
    for i, table in enumerate(prod_tables):
        quality_tuple = in_quality(table)
        wo_num = table[0]["fields"]["order"]["text"]
        status = quality_tuple[0]
        part_num = table[0]["fields"]["art"]["text"]
        description = table[0]["fields"]["artbez"]["text"]
        qty = quality_tuple[1]
        date = quality_tuple[2]
        progress = quality_tuple[3]
        mrb = quality_tuple[4]
        mrb_qty = quality_tuple[5]
        mrb_date = quality_tuple[6]
        quality_status_df.loc[i] = [
            wo_num,
            status,
            part_num,
            description,
            qty,
            date,
            progress,
            mrb,
            mrb_qty,
            mrb_date,
            timestamp
        ]
    # Save the quality data to a .csv file
    quality_status_df.to_csv(constants.RESULT_PATH, index=False)

    print("Prod table analysis completed in %s seconds!" %
          (time.time() - start_time))

    # Update yield data
    print("Getting yield data...")
    start_time = time.time()
    yield_df = pd.DataFrame(columns=[
        'Completed Process',
        'Work Slip',
        'Description',
        'P/N',
        'QTY Yield',
        'Next Process',
        'Completion Time'
    ])
    start_date = (datetime.datetime.now() -
                    datetime.timedelta(days=1)).strftime("%m/%d/%y")
    curr_row = 0
    # Iterate through the production tables to get all yield data
    for table in prod_tables:
        yield_tuple = get_yield(table, start_date)
        if yield_tuple[0]:
            part_num = table[0]["fields"]["art"]["text"]
            work_slip = yield_tuple[1]
            description = table[0]["fields"]["artbez"]["text"]
            process = yield_tuple[2]
            qty = yield_tuple[3]
            next_op = yield_tuple[4]
            completion_time = yield_tuple[5]
            yield_df.loc[curr_row] = [
                process,
                work_slip,
                description,
                part_num,
                qty,
                next_op,
                completion_time
            ]
            curr_row += 1
    # Save the yield data to a .csv file
    yield_df.to_csv(constants.YIELD_PATH, index=False)
    print("Yield data update completed in %s seconds!" %
            (time.time() - start_time))


    try:
        # Update WIP data
        print("Updating WIP data...")
        start_time = time.time()

        wip_df = pd.DataFrame(columns=[
            'wo_num',
            'part_num',
            'description',
            'customer',
            'qty_tbr',
            'yield',
            'mrb',
            'scrap',
            'due',
            'process',
            'timestamp'
        ])

        curr_row = 0
        for table in prod_tables:
            wip_tuple = get_wip_data(table)
            wo_num = wip_tuple[0]
            part_num = wip_tuple[1]
            description = wip_tuple[2]
            customer = wip_tuple[3]
            qty_tbr = wip_tuple[4]
            yield_qty = wip_tuple[5]
            mrb = wip_tuple[6]
            scrap = wip_tuple[7]
            due = wip_tuple[8]
            process = wip_tuple[9]
            wip_df.loc[curr_row] = [
                wo_num,
                part_num,
                description,
                customer,
                qty_tbr,
                yield_qty,
                mrb,
                scrap,
                due,
                process,
                timestamp
            ]
            curr_row += 1

        wip_df.to_csv(constants.WIP_PATH, index=False)
        print("WIP data update completed in %s seconds!" %
                    (time.time() - start_time))
    except Exception as e:
        print("Error updating WIP data, error: " + str(e))

    print("Update completed!")
    time.sleep(21600)


def main():
    """Run the main loop of the program."""
    error = False
    # Run the main loop indefinitely
    while True:
        curr_time = datetime.datetime.now()
        # If the current time is between 4-5 AM or if
        # there was an error updating, re-update the data
        if ((curr_time.hour >= 4 and curr_time.hour < 5) or error):
            try:
                update_data()
                error = False
            except Exception as e:
                print("Error caught: ", str(e))
                error = True
                continue
        os.system('cls')
        print("Waiting, time is " + datetime.datetime.now().strftime('%m-%d-%y %H:%M:%S'))
        time.sleep(constants.RUN_DELAY)


if __name__ == "__main__":
    # Set the locale to the system default to handle commas in numbers
    locale.setlocale(locale.LC_ALL, '')
    # Printing file documentation for help command
    if len(sys.argv) == 2 and sys.argv[1] == '--help':
        print(__doc__)
        exit()

    main()
