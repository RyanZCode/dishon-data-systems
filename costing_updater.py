"""Costing Data Updater

This program updates the costing data for all work orders in the production center Infosystem.

This program runs indefinitely until manually stopped.

This program uses the Abas REST API to get data and outputs data to a CSV file.

This program requires the following libraries be installed within the Python 
environment it is run in:
    * requests
    * schedule
    * aiohttp
    * pandas

This program requires that a file named 'constants.py' be available in the same
directory as the program, which contains the following constants:
    * AUTH_HEADER - The authorization header for the Abas REST API
    * CONNECTION_LIMIT - The maximum number of connections to allow at once
    * COSTING_PATH - The file path to output the costing data to
    * COSTING_URL - The URL for the costing database
    * ERROR_LOG_PATH - The file path to output error logs to
    * FX_URL - The URL for the exchange rate database
    * OPEN_PN_PATH - The file path to output the open part numbers to
    * PN_URL - The URL for the part number database
    * POST_HEADERS - The POST request headers for the Abas REST API
    * PRODUCT_DB_URL - The URL for the product database
    * PROD_CENTER_URL - The production center Infosystem URL
    * PROD_CENTER_PAYLOAD - The production center Infosystem POST request body
    * SALES_DB_URL - The URL for the sales database
    * SALES_ACTIVITIES_PAYLOAD - The sales activities POST request body
    * SALES_ACTIVITIES_URL - The sales activities Infosystem URL
    * UPDATE_TIME - The time of day (24 hour format) to update the costing data
    * WO_PAYLOAD - The work order post request body
    * WO_DB_URL - The URL for the work order database
"""
# Standard library imports
import sys
import json
import datetime
import time
import asyncio
import os

# Third-party library imports
import requests
import schedule
import aiohttp
import pandas as pd

# Local application/library specific imports
sys.path.append("C Ryan Zhou\\Costing_Report")
import constants


def get_prod_list() -> list[dict]:
    """Get the list of work orders from the production center Infosystem.
    
    Returns:
        list[dict]: The list of work orders.
    """
    response = requests.post(constants.PROD_CENTER_URL,
                             headers=constants.POST_HEADERS,
                             data=constants.PROD_CENTER_PAYLOAD,
                             timeout=None)
    prod_list = json.loads(response.text)["content"]["data"]["table"]
    return prod_list


def get_wo_data(wo_id) -> tuple[str, str, str, str, str]:
    """Get the work order data from the work order database.

    Args:
        wo_id (str): The ID of the work order.
    Returns:
        tuple[str, str, str, str, str]: The work order data.
    """
    wo_response = requests.get(constants.WO_DB_URL + wo_id, headers=constants.AUTH_HEADER, timeout=None)
    wo_data = json.loads(wo_response.text)["content"]["data"]["head"]["fields"]
    part_num = wo_data["artikel"]["text"]
    pn_id = wo_data["artikel"]["value"]
    timestamp = wo_data["erfass"]["text"]

    pn_response = requests.get(constants.PN_URL + pn_id, headers=constants.AUTH_HEADER, timeout=None)
    pn_data = json.loads(pn_response.text)["content"]["data"]["head"]["fields"]
    customer = pn_data["ycustomername"]["text"]
    sales_price = pn_data["vpr"]["value"]
    cust_part_num = pn_data["ypartnumber"]["value"]

    return (part_num, timestamp, sales_price, customer, cust_part_num)


async def get_costing_data(row, session, usd_to_cad) -> list[str, str, str, str]:
    """Get the costing data for a work order.

    Args:
        row (dict): The work order data.
        session (aiohttp.ClientSession): The aiohttp session object.
        usd_to_cad (float): The exchange rate from USD to CAD.
    Returns:
        list[str, str, str, str]: The costing data.
    """
    # Get the work order number
    if row["fields"]["yclosedwonumber"]["value"] == "":
        wo_num = row["fields"]["order"]["text"]
    else:
        wo_num = row["fields"]["yclosedwonumber"]["value"]

    # Ignore empty work order numbers
    if wo_num == "":
        return "None"

    wo_id = row["fields"]["order"]["value"]

    # Loop until the costing data is successfully retrieved
    while True:
        try:
            async with session.get(constants.COSTING_URL
                                   + "?criteria=typ=Final costing;text=" + wo_num,
                                   headers=constants.AUTH_HEADER) as response:
                print("Getting costing data for WO " + wo_num)
                start_time = time.time()
                costing_meta = await response.json()
                costing_metadata = costing_meta["content"]["data"]["erpDataObjects"]

                # No costing data found
                if len(costing_metadata) == 0:
                    return "None"

                database_id = costing_metadata[0]["meta"]["id"]
                costing_response = requests.get(constants.COSTING_URL + "/" + database_id,
                                                headers=constants.AUTH_HEADER,
                                                timeout=None)
                costing_data = json.loads(costing_response.text)["content"]["data"]["head"]["fields"]
                mat_cost = costing_data["matek"]["value"]
                ext_cost = costing_data["fremdek"]["value"]
                var_cost = costing_data["varfek"]["value"]
                count = costing_data["basis"]["value"]
                filed = False
                if wo_id != "": # Open work order
                    wo_tuple = get_wo_data(wo_id)
                    part_num = wo_tuple[0]
                    timestamp = wo_tuple[1]
                    sales_price = wo_tuple[2] * usd_to_cad
                    customer = wo_tuple[3]
                    cust_part_num = wo_tuple[4]
                else: # Filed work order
                    filed = True
                    part_num = row["fields"]["art"]["text"]
                    timestamp = costing_data["stand"]["text"]
                    pn_id = row["fields"]["art"]["value"]
                    pn_response = requests.get(constants.PN_URL + pn_id,
                                                headers=constants.AUTH_HEADER,
                                                timeout=None)
                    pn_data = json.loads(pn_response.text)["content"]["data"]["head"]["fields"]
                    sales_price = pn_data["vpr"]["value"] * usd_to_cad
                    cust_part_num = pn_data["ypartnumber"]["value"]
                    customer = pn_data["ycustomername"]["text"]
                print("Got costing data for WO " + wo_num + " in %s seconds" % (time.time() - start_time))
                return [
                    wo_num,
                    part_num,
                    cust_part_num,
                    customer,
                    sales_price,
                    mat_cost,
                    ext_cost,
                    var_cost,
                    count,
                    timestamp,
                    filed
                ]
        except Exception as e:
            # Log the error to the error log file
            with open(constants.ERROR_LOG_PATH, "a", encoding="utf-8") as log:
                log.write(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                          + " - Error getting costing data for WO " + wo_num + ": " + str(e) + "\n")
            print("Error getting costing data for WO " + wo_num + ": " + str(e))


async def get_all_costing_data(prod_list) -> list[list[str, str, str, str]]:
    """Get the costing data for all work orders in the production center Infosystem.
    
    Args:
        prod_list (list[dict]): The list of work orders.
    Returns:
        list[list[str, str, str, str]]: The costing data for all work orders.
    """
    # Create the aiohttp.ClientSession object with specified connection limit
    # and no timeouts
    connector = aiohttp.TCPConnector(limit=constants.CONNECTION_LIMIT)
    timeout = aiohttp.ClientTimeout(total=0)
    usd_to_cad = get_usd_to_cad()
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = []
        # Creating a list of tasks to get the production center Infosystem response
        for row in prod_list:
            task = asyncio.ensure_future(get_costing_data(row, session, usd_to_cad))
            tasks.append(task)
        print("Total work orders: " + str(len(prod_list)))
        # Run all the tasks asynchronously and return the results
        return await asyncio.gather(*tasks, return_exceptions=True)


def get_open_set() -> None:
    """Get the open set of part numbers from the sales activities Infosystem."""
    start_time = time.time()
    print("Getting open set...")
    response = requests.post(constants.SALES_ACTIVITIES_URL,
                             headers=constants.POST_HEADERS,
                             data=constants.SALES_ACTIVITIES_PAYLOAD,
                             timeout=None)
    table = json.loads(response.text)["content"]["data"]["table"]
    open_set = set()

    # Iterate through the sales activities table and get the part numbers
    for row in table:
        sales_response = requests.get(constants.SALES_DB_URL
                                      + row["fields"]["ttrans"]["value"],
                                      headers=constants.AUTH_HEADER,
                                      timeout=None)
        sales_table = json.loads(sales_response.text)["content"]["data"]["table"]
        for sales_row in sales_table:
            open_set.add(sales_row["fields"]["artikel"]["text"])

    df = pd.DataFrame(columns=["Part Number"])
    # Add the part numbers to the DataFrame
    for part in open_set:
        # Remove the "+" from the beginning of part numbers
        if part[0] == "+":
            part = part[1:]
        df.loc[len(df)] = part
    df.to_csv(constants.OPEN_PN_PATH, index=False)
    print("Got open set in %s seconds" % (time.time() - start_time))


def get_usd_to_cad() -> float:
    """Get the exchange rate from USD to CAD.
    
    Returns:
        float: The exchange rate from USD to CAD.
    """
    print("Getting USD to CAD exchange rate...")
    response = requests.get(constants.FX_URL, headers=constants.AUTH_HEADER, timeout=None)
    fx_table = json.loads(response.text)["content"]["data"]["table"]
    # Iterate through the exchange rate table and find the USD to CAD exchange rate
    for row in fx_table:
        if row["fields"]["land"]["text"] == "USD":
            print("Got USD to CAD exchange rate: " + str(row["fields"]["kkurs"]["value"]))
            return row["fields"]["kkurs"]["value"]
    raise RuntimeError("USD to CAD exchange rate not found")


def update_costing_data() -> None:
    """Update the costing data for all work orders in the production center Infosystem."""
    print("Running program...")

    # Clear the error log file
    open(constants.ERROR_LOG_PATH, 'w', encoding="utf-8").close()

    # Get the open set of part numbers
    get_open_set()

    print("Getting prod list...")
    start_time = time.time()
    prod_list = get_prod_list()
    print("Got prod list in %s seconds" % (time.time() - start_time))

    print("Getting costing data...")
    start_time = time.time()
    costing_df = pd.DataFrame(columns=[
        "WO Number",
        "Part Number",
        "Truncated Part Number",
        "Customer",
        "Sales Price (CAD)",
        "Material Cost (CAD)",
        "External Cost (CAD)",
        "Variable Production Cost (CAD)",
        "# of Parts",
        "Timestamp",
        "Filed"
    ])
    i = 0
    costing_list = asyncio.run(get_all_costing_data(prod_list))
    # Iterate through the costing data and add it to the DataFrame
    for costing_data in costing_list:
        if costing_data != "None":
            costing_df.loc[i] = costing_data
            i += 1
    costing_df.to_csv(constants.COSTING_PATH, index=False)
    print("Got all costing data in %s seconds" % (time.time() - start_time))
    print("Costing data updated succesfully!")
    time.sleep(172800)


def main():
    """The main function to run the program loop."""
    schedule.every().sunday.at(constants.UPDATE_TIME).do(update_costing_data)
    while True:
        schedule.run_pending()
        os.system('cls')
        print("Waiting, time is " + datetime.datetime.now().strftime('%m-%d-%y %H:%M:%S'))
        # Wait for 5 minutes
        time.sleep(300)


if __name__ == '__main__':
    # Printing file documentation for help command
    if len(sys.argv) == 2 and sys.argv[1] == '--help':
        print(__doc__)
        exit()
    main()
