"""Stock Movement Notifier

This program sends an email with the stock movement journal data from the 
previous day to the specified recipients every day at a specified time.

This program runs indefinitely until manually stopped.

This program requires the following libraries be installed within the Python environment it is run in:
    * requests
    * schedule
    * tabulate

This program requires that a file named 'constants.py' be available in the same
directory as the program, which contains the following constants:
    * AUTH_HEADERS - The authorization headers for the Abas REST API.
    * PORT_NUM - The SMTP port number.
    * SMTP_SERVER - The SMTP server.
    * SENDER_EMAIL - The email address of the sender.
    * RECIPIENTS - The email addresses of the recipients.
    * PASSWORD - The app password for the sender email.
    * POST_HEADERS - The POST request headers for the Abas REST API.
    * STOCK_URL - The URL for the Stock Movement Journal Infosystem.
    * PO_URL - The URL for the purchase order database.
    * PS_URL - The URL for the packing slip database.
    * PROD_CENTER_URL - The URL for the production center Infosystem.
"""
# Standard library imports
import smtplib
import ssl
import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import sys
import json
import locale
import time
import os

# Third-party library imports
from tabulate import tabulate
import requests
import schedule

# Local application/library specific imports
sys.path.append("C Ryan Zhou\\Abas_Notifs\\PO_Notifs")
import constants


def send_email(processing_data, materials_data) -> None:
    """Send an email with the processing and materials data to the specified recipients.
    
    Args:
        processing_data (list[list[str]]): The processing data to send in the email.
        materials_data (list[list[str]]): The materials data to send in the email.
    """
    print("Sending email...")

    # Getting yesterday's date
    yesterday = (datetime.datetime.now() -
            datetime.timedelta(days=1)).strftime('%m/%d/%y')

    # Turning the processing data into a table and formatting it
    if processing_data == "None": # No data
        text_processing_table="No processing receipts yesterday."
        processing_table="No processing receipts yesterday."
    else:
        text_processing_table = tabulate(processing_data, headers="firstrow",
                        tablefmt="grid", stralign="center", numalign="center")
        processing_table = tabulate(processing_data, headers="firstrow",
                        tablefmt="html", stralign="center", numalign="center")
        processing_table = processing_table.replace(
            "<table>", '<table border="1" cellpadding="5" cellspacing="0" style="border-collapse:collapse;">')

    # Turning the materials data into a table and formatting it
    if materials_data == "None": # No data
        text_materials_table = "No material receipts yesterday."
        materials_table = "No material receipts yesterday."
    else:
        text_materials_table = tabulate(materials_data, headers="firstrow",
                        tablefmt="grid", stralign="center", numalign="center")
        materials_table = tabulate(materials_data, headers="firstrow",
                        tablefmt="html", stralign="center", numalign="center")
        materials_table = materials_table.replace(
            "<table>", '<table border="1" cellpadding="5" cellspacing="0" style="border-collapse:collapse;">')

    # Creating email text
    text = f"""Good morning everyone,
See below for the movements in the stock movement journal yesterday ({yesterday}):
Processing:
{text_processing_table}
Materials:
{text_materials_table}
Thank you for your attention.
This is an automated email managed by Ryan Zhou. To give feedback or unsubscribe, please contact r97zhou@uwaterloo.ca. If any data is incorrect/missing, please contact me.
    """
    # Creating email html
    html = f"""
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
</head>
<body><p>Good morning everyone,</p>
<p>See below for the movements in the stock movement journal yesterday ({yesterday}):</p>
<p>Processing:</p>
{processing_table}
<p>Materials:</p>
{materials_table}
<p>Thank you for your attention.</p>
<small>This is an automated email managed by Ryan Zhou.</small>
<small>To give feedback or unsubscribe, please contact <b>r97zhou@uwaterloo.ca</b>. If any data is incorrect/missing, please contact me.</small>
</body></html>
    """
    # Creating email message
    msg = MIMEMultipart("alternative", None, [
                        MIMEText(text), MIMEText(html, 'html')])
    msg['Subject'] = "Stock Movement Info - %s" % (
        datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%m/%d/%y')
    msg['From'] = "Dishon Notifications"
    msg['To'] = ', '.join(constants.RECIPIENTS)

    # Sending email
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(constants.SMTP_SERVER, constants.PORT_NUM, context=context) as server:
        server.login(constants.SENDER_EMAIL, constants.PASSWORD)
        server.send_message(msg)
        print("Email sent!")
    time.sleep(21600)


def get_packing_slip_info(database_id) -> str:
    """Get the supplier name from the database ID.
    
    Args:
        database_id (str): The database ID of the supplier.
    Returns:
        str: The name of the supplier
    """
    if database_id == "":  # Ignore empty database IDs
        return ("-", "-")
    # Get the supplier name from the supplier database
    response = requests.get(constants.PS_URL + database_id, headers=constants.POST_HEADERS, timeout=None)
    supplier = json.loads(response.text)["content"]["data"]["head"]["fields"]["liefname"]["text"]
    item_text = json.loads(response.text)["content"]["data"]["table"][0]["fields"]["ptext"]["value"]
    if item_text == "":
        item_text = "-"
    return (supplier, item_text)


def merge_materials_data(data) -> list[list[str]]:
    """Merge identical table entries.
    
    Args:
        data (list[list[str]]): The table data to merge.
    Returns:
        list[list[str]]: The merged table data.
    """
    # This code sucks but I don't wanna refactor it
    merged_data = []
    prev_po = data[0][0]
    prev_supplier = data[0][1]
    prev_desc = data[0][2]
    prev_pn = data[0][3]
    prev_item_text = data[0][4]
    prev_loc = data[0][5]
    prev_qty = float(locale.atof(data[0][6]))
    prev_row = 0
    merged = False
    curr_po = "No Value"
    # Iterate through the table and merge identical entries (refactor this part?)
    for i in range(1, len(data) + 1):
        if i < len(data):
            curr_po = data[i][0]
            curr_supplier = data[i][1]
            curr_desc = data[i][2]
            curr_pn = data[i][3]
            curr_item_text = data[i][4]
            curr_loc = data[i][5]
            curr_qty = float(locale.atof(data[i][6]))
        if (i < len(data) and curr_po == prev_po and curr_supplier == prev_supplier
            and curr_desc == prev_desc and curr_pn == prev_pn
            and curr_item_text == prev_item_text and curr_loc == prev_loc):
            prev_qty += curr_qty
            merged = True
        else:
            merged_data.append(data[prev_row])
            if merged:
                merged_data[-1][6] = prev_qty
                merged = False
            if curr_po != "No Value":
                prev_po = curr_po
                prev_supplier = curr_supplier
                prev_desc = curr_desc
                prev_pn = curr_pn
                prev_item_text = curr_item_text
                prev_loc = curr_loc
                prev_qty = curr_qty
                prev_row = i

    return merged_data


def get_materials_data() -> list[list[str]]:
    """Get the stock data from the Stock Movement Journal Infosystem.
    
    Args:
        None
    Returns:
        list[list[str]]: The stock data from the Stock Movement Journal Infosystem.
    """
    print("Getting materials data...")
    # Create the work order post request body
    yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%m/%d/%Y')
    payload = json.dumps({
        "actions": [
            {
                "_type": "SetFieldValue",
                "fieldName": "adatum",
                "value": yesterday
            },
            {
                "_type": "SetFieldValue",
                "fieldName": "edatum",
                "value": yesterday
            },
            {
                "_type": "SetFieldValue",
                "fieldName": "zugang",
                "value": True
            },
            {
                "_type": "SetFieldValue",
                "fieldName": "bstart"
            }
        ],
        "headFields": "-",
        "tableFields": "budat, ysuch, namebspr, nplatz, zmge, amge, yworkorder, ypurchaseorder, ncharge, ypurchasepackslip"
    })
    response = requests.post(constants.STOCK_URL,
                                headers=constants.POST_HEADERS,
                                data=payload, timeout=None)
    # If there is no data for yesterday
    if "table" not in json.loads(response.text)["content"]["data"]:
        return "None"
    table = json.loads(response.text)["content"]["data"]["table"]
    data = []
    # Iterate through the table to get the data
    for row in table:
        receipt_loc = row["fields"]["nplatz"]["text"]
        # Only get processing data
        if ("MATERIAL" in receipt_loc) or ("HARDWARE" in receipt_loc):
            if row["fields"]["ypurchaseorder"]["text"] == "":
                po = "-"
            else:
                # Get supplier by PO
                po = row["fields"]["ypurchaseorder"]["text"].replace("+", "")
            info_tuple = get_packing_slip_info(row["fields"]["ypurchasepackslip"]["value"])
            supplier = info_tuple[0]
            item_text = info_tuple[1]
            description = row["fields"]["namebspr"]["text"]
            material = row["fields"]["ysuch"]["text"]

            if row["fields"]["zmge"]["text"] == "":
                qty = row["fields"]["amge"]["text"]
            else:
                qty = row["fields"]["zmge"]["text"]

            data.append([po, supplier, description, material, item_text, receipt_loc, qty])

    # Sort the table
    data = sorted(data, key=lambda row: [row[5], row[3]])

    # If there is no data
    if not data:
        return "None"

    # Merge table entries
    data = merge_materials_data(data)

    # Give each entry a number
    for i, row in enumerate(data):
        row.insert(0, i + 1)

    # Add table headers
    data.insert(0, [
        "Item",
        "PO",
        "Supplier/Processor",
        "Description",
        "Part/Material Number",
        "Item Text",
        "Receipt Location",
        "QTY"
    ])

    print("Got materials data!")
    return data


def get_supplier(database_id) -> str:
    """Get the supplier name from the database ID.
    
    Args:
        database_id (str): The database ID of the supplier.
    Returns:
        str: The name of the supplier
    """
    if database_id == "":  # Ignore empty database IDs
        return ""
    # Get the supplier name from the supplier database
    response = requests.get(constants.PO_URL + database_id, headers=constants.POST_HEADERS, timeout=None)
    return json.loads(response.text)["content"]["data"]["head"]["fields"]["liefname"]["text"]


def get_next_process(wo_num, process) -> str:
    """Get the next process for a work order number.
    
    Args:
        wo_num (str): The work order number.
    Returns:
        str: The next process for the work order.
    """
    # Create the POST request body with the work order number
    print("Getting response from work order " + wo_num)
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
        "tableFields": "order,art,artbez,frgmge,ycomplete,mrbqty,rverlust,twterm,vorgang"
    })
    # Send the POST request to the production center Infosystem
    response = requests.post(constants.PROD_CENTER_URL, headers=constants.POST_HEADERS,
                             data=payload, timeout=None)
    table = json.loads(response.text)["content"]["data"]["table"]
    for i in range (1, len(table)):
        # Get the next process in the work order
        if table[i]["fields"]["art"]["text"] == process:
            if i == len(table) - 1: # Check if this is the last process
                return "None"
            elif table[i + 1]["fields"]["artbez"]["text"] == "1st Off": # Skip first off
                return table[i + 2]["fields"]["artbez"]["text"]
            else:
                return table[i + 1]["fields"]["artbez"]["text"]
    return "Not Found"


def merge_processing_data(data) -> list[list[str]]:
    """Merge identical table entries.
    
    Args:
        data (list[list[str]]): The table data to merge.
    Returns:
        list[list[str]]: The merged table data.
    """
    merged_data = []
    prev_pn = data[0][3]
    prev_loc = data[0][5]
    prev_qty = float(locale.atof(data[0][6]))
    prev_row = 0
    merged = False
    curr_pn = "No Value"
    # Iterate through the table and merge identical entries
    for i in range(1, len(data) + 1):
        if i < len(data):
            curr_pn = data[i][3]
            curr_loc = data[i][5]
            curr_qty = float(locale.atof(data[i][6]))
        if i < len(data) and curr_pn == prev_pn and curr_loc == prev_loc:
            prev_qty += curr_qty
            merged = True
        else:
            merged_data.append(data[prev_row])
            wo_list = get_wo_nums(data[prev_row][4], data[prev_row][3])
            wo_nums = ', '.join(wo_list)
            merged_data[-1][4] = wo_nums
            merged_data[-1][7] = get_next_process(wo_list[0], merged_data[-1][3 ])
            if merged:
                merged_data[-1][6] = prev_qty
                merged = False
            if curr_pn != "No Value":
                prev_pn = curr_pn
                prev_loc = curr_loc
                prev_qty = curr_qty
                prev_row = i

    return merged_data


def get_wo_nums(database_id, process) -> list[str]:
    """Get the work order numbers for a purchase order by its database ID.
    
    Args:
        database_id (str): The database ID of the purchase order.
    Returns:
        list[str]: The work order numbers for the given purchase order.
    """
    response = requests.get(constants.PO_URL + database_id, headers=constants.POST_HEADERS, timeout=None)
    table = json.loads(response.text)["content"]["data"]["table"]
    wo_list = []
    # Iterate through the table to get the work order numbers
    for row in table:
        if "WO" in row["fields"]["ptext"]["value"] and row["fields"]["artikel"]["text"] == process:
            wo_list.append(row["fields"]["ptext"]["value"].replace("WO ", ""))
    return wo_list


def get_processing_data() -> list[list[str]]:
    """Get the stock data from the Stock Movement Journal Infosystem.
    
    Args:
        None
    Returns:
        list[list[str]]: The stock data from the Stock Movement Journal Infosystem.
    """
    print("Getting processing data...")
    # Create the work order post request body
    yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%m/%d/%Y')
    payload = json.dumps({
        "actions": [
            {
                "_type": "SetFieldValue",
                "fieldName": "adatum",
                "value": yesterday
            },
            {
                "_type": "SetFieldValue",
                "fieldName": "edatum",
                "value": yesterday
            },
            {
                "_type": "SetFieldValue",
                "fieldName": "zugang",
                "value": True
            },
            {
                "_type": "SetFieldValue",
                "fieldName": "bstart"
            }
        ],
        "headFields": "-",
        "tableFields": "budat, ysuch, namebspr, nplatz, zmge, amge, yworkorder, ypurchaseorder, ncharge"
    })
    response = requests.post(constants.STOCK_URL,
                                headers=constants.POST_HEADERS,
                                data=payload, timeout=None)
    # If there is no data for yesterday
    if "table" not in json.loads(response.text)["content"]["data"]:
        return "None"
    table = json.loads(response.text)["content"]["data"]["table"]
    data = []

    # Iterate through the table to get the data
    for row in table:
        receipt_loc = row["fields"]["nplatz"]["text"]
        # Only get processing data
        if "PROCESSING" in receipt_loc:
            if row["fields"]["ypurchaseorder"]["text"] == "":
                po = "-"
                supplier = "-"
            else:
                # Get supplier by PO
                po = row["fields"]["ypurchaseorder"]["text"].replace("+", "")
                supplier = get_supplier(row["fields"]["ypurchaseorder"]["value"])
            description = row["fields"]["namebspr"]["text"]
            process = row["fields"]["ysuch"]["text"]

            if row["fields"]["zmge"]["text"] == "":
                qty = row["fields"]["amge"]["text"]
            else:
                qty = row["fields"]["zmge"]["text"]

            data.append([
                po,
                supplier,
                description,
                process,
                row["fields"]["ypurchaseorder"]["value"],
                receipt_loc,
                qty,
                "-"
            ])

    # No data
    if not data:
        return "None"

    # Sort the table
    data = sorted(data, key=lambda row: [row[4], row[3], row[5]])

    # Merge table entries
    data = merge_processing_data(data)

    # Give each entry a number
    for i, row in enumerate(data):
        row.insert(0, i + 1)

    # Add table headers
    data.insert(0, [
            "Item",
            "PO",
            "Supplier/Processor",
            "Description", "Process",
            "Work Order(s)",
            "Receipt Location",
            "QTY",
            "Next Process"
        ])

    print("Got processing data!")
    return data


def run_schedule() -> None:
    """Get the data and send the email."""
    print("Running...")
    send_email(get_processing_data(), get_materials_data())


def main():
    """The main function to run the program loop."""
    schedule.every().day.at("07:00").do(run_schedule)
    while True:
        schedule.run_pending()
        os.system('cls')
        print("Waiting, time is " + datetime.datetime.now().strftime('%m-%d-%y %H:%M:%S'))
        time.sleep(30)


if __name__ == "__main__":
    # Set the locale to the system default to handle commas in numbers
    locale.setlocale(locale.LC_ALL, '')
    # Printing file documentation for help command
    if len(sys.argv) == 2 and sys.argv[1] == '--help':
        print(__doc__)
        exit()
    main()
