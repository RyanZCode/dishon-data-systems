"""Yield Notifier

This program automatically sends an email to the specified recipients with a 
table containing the data from the yield data CSV file.

This program runs indefinitely until manually stopped.

This program uses a CSV file for input and email for output.

This program requires that the 'tabulate' library be installed within the 
Python environment it is run in.

This program requires that a file name 'constants.py' be available in the same
directory as the program, which contains the following constants:
    * DATA_PATH - The absolute file path of the yield data CSV file.
    * PORT_NUM - The SMTP port number.
    * SMTP_SERVER - The SMTP server.
    * SENDER_EMAIL - The email address of the sender.
    * RECIPIENTS - The email addresses of the recipients.
    * PASSWORD - The app password for the sender email.
    * POST_HEADERS - The POST request headers for the Abas REST API.
"""
# Standard library imports
import smtplib
import ssl
import csv
import time
import datetime
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import sys

# Third-party library imports
from tabulate import tabulate

# Local application/library specific imports
sys.path.append("C Ryan Zhou\\Abas_Notifs\\Yield_Notifs")
import constants


def send_email() -> None:
    """Send an email with the yield data from the CSV file to the specified recipients."""
    print("Sending email...")
    # Get yesterday's date
    date = (datetime.datetime.now() -
            datetime.timedelta(days=1)).strftime('%m/%d/%y')

    # Create email text
    text = f"""Good morning everyone,
See below for the processes that were fully yielded yesterday ({date}):
{{table}}
Thank you for your attention.
This is an automated email managed by Ryan Zhou. To give feedback or unsubscribe, please contact r97zhou@uwaterloo.ca. If any data is incorrect/missing, please contact me.
    """

    # Create email html
    html = f"""
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
</head>
<body><p>Good morning everyone,</p>
<p>See below for the processes that were <b>fully yielded</b> yesterday ({date}):</p>
{{table}}
<p>Thank you for your attention.</p>
<small>This is an automated email managed by Ryan Zhou.</small>
<small>To give feedback or unsubscribe, please contact <b>r97zhou@uwaterloo.ca</b>. If any data is incorrect/missing, please contact me.</small>
</body></html>
    """

    # Read data from the CSV file and sort it
    data = []
    with open(constants.DATA_PATH, encoding="utf-8") as input_file:
        reader = csv.reader(input_file)
        data.append(next(reader))
        data[1:] = sorted(reader, key=lambda row: (row[0], row[1]))
    # Turn the CSV file's data into a table and format it
    if len(data) == 1: # No processes fully yielded
        text = text.format(table="No processes fully yielded yesterday.")
        html = html.format(table="No processes fully yielded yesterday.")
    else:
        text = text.format(table=tabulate(data, headers="firstrow",
                        tablefmt="grid", stralign="center", numalign="center"))
        html = html.format(table=tabulate(data, headers="firstrow",
                        tablefmt="html", stralign="center", numalign="center"))
        html = html.replace(
            "<table>",
            '<table border="1" cellpadding="5" cellspacing="0" style="border-collapse:collapse;">')
    # Create the email message
    msg = MIMEMultipart("alternative", None, [
                        MIMEText(text), MIMEText(html, 'html')])
    msg['Subject'] = "Yield Info - %s" % (
        datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%m/%d/%y')
    msg['From'] = "Dishon Notifications"
    msg['To'] = ', '.join(constants.RECIPIENTS)

    # Send the email
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(constants.SMTP_SERVER, constants.PORT_NUM, context=context) as server:
        server.login(constants.SENDER_EMAIL, constants.PASSWORD)
        server.send_message(msg)
        print("Email sent!")
    time.sleep(21600)


def main():
    """Run the main function loop of the program"""
    run_once = False
    # Loop indefinitely to check the time and run the main function
    while True:
        if datetime.datetime.now().hour == 6: # Reset the run_once flag at 6 AM
            run_once = True
        if datetime.datetime.now().hour >= 7 and run_once: # Run the main function after 7 AM
            # Get file times
            curr_date = datetime.datetime.now().strftime('%m/%d/%y')
            file_date = datetime.datetime.fromtimestamp(
                os.path.getmtime(constants.DATA_PATH)).strftime('%m/%d/%y')
            if file_date == curr_date:  # Check if the file was modified today
                # Run main function
                send_email()
                run_once = False
        # Wait for 30 seconds before checking the time again
        os.system('cls')
        print("Waiting, time is " + datetime.datetime.now().strftime('%m-%d-%y %H:%M:%S'))
        time.sleep(30)


if __name__ == "__main__":
    # Printing file documentation for help command
    if len(sys.argv) == 2 and sys.argv[1] == '--help':
        print(__doc__)
        exit()
    
    main()
