from datetime import datetime
import pandas
import random
import smtplib
import os

# ─── Configuration ────────────────────────────────────────────────────────────
# Your Gmail address and App Password (NOT your regular Gmail password).
# To generate an App Password: Google Account → Security → App Passwords.
my_email = os.getenv('MY_EMAIL')
password = os.getenv('MY_EMAIL_PSWRD')

# ─── Get today's date ─────────────────────────────────────────────────────────
# datetime.now() gives the current date and time.
# We extract just (month, day) as a tuple so we can match it against birthdays.
# Example: if today is April 5, today_tuple = (4, 5)
today = datetime.now()
today_tuple = (today.month, today.day)

# ─── Load birthday data ───────────────────────────────────────────────────────
# pandas reads the CSV file into a DataFrame (like a spreadsheet in Python).
# Expected columns: name, email, month, day
# Example row: John, john@gmail.com, 4, 5
data = pandas.read_csv("birthdays.csv")

# ─── Build a lookup dictionary ────────────────────────────────────────────────
# We convert the DataFrame into a dictionary where:
#   key   = (month, day) tuple  →  e.g. (4, 5)
#   value = the full row for that person (name, email, month, day)
# This makes it easy to check "is there a birthday today?" in O(1) time.
birthday_dict = {
    (data_row.month, data_row.day): data_row
    for (index, data_row) in data.iterrows()
}


# ─── Helper: send an email ────────────────────────────────────────────────────
# Extracted into a function so we don't repeat the SMTP connection logic twice.
# Parameters:
#   to_addr  – recipient email address
#   subject  – email subject line
#   body     – plain-text email body
def send_email(to_addr, subject, body):
    try:
        # Connect to Gmail's SMTP server on port 587 (TLS port)
        connection = smtplib.SMTP("smtp.gmail.com", 587)
        # starttls() upgrades the connection to encrypted TLS
        connection.starttls()
    except TimeoutError:
        print("Failed to connect to SMTP server")
        return  # exit the function early if connection failed

    print("Connected to SMTP server")
    # Log in with your Gmail credentials
    connection.login(user=my_email, password=password)
    # sendmail() sends the email.
    # The message string must start with headers (e.g. "Subject:...")
    # followed by a blank line, then the body.
    connection.sendmail(
        from_addr=my_email,
        to_addrs=to_addr,
        msg=f"Subject:{subject}\n\n{body}"
    )
    # Always close the connection when done
    connection.quit()
    print(f"Email sent to {to_addr}")


# ─── Check for a birthday today ───────────────────────────────────────────────
# The `in` operator checks if today_tuple exists as a key in birthday_dict.
if today_tuple in birthday_dict:

    # Retrieve the row for today's birthday person
    birthday_person = birthday_dict[today_tuple]

    # ── Pick a random letter template ─────────────────────────────────────────
    # There are 3 letter templates: letter_1.txt, letter_2.txt, letter_3.txt
    # random.randint(1, 3) picks a random number between 1 and 3 (inclusive)
    with open(f"letter_{random.randint(1, 3)}.txt") as letter_file:
        contents = letter_file.read()
        # Replace the placeholder [NAME] in the template with the actual name
        # Example: "Dear [NAME]," → "Dear John,"
        contents = contents.replace("[NAME]", birthday_person["name"])

    # ── Send the birthday email ────────────────────────────────────────────────
    send_email(
        to_addr=birthday_person["email"],
        subject=f"Happy Birthday {birthday_person['name']}",
        body=contents
    )

else:
    # ── No birthday today — notify yourself ───────────────────────────────────
    # Instead of just printing to the console, we email ourselves so we know
    # the script ran successfully even on days with no birthdays.
    print("No birthday today — sending notification to sender")

    send_email(
        to_addr=my_email,                        # send to yourself
        subject="Birthday Bot No birthdays today",
        body=(
            f"Hi,\n\n"
            f"The birthday bot ran on {today.strftime('%d %B %Y')} "
            f"and found no birthdays today.\n\n"
            f"Everything is working fine!"
        )
    )
