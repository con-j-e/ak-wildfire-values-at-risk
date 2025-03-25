import os
import pathlib

from utils.general import write_log_check_email_body, send_email, archive_log

def main():

    sender, recipient, password = os.getenv('SEND_EMAIL_PARAMS').split(',')

    body = write_log_check_email_body(file_path='main_info.log', previous_hours=1, check_level='WARNING')
    if body:
        send_email('LOGGING NOTIFICATION: ak-wildfire-values-at-risk', body, sender, recipient, password)

    with open('main_info.log', 'r') as file:
        lines = sum(1 for _ in file)
    if lines > 10_000:
        archive_log('main_info.log', 'logs')

    return

if __name__ == "__main__":
    main()