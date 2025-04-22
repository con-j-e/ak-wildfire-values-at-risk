import os

from utils.general import write_log_check_email_body, send_email, archive_log

def main():

    sender, recipient, password = os.getenv('SEND_EMAIL_PARAMS').split(',')

    # checking main_info.log
    body = write_log_check_email_body(file_path='main_info.log', previous_hours=1, check_level='WARNING')
    if body:
        send_email('LOGGING NOTIFICATION: ak-wildfire-values-at-risk, main.py', body, sender, recipient, password)

    with open('main_info.log', 'r') as file:
        lines = sum(1 for _ in file)
    if lines > 10_000:
        archive_log('main_info.log', 'logs')

    # checking write_tabulator_rows_info.log
    body = write_log_check_email_body(file_path='write_tabulator_rows_info.log', previous_hours=1, check_level='WARNING')
    if body:
        send_email('LOGGING NOTIFICATION: ak-wildfire-values-at-risk, write_tabulator_rows.py', body, sender, recipient, password)

    with open('write_tabulator_rows_info.log', 'r') as file:
        lines = sum(1 for _ in file)
    if lines > 10_000:
        archive_log('write_tabulator_rows_info.log', 'logs')

    # checking service_maintenance_info.log
    body = write_log_check_email_body(file_path='service_maintenance_info.log', previous_hours=1, check_level='WARNING')
    if body:
        send_email('LOGGING NOTIFICATION: ak-wildfire-values-at-risk, service_maintenance.py', body, sender, recipient, password)

    with open('service_maintenance_info.log', 'r') as file:
        lines = sum(1 for _ in file)
    if lines > 10_000:
        archive_log('service_maintenance_info.log', 'logs')

    return

if __name__ == "__main__":
    main()