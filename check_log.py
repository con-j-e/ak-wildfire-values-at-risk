import ast
import pathlib

from utils.general import write_log_check_email_body, send_email, archive_log

def main():

    secrets_dir = pathlib.Path().cwd() / 'secrets'

    with open(secrets_dir / 'send_email_params.txt', 'r') as file:
        email_params_str = file.read()
        email_params_tup = ast.literal_eval(email_params_str)
        sender, recipient, password = email_params_tup  

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