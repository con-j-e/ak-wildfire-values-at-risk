from datetime import datetime
from email.mime.text import MIMEText
import logging
import pandas as pd
import pytz
import re
import smtplib
import traceback
from typing import Sequence
from types import TracebackType

def basic_file_logger(file_name: str, log_level: str = 'INFO') -> logging.Logger:
    '''
    * Logs to a file using the specified logging level.
    * Information categories will be pipe-delimited, for reading into a dataframe.
    * Logger is given name = __name__.
    * If logger with name = __name__ already has handlers, it will be returned as-is.

    Args:
        * file_name (str) -- The name of the file.
        * log_level (str, optional) -- The logging level as a string (default is 'INFO').

    Returns:
        * Logger -- The configured logger.
    '''    
    logger = logging.getLogger(__name__)

    if not logger.hasHandlers():
        log_level_dict = {
            'DEBUG': logging.DEBUG,
            'INFO': logging.INFO,
            'WARNING': logging.WARNING,
            'ERROR': logging.ERROR,
            'CRITICAL': logging.CRITICAL
        }
        
        level = log_level_dict.get(log_level.upper(), logging.INFO)

        logger.setLevel(level)

        file_handler = logging.FileHandler(f'{file_name}.log', errors='backslashreplace')
        file_handler.setLevel(level)

        formatter = logging.Formatter(r'%(asctime)s|%(levelname)s|%(module)s|%(lineno)d|%(message)s')
        file_handler.setFormatter(formatter)

        logger.addHandler(file_handler)

    return logger

def format_logged_exception(exc_type: type[BaseException], exc_val: BaseException, exc_tb: TracebackType, max_chars: int = 2000) -> str:
    '''
    * Formats standard exception information made available by context manager __exit__ or __aexit__ calls, or inside of an Except block.
    * Replaces new lines with html line breaks, removes '^' and '~' characters, replaces long whitespace with a single space.
    * Truncates returned string according to max_chars argument.

    Args:
        * exc_type (type[BaseException]) -- Exception type.
        * exc_val (BaseException) -- Exception value.
        * exc_tb (TracebackType) -- Traceback.
        * max_chars (int, optional) -- maximum character count of returned string. Defaults to 2000.

    Returns:
        * str -- Formatted exception information.
    '''
    exc_format = traceback.format_exception(exc_type, exc_val, exc_tb)
    exc_format_str = ''.join(exc_format)
    exc_format_str = exc_format_str.replace('\n', '<br>').replace('^','').replace('~','')
    exc_format_str = re.sub(r'\s+', ' ', exc_format_str)
    exc_short_format_str = exc_format_str if len(exc_format_str) < max_chars else f'{exc_format_str[:max_chars]}...'
    return exc_short_format_str

def send_email(subject: str, body: str, sender: str, recipients: str | Sequence[str], password: str) -> None:
    '''
    This function was written for use with a gmail sender account. With gmail you must enable 2fa to generate an app password for programmatic use.

    Args:
        subject (str): Email subject.
        body (str): Email body.
        sender (str): Email address of sender.
        recipients (str | Sequence[str]): Email address or addresses of recipient or recipients.
        password (str): App password to access sender email account programmatically. 
    '''
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = recipients if isinstance(recipients, str) else ', '.join(recipients)
    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp_server:
        smtp_server.login(sender, password)
        smtp_server.sendmail(sender, recipients, msg.as_string())

def utc_epoch_to_ak_time_str(epoch: int) -> str:
    '''
    Converts UTC unix timestamp in milliseconds to formatted America/Anchorage time string.
    '''
    seconds = epoch / 1000
    utc_time = datetime.fromtimestamp(seconds, pytz.utc)
    ak_timezone = pytz.timezone('America/Anchorage')
    ak_time = utc_time.astimezone(ak_timezone).strftime('%Y-%m-%d %H:%M:%S')
    return f'{ak_time} AK Time'
