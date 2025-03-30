from datetime import datetime
from email.mime.text import MIMEText
import logging
import pandas as pd
from pathlib import Path 
import pytz
import re
import smtplib
import traceback
from typing import Sequence
from types import TracebackType

def basic_file_logger(file_path: str | Path, log_level: str = 'INFO') -> logging.Logger:
    '''
    * Logs to a file using the specified logging level.
    * Information categories will be pipe-delimited, for reading into a dataframe.
    * Logger is given name = __name__.
    * If logger with name = __name__ already has handlers, it will be returned as-is.
    * Intended for use-cases in which the main process only has need for a single log file and file handler configuration.

    Args:
        * file_path (str) -- Path to the log file. Any valid string path or Path object is accepted.
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

        file_handler = logging.FileHandler(file_path, errors='backslashreplace')
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

def archive_log(file_path: str | Path, archive_dir_path: str | Path) -> None:
    '''
    Moves log file to specified archive directory and appends a timestamp suffix to the archived file.

    Args:
        file_path (str | Path): Path to the log file. Any valid string path or Path object is accepted.
        archive_dir_path (str | Path): Path to the log archive directory. Any valid string path or Path object is accepted. Will be created if it doesn't exist.
    '''   
    source = Path(file_path) if isinstance(file_path, str) else file_path
    dest = Path(archive_dir_path) if isinstance(archive_dir_path, str) else archive_dir_path

    if not source.exists():
        raise FileNotFoundError(f'{file_path} not found.')
    if not dest.exists():
        dest.mkdir(parents=True)

    date = datetime.now().strftime('%Y_%m_%d')
    source.rename(dest / f'{source.stem}_{date}.log')

def write_log_check_email_body(file_path: str | Path, previous_hours: int,  check_level: str = 'WARNING') -> str | None:
    '''
    Reads a log file and writes a simple text body that is intended to be sent in automated emails.
    Expects the following log format: %(asctime)s|%(levelname)s|%(module)s|%(lineno)d|%(message)s.

    Args:
        file_path (str): Path to the log file. Any valid string path or Path object is accepted.
        previous_hours (int): Number of hours back in time to check logging messages.
        check_level (str, optional): Lowest logging level at which to begin including messages in the email body. Defaults to 'WARNING'.

    Returns:
        * str | None: Logging message updates, each seperated by a double new line.
    '''
    level_dict = {
        'DEBUG': ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'),
        'INFO': ('INFO', 'WARNING', 'ERROR', 'CRITICAL'),
        'WARNING': ('WARNING', 'ERROR', 'CRITICAL'),
        'ERROR': ('ERROR', 'CRITICAL'),
        'CRITICAL': ('CRITICAL',)
    }

    log_df = pd.read_csv(file_path, header=None, delimiter='|')
    log_df = log_df[log_df[1].isin(level_dict[check_level])]

    if len(log_df) < 1:
        return None

    log_df['log_datetime'] = log_df[0].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S,%f'))
    log_df['datetime_now'] = datetime.now()
    log_df['time_delta'] = log_df['datetime_now'] - log_df['log_datetime']
    log_df['time_delta_hours'] = log_df['time_delta'].apply(lambda x: x.total_seconds() / 3600)
    log_df = log_df[log_df['time_delta_hours'] <= previous_hours]

    if len(log_df) < 1:
        return None

    log_df.sort_values('log_datetime', ascending=False, inplace=True)

    body = []
    for row in log_df.itertuples(index=False):
        body.append(f'{row[0]} | {row[1]} | {row[2]} | {row[3]} | {row[4]}')

    return '\n\n'.join(body)

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

def utc_epoch_to_ak_time_str(epoch: int, format_milliseconds: bool = False) -> str:
    '''
    Converts UTC unix timestamp in milliseconds to formatted America/Anchorage time string.
    '''
    seconds = epoch / 1000
    utc_time = datetime.fromtimestamp(seconds, pytz.utc)
    ak_timezone = pytz.timezone('America/Anchorage')
    format = '%Y-%m-%d %H:%M:%S.%f' if format_milliseconds else '%Y-%m-%d %H:%M:%S'
    ak_time = utc_time.astimezone(ak_timezone).strftime(format)
    return ak_time
