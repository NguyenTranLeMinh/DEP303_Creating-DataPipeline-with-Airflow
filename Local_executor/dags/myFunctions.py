import os
from google_drive_downloader import GoogleDriveDownloader as gdd

FILES_PATH = "/home/airflow/airflow/downloads/"


def _branching():
    if os.path.exists(FILES_PATH + "Questions.csv") and os.path.exists(FILES_PATH + "Answers.csv"):
        return 'end'
    return 'processing.clear_file'


def _dowload_answer_file_task():
    gdd.download_file_from_google_drive(file_id='1FflYh-YxXDvJ6NyE9GNhnMbB-M87az0Y',
                                    dest_path='/home/airflow/airflow/downloads/Answers.csv',
                                    showsize=True)   


def _dowload_question_file_task():
    gdd.download_file_from_google_drive(file_id='1pzhWKoKV3nHqmC7FLcr5SzF7qIChsy9B',
                                    dest_path='/home/airflow/airflow/downloads/Questions.csv',
                                    showsize=True)  

