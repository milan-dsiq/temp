from flask import Blueprint, request, jsonify
import dotenv
import os
from utils.apis.helper import FileCollation, Helper
from utils.schema.models import TaskLogger, ScrapedErrorLog, ScrapedSignalsLog
import pandas as pd
import re
from io import StringIO
from datetime import datetime
dotenv.load_dotenv()

startetl = Blueprint("startetl", __name__)

'''
Steps For ETL:
1. get the files from the storage and store them in temp and then start loading them
2. create a data frame of pandas 
3. clean the data if necessary
4. store the data in the database
5. return the response
'''

def get_source(task_logger_id):
    try:
        helper = Helper(TaskLogger)
        filter_criteria = {'id': task_logger_id}
        args = ['system_info', 'task_manager']
        data = helper.fetchDataFromDbDynamic(*args, **filter_criteria)
        return {
            'system_info': data[0].system_info,
            'destination_info': data[0].task_manager.destination
        }
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def chunk_data(data, chunk_size=1000):
    try:
        for i in range(0, len(data), chunk_size):
            yield data[i:i + chunk_size]  # Yield chunks of 'chunk_size'
    except Exception as e:
        yield {"error": str(e)} 

@startetl.route('/start_etl', methods=['POST'])
def run_etl():
  try:
    data = request.get_json()
    task_logger_id = data.get("task_logger_id")
    
    local_storage_path =os.path.join(os.path.dirname(__file__), '../../temp')
    file_and_system_info = get_source(task_logger_id)
    source_info = {}
    source_info['system_user'] = file_and_system_info['destination_info']['destination_user']
    source_info['system_password'] = os.getenv('DESTINATION_PASSWORD')
    source_info['system_ip'] = file_and_system_info['destination_info']['destination_ip']
    folder_name = file_and_system_info['system_info']['errors_file'].split('_')[-3]
    source_info['errors_file'] = file_and_system_info['destination_info']['destination_path']+'/'+folder_name+'/'+file_and_system_info['system_info']['errors_file'].split('/')[-1]
    source_info['signals_file'] = file_and_system_info['destination_info']['destination_path']+'/'+folder_name+'/'+file_and_system_info['system_info']['signals_file'].split('/')[-1]
    source_info['output_file'] = file_and_system_info['destination_info']['destination_path']+'/'+folder_name+'/'+file_and_system_info['system_info']['output_file'].split('/')[-1]
    file_collator = FileCollation(source_info=source_info, destination_info="", local_storage_path=local_storage_path)# using the same helper function to get the file now from the storage with reverse arguments
    local_path = file_collator.transfer_from_container1() # this function transfer from the storage to the local temp folder so that we can load the data

    # Correct the error files for loading the data
    with open(local_path['local_path']['errors_file'], 'r', encoding='utf-8') as file:
        errors_data = file.read().strip()

    # Remove empty JSON objects "{}"
    errors_data = re.sub(r'\{\s*\}', '', errors_data)

    errors_data = re.sub(r'}\s*{', '},\n{', errors_data)  # Fix missing commas between objects
    if not errors_data.startswith('['):  # Wrap in a list
        errors_data = f'[{errors_data}]'

    error_df = pd.read_json(StringIO(errors_data))
    error_df['task_logger_id'] = task_logger_id
    error_df['timestamp'] = pd.to_datetime(error_df['timestamp'])
    error_df.rename(columns={'timestamp': 'error_timestamp'}, inplace=True)
    list_chunked_error_df = list(chunk_data(error_df))
    helper = Helper(ScrapedErrorLog)
    for df in list_chunked_error_df:
      helper.load_data(df)

    #now signals
    with open(local_path['local_path']['signals_file'], "r") as file:
      log_lines = file.readlines()

    log_pattern = re.compile(
    r"(?P<timestamp>[\d-]+\s[\d:]+) - INFO - (?P<message>.+?)\s*(?:\((?P<response_code>\d+)\))?\s*(?:from|for)?\s*(?P<url>https?://\S+)"
    )
  # Parse log file
    log_data = [] 

    for line in log_lines:
      match = log_pattern.search(line)
      if match:
        response_code = match.group("response_code")
        message = match.group("message")

        # If response_code is present in the message, remove it
        if response_code:
            message = message.replace(f"({response_code})", "").strip()

        log_data.append({
            "task_logger_id": task_logger_id,
            "url": match.group("url"),
            "response_code": int(response_code) if response_code else None,
            "message": message,
            "signals_timestamp": match.group("timestamp")
        })
    log_df = pd.DataFrame(log_data)
    log_df = pd.DataFrame(log_data, dtype="object")
    log_df['signals_timestamp'] = pd.to_datetime(log_df['signals_timestamp'])
    list_chunked_df = list(chunk_data(log_df))
    helper = Helper(ScrapedSignalsLog)
    for df in list_chunked_df:
      helper.load_data(df)


    #remove the files  from the temporary directory
    for key, value in local_path['local_path'].items():  
      os.remove(value)
    return jsonify({"status": "success", "message": "ETL started successfully"}), 200
  except Exception as e:
    print(e)
    return jsonify({"status": "failed", "message": str(e)}), 500
  


def get_task_and_celery_id(file_path):
    try:
        file_name = os.path.basename(file_path)  # Get filename only
        parts = file_name.split("_")  # Split filename by "_"
        task_id, celery_id = parts[0], parts[1]  # Extract IDs
        return task_id, celery_id
    except Exception:
        return None, None  # Return None if parsing fails
@startetl.route('/temp_etl', methods=['POST'])
def upload_and_process_files():
    try:
        local_storage_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../temp'))
        required_files = ['errors_file', 'signals_file']
        local_path = {'local_path': {}}

        # Ensure required files exist in the request
        for file_key in required_files:
            if file_key not in request.files:
                return jsonify({'error': f'Missing file: {file_key}'}), 400

        # Save files
        for file_key in required_files:
            file = request.files[file_key]
            if file.filename == '':
                return jsonify({'error': f'No filename for {file_key}'}), 400

            file_path = os.path.join(local_storage_path, file.filename)
            file.save(file_path)
            local_path['local_path'][file_key] = file_path

        # Extract Task & Celery ID from `signals_file`
        task_id, celery_id = get_task_and_celery_id(local_path['local_path']['signals_file'])

        # Process `errors_file`
        with open(local_path['local_path']['errors_file'], 'r', encoding='utf-8') as file:
            errors_data = file.read().strip()

        # Remove empty JSON objects "{}"
        errors_data = re.sub(r'\{\s*\}', '', errors_data)
        errors_data = re.sub(r'}\s*{', '},\n{', errors_data)  # Fix missing commas between objects
        if not errors_data.startswith('['):  # Wrap in a list
            errors_data = f'[{errors_data}]'

        error_df = pd.read_json(StringIO(errors_data))
        error_df['task_logger_id'] = task_id
        error_df['celery_task_id'] = celery_id
        error_df['timestamp'] = pd.to_datetime(error_df['timestamp'])
        error_df.rename(columns={'timestamp': 'error_timestamp'}, inplace=True)
        list_chunked_error_df = list(chunk_data(error_df))
        helper = Helper(ScrapedErrorLog)
        for df in list_chunked_error_df:
            helper.load_data(df)

        # Process `signals_file`
        with open(local_path['local_path']['signals_file'], "r") as file:
            log_lines = file.readlines()

        log_pattern = re.compile(
            r"(?P<timestamp>[\d-]+\s[\d:]+) - INFO - (?P<message>.+?)\s*(?:\((?P<response_code>\d+)\))?\s*(?:from|for)?\s*(?P<url>https?://\S+)"
        )

        log_data = []
        for line in log_lines:
            match = log_pattern.search(line)
            if match:
                response_code = match.group("response_code")
                message = match.group("message")

                # If response_code is present in the message, remove it
                if response_code:
                    message = message.replace(f"({response_code})", "").strip()

                log_data.append({
                    "task_id": task_id,
                    "celery_id": celery_id,
                    "url": match.group("url"),
                    "response_code": int(response_code) if response_code else None,
                    "message": message,
                    "signals_timestamp": match.group("timestamp")
                })

        log_df = pd.DataFrame(log_data, dtype="object")
        log_df['signals_timestamp'] = pd.to_datetime(log_df['signals_timestamp'])
        log_df['task_logger_id'] = task_id
        log_df['celery_task_id'] = celery_id
        list_chunked_df = list(chunk_data(log_df))
        helper = Helper(ScrapedSignalsLog)
        for df in list_chunked_df:
            helper.load_data(df)

        # Remove files from the temporary directory
        for key, value in local_path['local_path'].items():
            os.remove(value)

        return jsonify({"status": "success", "message": "ETL started successfully"}), 200

    except Exception as e:
        print(e)
        return jsonify({"status": "failed", "message": str(e)}), 500
