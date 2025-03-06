import argparse
import pandas
import os
import shutil
from datetime import datetime, timedelta
import glob
import numpy as np
import re 
import influxdb_client, os, time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import gzip
import logging
import sys
from pathlib import Path

global org


# Ensure the "logging" directory exists
log_dir = "logging"
os.makedirs(log_dir, exist_ok=True)

# Define the full path for the log file
log_file = os.path.join(log_dir, "check_trend.log")

# Create a logger
logger = logging.getLogger("MyLogger")
logger.setLevel(logging.DEBUG)  # Capture DEBUG and above messages

# Clear existing handlers from root (prevents interference from default handlers)
logging.getLogger().handlers = []

# Create a file handler to log DEBUG and above
file_handler = logging.FileHandler(log_file)
file_handler.setLevel(logging.DEBUG)

# Create a console handler (use stdout to prevent it from using stderr)
console_handler = logging.S treamHandler(sys.stdout)  # Ensure logging goes to stdout
console_handler.setLevel(logging.INFO)  # Show only INFO and above on console

# Create a formatter
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", "%Y-%m-%d %H:%M:%S")
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Add the handlers to the logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# Example log messages
logger.debug("Debug message: logging is set up.")
logger.info("Info message: logging is set up.")


starttime = datetime.now()
mergedfiles = datetime.now()
logger.info(starttime)
search_string = "LOCAL."  # search string to catch the lines

parser = argparse.ArgumentParser(description='Convert and Compress Codesys csv datalog')
parser.add_argument('--t', default=30, help='script delaytime')
parser.add_argument('--o', default='/home/peter/Documents/Radar2.0/trend_files/trend_output.txt', help='Output file')
#parser.add_argument('--d', default='/home/peter/Documents/Radar2.0/Trend_files_zip', help='Trend file directory')
parser.add_argument('--d', default=str(Path.cwd()), help='Trend file directory')
parser.add_argument('--w', default='/home/peter/Documents/Radar2.0/trend_files', help='trendfile working directory')                    
parser.add_argument('--v', action='store_true', help='Display version')
parser.add_argument('--c', default='Influx', help='Connection name')
parser.add_argument('--token', default='8swycDQhZlMSdCNkXdKwTJkFHBQ_pqkn7Yl8W74Yf7agEIci-ot1mdtnM1-F_qjSi57PJnRRy9dJUiPgboglFg==', help='Token string')
parser.add_argument('--bucket', default='radarbucket', help='Bucket name')
parser.add_argument('--url', default='thunholm.homelinux.com:8086', help='Influxdb url and port')
parser.add_argument('--org', default='jci', help='Organisation name')
args = parser.parse_args()


token = args.token
org = args.org
url = args.url
bucket = args.bucket

client = InfluxDBClient(url=url, token=token, org=org)
write_api = client.write_api(write_options=SYNCHRONOUS)

#  Use loggger to logging.info messages to log
# logger.basicConfig(level=logging.INFO)

# write_client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
 

starttime = datetime.now()
mergedfiles = datetime.now()
logger.info(starttime)
search_string = "LOCAL."  # search string to catch the lines
logger.debug(f"influxdb bucket : {args.bucket}")
logger.debug(f"influxdb organisation: {args.org}")
logger.debug(f"influxdb url: {args.url}")
logger.debug(f"influxdb token: {args.token}")


def check_trendfiles():
    logger.info("Beginning processing!")
    logger.debug(f"script delay time: {args.t}")
    logger.debug(f"Output file: {args.o}")
    logger.debug(f"Trend directory: {args.d}")
    logger.debug(f"Working directory: {args.w}")


    src_dir = args.d
    dest_dir = args.w
    unzipped_files = []  # List to store unzipped filenames
    ship_names = []
    # pattern = r"(?<=Norrkoping)(.*?)\.(gz\.txt)$"  # regular expression pattern to extract shipname
    # pattern = r"^(?!FI_Kauhavan)[A-Za-z0-9\s]*Marine\s*Norrkoping[_\s]*([A-Za-z0-9]+(?:[_A-Za-z0-9]+)*)\.gz\.txt$"
    pattern = re.compile(r'(?<=Norrkoping_)(.+?)(?=\.gz$)|(?<=_Norrkoping_)(.+?)(?=\.gz$)|(?<=Marine_Nrk_)(.+?)(?=\.gz$)')

    # Remove output file if it exists
    try:
        os.remove(args.o)
    except OSError as e:
        logger.debug(f"Warning: {e}")

    try:
        # List all files in the source directory
        files = [f for f in os.listdir(src_dir) if os.path.isfile(os.path.join(src_dir, f))]
        logger.debug(f"Number of files in '{src_dir}': {len(files)}")

        for file_name in files:
            src_file = os.path.join(src_dir, file_name)

            # Destination file path
            dest_file = os.path.join(dest_dir, file_name)

            # Copy file to destination
            shutil.copy(src_file, dest_file)
            logger.debug(f"Copied: {src_file} -> {dest_file}")
            match = re.search(pattern, file_name)
            # Unzip GZip files and remove the original if applicable
            if match:
                unzipped_name = dest_file + ".txt"  # Define the name of the unzipped file
                unzip_gz_file(dest_file, unzipped_name)  # Unzip the file
                os.remove(dest_file)  # Remove the original .gz file
                logger.debug(f"Unzipped: {unzipped_name}")
                unzipped_files.append(unzipped_name)
                base_file_name = os.path.basename(unzipped_name)
                logger.debug(f"Unzipped name: {base_file_name}, type: {type(unzipped_name)}")

                ship_name = next(filter(None, match.groups()))  # Get the extracted ship name
                logger.debug(f"Ship name in check_trendfiles: {ship_name}")
                ship_names.append(ship_name)
            else:
                logger.info("No match found")


    except OSError as e:
        logger.debug(f"Error copying files: {e}")

    # Record the time when files were moved
    filemoved = datetime.now()
    return filemoved, unzipped_files, ship_names

def unzip_gz_file(gz_file_path, output_file_path):
    """
    Decompresses a GZip file.

    Parameters:
        gz_file_path (str): Path to the .gz file.
        output_file_path (str): Path to save the decompressed file.
    """
    try:
        with gzip.open(gz_file_path, 'rb') as gz_file:
            with open(output_file_path, 'wb') as out_file:
                shutil.copyfileobj(gz_file, out_file)
        logger.debug(f"File decompressed to '{output_file_path}'.")
    except FileNotFoundError:
        logger.debug(f"The file '{gz_file_path}' was not found.")
    except OSError as e:
        logger.debug(f"Error processing file '{gz_file_path}': {e}")
    except EOFError as e:
        logger.debug(f"Error end of file error '{gz_file_path}': {e}")  

def remove_files_by_extension(directory, extension):
    for filename in os.listdir(directory):
        if filename.endswith(extension):
            file_path = os.path.join(directory, filename)
            os.remove(file_path)  # Remove the file
            logger.debug(f"Removed: {file_path}")



def create_panda(trend_file):
    global mergedfiles
    global search_string
    global recordrows
    valid_files = []
    cutoff_date = datetime.now() - timedelta(days=60)
    
    try:
        if trend_file and isinstance(trend_file, list):
            # Check if the files in trend_file exist and are not empty
            for file in trend_file:
                if os.path.isfile(file) and os.path.getsize(file) > 0:
                    temp_record = pandas.read_csv(file, names=['Value', 'Timestamp'])
                    temp_record['Timestamp'] = pandas.to_datetime(temp_record['Timestamp'], errors='coerce')
                    if temp_record['Timestamp'].max() >= cutoff_date:
                        valid_files.append(file)
                    else:
                        logger.warning(f"File '{file}' contains timestamps older than 60 days.")
                else:
                    logger.warning(f"File '{file}' is empty or does not exist.")
            
            if valid_files:
                record = (pandas.read_csv(file, names=['Value', 'Timestamp']) for file in valid_files)
                record = pandas.concat(record)        
                logger.debug('merged files')        
                mergedfiles = datetime.now()
            else:
                logger.warning('No valid files to process.')
        else:
            # Single file processing for file
            if os.path.isfile(file) and os.path.getsize(file) > 0:
                record = pandas.read_csv(file, names=['Value', 'Timestamp'])  # Generate header
            else:
                logger.warning(f"File '{file}' is empty or does not exist.")
        recordrows=len(record.index)  #no of rows in the input file
        input_string = "sensorname"

        # Remove the # in the value column
        record["Value"] = record["Value"].str.replace("#", "", regex=False)
        record["Value"] = record["Value"].replace({"#FALSE": "0","#TRUE": "1"})
        # Apply the function row by row
        record['Chiller'] = record.apply(get_chiller, axis=1)
        record['Sensor'] = record.apply(get_sensor_type, axis=1)
        filtered_record = record[~record['Value'].str.contains(search_string, na=False)]
        return filtered_record        
    except Exception as e:
        logger.error(f"An error occurred while processing files: {e}")




# Initialize a variable to store the last valid value
last_valid_value = None
last_valid_value2 = None
# Function to determine Chiller value
def get_chiller(row):
    global last_valid_value  # Use global or better encapsulate for cleaner design
    global search_string
    if search_string in row['Value']:
        # Extract value when condition is met
        last_valid_value = row['Value'].split('.')[2] if len(row['Value'].split('.')) > 2 else None
        return last_valid_value
    else:
        # Use the last valid value if condition not met
        return last_valid_value

def get_sensor_type(row):
    global last_valid_value2  # Keep track of the last valid value
    if search_string in row['Value']:
        # Use regex to extract the value between the first set of pipe characters
        # match = re.search(r'\|([^|]+)\|', row['Value'])
        match = re.search(r'\.([^.|]+)(?=\|)|\|([^|]+)\|', row['Value'])

        if match:
            last_valid_value2 = match.group(1)  # Extract matched value
            # logger.debug(f"Extracted Sensor: {last_valid_value2}")
        return last_valid_value2
    else:
        # Use the last valid value if condition is not met
        return last_valid_value2

def write_influx(dframe, bucket_name, measure_name):
    import pandas as pd

    # Check if the DataFrame is None
    if dframe is None:
        logger.debug("DataFrame is None. Skipping InfluxDB insertion.")
        return  # Exit the function early

    # Check if the DataFrame is empty
    if dframe.empty:
        logger.debug("DataFrame is empty. Skipping InfluxDB insertion.")
        return  # Exit the function early

    # Ensure we are working on a copy
    dframe = dframe.copy()

    # Validate required 'Timestamp' column and convert it
    if "Timestamp" not in dframe.columns:
        raise ValueError("The DataFrame must have a 'Timestamp' column.")
    dframe["Timestamp"] = pd.to_datetime(dframe["Timestamp"])
    
    # Set the 'Timestamp' column as the index for InfluxDB
    dframe = dframe.set_index("Timestamp")
    
    # Ensure 'Value' column is numeric
    dframe["Value"] = pd.to_numeric(dframe["Value"], errors="coerce")

    # Validate required columns for tags
    required_columns = ["Chiller", "Sensor"]
    missing_columns = [col for col in required_columns if col not in dframe.columns]
    if missing_columns:
        raise ValueError(f"Missing required tag columns: {missing_columns}")

    # Write the DataFrame to InfluxDB
    try:
        write_api.write(
            bucket=bucket_name,
            org=org,
            record=dframe,
            data_frame_measurement_name=measure_name,
            data_frame_tag_columns=["Chiller", "Sensor"],  # Tag column names
        )
    except Exception as e:
        logger.error(f"Failed to write data to InfluxDB: {e}")
        raise RuntimeError(f"Failed to write data to InfluxDB: {e}")
        
def main():
    while True:
        starttime = datetime.now()
        #  Start of the main program
        file_check_time, trend_filenames, trend_shipnames = check_trendfiles()


        for file, shipname in zip(trend_filenames, trend_shipnames):
            logger.debug('filename '+ file + ' ship name '+ shipname )
            #  Convert the trend file to Pandas dataframe
            # search_string = "Codesys"  # search string to catch the lines
            
            mod_dataframe=create_panda(glob.glob(file))
            logger.debug('mod_dataframe' )
            logger.debug(mod_dataframe )
            #  write dataframe with shipnameto influxdb
            logger.debug('data frame number of rows %s' % recordrows)
            write_influx(mod_dataframe,bucket,shipname)
            logger.debug('ship data  writtten to influxdb %s' % shipname)



        scriptend=datetime.now()




        scriptchiller=datetime.now()
        movetime=file_check_time-starttime
        mergedtime=mergedfiles-file_check_time
        chillertime=scriptchiller-mergedfiles
        sensortime=scriptend-mergedfiles
        totalscripttime=scriptend-starttime

        movet=int(round(movetime.total_seconds()))
        merget=int(round(mergedtime.total_seconds()))
        chillert=int(round(chillertime.total_seconds()))
        sensort=int(round(sensortime.total_seconds()))
        exect=int(round(totalscripttime.total_seconds()))



        logger.info('move of file time. %s seconds' % movet)
        logger.info('merge to pandas dataframe of input file time. %s seconds' % merget)
        logger.debug('search and add chillers column of dataframe time. %s seconds' % chillert)
        logger.debug('search and add sensort type column of dataframe time. %s seconds' % sensort)
        logger.info('script total execution time. %s second' % exect)

        db_write_end=datetime.now()
        totalwritetime=db_write_end-scriptend

        db_writetime=int(round(totalwritetime.total_seconds()))
        logger.info('DB write time. %s second' % db_writetime)
          
        logger.info("Sleeping for " + args.t + " seconds")
        time.sleep(int(args.t))  # Sleep for 30 minutes
if __name__ == "__main__":
    main()
