import argparse
import pandas
import os
import shutil
from datetime import datetime
import glob
import numpy as np
import re 
import influxdb_client, os, time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import gzip
import logging

global org


# Create a logger
logger = logging.getLogger("MyLogger")
logger.setLevel(logging.DEBUG)  # Set the logger's overall level to the lowest level you want to capture (DEBUG)

# Create a file handler to log DEBUG and above
file_handler = logging.FileHandler("check_trend.log")
file_handler.setLevel(logging.DEBUG)  # File captures DEBUG and above

# Create a console handler to log INFO and above
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)  # Console shows INFO and above

# Create a formatter for both handlers
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", "%Y-%m-%d %H:%M:%S")
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Add the handlers to the logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)


token = "1_EX68vrFJOHvCu_Qu3r5s668UUcZKdwWhdsnleLa7EeDkGNwhzOWg_27LiYN8_jhbxZnF7ckoXLJItTF_h97g=="
org = "jci"
url = "http://192.168.1.203:8086"
bucket = "radarbucket"

client = InfluxDBClient(url=url, token=token, org=org)
write_api = client.write_api(write_options=SYNCHRONOUS)

#  Use loggger to logging.info messages to log
logging.basicConfig(level=logging.INFO)

# write_client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)

starttime = datetime.now()
logging.info(starttime)

parser = argparse.ArgumentParser(description='Convert and Compress Codesys csv datalog')
parser.add_argument('--f', default='/home/peter/Documents/Radar2.0/trend.txt', help='Input file')
parser.add_argument('--o', default='/home/peter/Documents/Radar2.0/trend_files/trend_output.txt', help='Output file')
parser.add_argument('--d', default='/home/peter/Documents/Radar2.0/Trend_files_zip', help='Trend file directory')
parser.add_argument('--w', default='/home/peter/Documents/Radar2.0/trend_files', help='trendfile working directory')                    
parser.add_argument('--v', action='store_true', help='Display version')
parser.add_argument('--c', default='Influx', help='Connection name')
args = parser.parse_args()

def check_trendfiles():
    logging.debug("Beginning processing!")
    logging.debug(f"Input file: {args.f}")
    logging.debug(f"Output file: {args.o}")
    logging.debug(f"Trend directory: {args.d}")
    logging.debug(f"Working directory: {args.w}")

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
        logging.debug(f"Warning: {e}")

    try:
        # List all files in the source directory
        files = [f for f in os.listdir(src_dir) if os.path.isfile(os.path.join(src_dir, f))]
        logging.debug(f"Number of files in '{src_dir}': {len(files)}")

        for file_name in files:
            src_file = os.path.join(src_dir, file_name)

            # Destination file path
            dest_file = os.path.join(dest_dir, file_name)

            # Copy file to destination
            shutil.copy(src_file, dest_file)
            logging.debug(f"Copied: {src_file} -> {dest_file}")
            match = re.search(pattern, file_name)
            # Unzip GZip files and remove the original if applicable
            if match:
                unzipped_name = dest_file + ".txt"  # Define the name of the unzipped file
                unzip_gz_file(dest_file, unzipped_name)  # Unzip the file
                os.remove(dest_file)  # Remove the original .gz file
                logging.debug(f"Unzipped: {unzipped_name}")
                #unzipped_files.append(unzipped_name)
                base_file_name = os.path.basename(unzipped_name)
                logging.debug(f"Unzipped name: {base_file_name}, type: {type(unzipped_name)}")

                ship_name = next(filter(None, match.groups()))  # Get the extracted ship name
                logging.debug(f"Ship name: {ship_name}")
                ship_names.append(ship_name)
            else:
                logging.info("No match found")


    except OSError as e:
        logging.debug(f"Error: {e}")

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
        logging.debug(f"File decompressed to '{output_file_path}'.")
    except FileNotFoundError:
        logging.debug(f"The file '{gz_file_path}' was not found.")
    except OSError as e:
        logging.debug(f"Error processing file '{gz_file_path}': {e}")
    except EOFError as e:
        logging.debug(f"Error end of file error '{gz_file_path}': {e}")  

def remove_files_by_extension(directory, extension):
    for filename in os.listdir(directory):
        if filename.endswith(extension):
            file_path = os.path.join(directory, filename)
            os.remove(file_path)  # Remove the file
            logging.debug(f"Removed: {file_path}")



def create_panda(trend_file):
    global mergedfiles
    global search_string
    global recordrows
    if (args.f.find('*')):    
        # record = (pandas.read_csv(file, names=['Value','Timestamp']) for file in glob.glob(args.f))
        record = (pandas.read_csv(file, names=['Value','Timestamp']) for file in trend_file)
        record = pandas.concat(record)        
        logging.debug('merged files ')        
        mergedfiles=datetime.now()            
    else:        
        record = pandas.read_csv(args.f, names=['Value','Timestamp']) # Generate header   


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
        match = re.search(r'\|([^|]+)\|', row['Value'])
        if match:
            last_valid_value2 = match.group(1)  # Extract matched value
            # logging.debug(f"Extracted Sensor: {last_valid_value2}")
        return last_valid_value2
    else:
        # Use the last valid value if condition is not met
        return last_valid_value2

def write_influx(dframe, bucket_name, measure_name):
    import pandas as pd

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
    write_api.write(
        bucket=bucket_name,
        org=org,
        record=dframe,
        data_frame_measurement_name=measure_name,
        data_frame_tag_columns=["Chiller", "Sensor"],  # Tag column names
    )

#  Start of the main program
file_check_time, trend_filenames, trend_shipnames = check_trendfiles()
for file, shipname in zip(trend_filenames, trend_shipnames):
    logging.info("Trend filenames: " + file + " Shipname " + shipname)

#  Convert the trend file to Pandas dataframe
# search_string = "Codesys"  # search string to catch the lines
search_string = "LOCAL."  # search string to catch the lines

mod_dataframe=create_panda(glob.glob(args.f))
scriptchiller = datetime.now()




scriptend=datetime.now()

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

logging.info('data frame number of rows %s' % recordrows)
logging.info('move of file time. %s seconds' % movet)
logging.info('merge to pandas dataframe of input file time. %s seconds' % merget)
logging.info('search and add chillers column of dataframe time. %s seconds' % chillert)
logging.info('search and add sensort type column of dataframe time. %s seconds' % sensort)
logging.info('script total execution time. %s second' % exect)

logging.debug(mod_dataframe)

logging.info('data frame number of rows %s' % recordrows)
logging.info('move of file time. %s seconds' % movet)
logging.info('merge to pandas dataframe of input file time. %s seconds' % merget)
logging.info('search and add chillers column of dataframe time. %s seconds' % chillert)
logging.info('search and add sensort type column of dataframe time. %s seconds' % sensort)
logging.info('script total execution time. %s second' % exect)
write_influx(mod_dataframe,bucket,"testship")
db_write_end=datetime.now()
totalwritetime=db_write_end-scriptend
db_writetime=int(round(totalwritetime.total_seconds()))
logging.info('DB write time. %s second' % db_writetime)

