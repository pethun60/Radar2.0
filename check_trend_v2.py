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

global org


token = "1_EX68vrFJOHvCu_Qu3r5s668UUcZKdwWhdsnleLa7EeDkGNwhzOWg_27LiYN8_jhbxZnF7ckoXLJItTF_h97g=="
org = "jci"
url = "http://192.168.1.203:8086"
bucket = "radarbucket"

client = InfluxDBClient(url=url, token=token, org=org)
write_api = client.write_api(write_options=SYNCHRONOUS)

# write_client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)

starttime = datetime.now()
print(starttime)

parser = argparse.ArgumentParser(description='Convert and Compress Codesys csv datalog')
parser.add_argument('--f', default='/home/peter/Documents/Radar2.0/trend.txt', help='Input file')
parser.add_argument('--o', default='/home/peter/Documents/Radar2.0/trend_files/trend_output.txt', help='Output file')
parser.add_argument('--v', action='store_true', help='Display version')
parser.add_argument('--c', default='Influx', help='Connection name')
args = parser.parse_args()


print('Beginning processing!')
print(' Input file : ' + args.f)
print(' Output file: ' + args.o)

try:
    os.remove(args.o)
except OSError:
    pass

try:
    shutil.copy(args.f, '/home/peter/Documents/Radar2.0/trend_files/in_trend.txt')
except OSError:
    pass
filemoved=datetime.now()
print(filemoved)

if (args.f.find('*')):    
    record = (pandas.read_csv(file, names=['Value','Timestamp']) for file in glob.glob(args.f))
    record = pandas.concat(record)        
    print('merged files ')        
    mergedfiles=datetime.now()            
else:        
    record = pandas.read_csv(args.f, names=['Value','Timestamp']) # Generate header   

recordrows=len(record.index)  #no of rows in the input file
search_string = 'Codesys'
input_string = "sensorname"

# Remove the # in the value column
record["Value"] = record["Value"].str.replace("#", "", regex=False)
record["Value"] = record["Value"].replace({"#FALSE": "0","#TRUE": "1"})


# Initialize a variable to store the last valid value
last_valid_value = None
last_valid_value2 = None
# Function to determine Chiller value
def get_chiller(row):
    global last_valid_value  # Use global or better encapsulate for cleaner design
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
            # print(f"Extracted Sensor: {last_valid_value2}")
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


# Apply the function row by row
record['Chiller'] = record.apply(get_chiller, axis=1)
scriptchiller=datetime.now()
record['Sensor'] = record.apply(get_sensor_type, axis=1)
filtered_record = record[~record['Value'].str.contains(search_string, na=False)]

scriptend=datetime.now()

movetime=filemoved-starttime
mergedtime=mergedfiles-filemoved
chillertime=scriptchiller-mergedfiles
sensortime=scriptend-mergedfiles
totalscripttime=scriptend-starttime

movet=int(round(movetime.total_seconds()))
merget=int(round(mergedtime.total_seconds()))
chillert=int(round(chillertime.total_seconds()))
sensort=int(round(sensortime.total_seconds()))
exect=int(round(totalscripttime.total_seconds()))

print('data frame number of rows %s' % recordrows)
print('move of file time. %s seconds' % movet)
print('merge to pandas dataframe of input file time. %s seconds' % merget)
print('search and add chillers column of dataframe time. %s seconds' % chillert)
print('search and add sensort type column of dataframe time. %s seconds' % sensort)
print('script total execution time. %s second' % exect)

print(record)
print(filtered_record)

print('data frame number of rows %s' % recordrows)
print('move of file time. %s seconds' % movet)
print('merge to pandas dataframe of input file time. %s seconds' % merget)
print('search and add chillers column of dataframe time. %s seconds' % chillert)
print('search and add sensort type column of dataframe time. %s seconds' % sensort)
print('script total execution time. %s second' % exect)
write_influx(filtered_record,bucket,"testship")
db_write_end=datetime.now()
totalwritetime=db_write_end-scriptend
db_writetime=int(round(totalwritetime.total_seconds()))
print('DB write time. %s second' % db_writetime)

