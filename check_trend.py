import argparse
import pandas
import os
import shutil
from datetime import datetime
import glob
import numpy as np
import re 

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

def write_influx(dframe,bucket,measure)

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

print(filtered_record)

print('data frame number of rows %s' % recordrows)
print('move of file time. %s seconds' % movet)
print('merge to pandas dataframe of input file time. %s seconds' % merget)
print('search and add chillers column of dataframe time. %s seconds' % chillert)
print('search and add sensort type column of dataframe time. %s seconds' % sensort)
print('script total execution time. %s second' % exect)