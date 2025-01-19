#!/usr/bin/env python3

import argparse
import pandas
import gzip
from datetime import datetime
import shutil
import os
import glob

version = '1.0.6'
now = datetime.now()
print(now)
# TODO: git version

# Convert and compress Codesys csv datalog so it is ready for Iris email

#------------------------------------------------------------------------------

try:
    # Create comandline parser
    parser = argparse.ArgumentParser(description='Convert and Compress Codesys csv datalog')
    parser.add_argument('--v', action='store_true', help='Display version')
    #parser.add_argument('--i', default='/home/peter/csv_files/newradar.csv', help='Input file')
    #parser.add_argument('--o', default='/home/peter/trend.txt', help='Output file')
    parser.add_argument('--i', default='/home/jci/csv_files/*.csv', help='Input file')
    parser.add_argument('--o', default='/home/jci/trend.txt', help='Output file')
    #parser.add_argument('--i', default='./*.csv', help='Input file')
    #parser.add_argument('--o', default='./trend.txt', help='Output file')
    parser.add_argument('--c', default='Codesys', help='Connection name')
    parser.add_argument('--r', default='120', help='Resolution')
    args = parser.parse_args()

    # The actual program
    if args.v:
       print('CodesysCsv version ' + version) # Print version information
    else:
        print('Beginning processing!')
        print(' Input file : ' + args.i)
        print(' Output file: ' + args.o)
        print(' Connection name : ' + args.c)
        print(' Resolution : ' + args.r)
        print()

        try:
            os.remove('/home/jci/mail/trend.txt.gz')
        except OSError:
            pass
        try:
            os.remove('/home/jci/csv_files/Application_DataLogChannel.csv')
        except OSError:
            pass
        try:
            os.remove('/home/jci/csv_files/Application_DataLogChannel_1.csv')
        except OSError:
            pass
        try:
            os.remove('/home/jci/csv_files/Application_DataLogChannel_2.csv')
        except OSError:
            pass
        try:
            os.remove('/home/jci/csv_files/Application_DataLogChannel_3.csv')
        except OSError:
            pass
        try:
            os.remove('/home/jci/csv_files/Application_DataLogChannel_4.csv')
        except OSError:
            pass
        try:
            os.remove('/home/jci/csv_files/Application_DataLogChannel_5.csv')
        except OSError:
            pass
        try:
            os.remove('/home/jci/csv_files/Application_DataLogChannel_6.csv')
        except OSError:
            pass
        try:
            shutil.move("/var/opt/codesys/PlcLogic/ac_datalog/Application_DataLogChannel.csv", '/home/jci/csv_files/')
        except OSError:
            pass
        try:
            shutil.move("/var/opt/codesys/PlcLogic/ac_datalog/Application_DataLogChannel_1.csv", '/home/jci/csv_files/')
        except OSError:
            pass
        try:
            shutil.move("/var/opt/codesys/PlcLogic/ac_datalog/Application_DataLogChannel_2.csv", '/home/jci/csv_files/')        
        except OSError:
            pass
        try:
            shutil.move("/var/opt/codesys/PlcLogic/ac_datalog/Application_DataLogChannel_3.csv", '/home/jci/csv_files/')
        except OSError:
            pass
        try:
            shutil.move("/var/opt/codesys/PlcLogic/ac_datalog/Application_DataLogChannel_4.csv", '/home/jci/csv_files/')
        except OSError:
            pass
        try:
            shutil.move("/var/opt/codesys/PlcLogic/ac_datalog/Application_DataLogChannel_5.csv", '/home/jci/csv_files/')
        except OSError:
            pass
        try:
            shutil.move("/var/opt/codesys/PlcLogic/ac_datalog/Application_DataLogChannel_6.csv", '/home/jci/csv_files/')
        except OSError:
            pass

        filemoved=datetime.now()
        print(filemoved)
        if (args.i.find('*')):
            record = (pandas.read_csv(file, names=['Stamp','Name','Value','Type']) for file in glob.glob(args.i))
            record = pandas.concat(record)
            print('merged files ')
            mergedtime=datetime.now()
        else:
            record = pandas.read_csv(args.i, names=['Stamp','Name','Value','Type']) # Generate header

        outFile = open(args.o, 'w+')

        initial = True
        record['Value'] = record['Value'].apply(str)    # Convert to string
        record['Stamp'] = pandas.to_datetime(record['Stamp'], format='%Y-%m-%d-%H:%M:%S:%f')    # Correct time/date format
        recordrows=len(record.index)  #no of rows in the csv file
        for sensor in record['Name'].unique():  # Loop through all unique sensor.point
            if initial:
                initial = False
            else:
                #print()
                outFile.write('\n')

            sensorSplit = sensor.split(sep='.')
            header = '*LOCAL.' + args.c + '.' + sensor + '|' + sensorSplit[1] +'|' + args.r + '|0|0' # Generate header line
            #print(' ' + header)
            outFile.write(header + '\n')
            recordSensor = record.query('Name == @sensor')
            recordSensor.apply(
                lambda row: outFile.write('#' + row['Value'] + ',' + row['Stamp'].strftime("%Y-%m-%d %H:%M:%S") + '\n'),     # Generate data line
                axis=1
            )

        outFile.close()

        with open(args.o, 'rb') as dataFile:    # Zip the file
            with gzip.open(args.o + '.gz', 'wb') as zipFile:
                zipFile.writelines(dataFile)

        shutil.move('/home/jci/trend.txt.gz', '/home/jci/mail')
        scriptend=datetime.now()
        exectime=scriptend-now
        movetime=mergedtime-now
        exect=int(round(exectime.total_seconds()))
        movet=int(round(movetime.total_seconds()))
        print('data frame number of rows %s' % recordrows)
        print('script execution time. %s second' % exect)
        print('merge of file time. %s seconds' % movet)
except Exception as e:
    print(repr(e))
