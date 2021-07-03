import sys

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import time
import datetime
import pytz

from influxdb import InfluxDBClient, DataFrameClient
import datetime
import tzlocal

import h5py


def parse_time_str(time_str_list):
    '''
    This returns a naive datetime object from an input string.
    The string must be either an ISO-8601 format or an epoch unixtime (UTC).
    Returns a datetime.datetime, and a boolen that is true when the time is recognised as unix timestamp
    In case of not recognition a None object and a False flag are returned.
    '''
    
    ntokens = len(time_str_list)
    
    time_str = ''
    
    if ntokens>1:
        for i in range(ntokens):
            time_str += time_str_list[i] + ' '
        time_str = time_str[:-1] # remove the last blank space
    
    if ntokens==1:
        time_str = time_str_list[0]
    
    try:
        dt = dateutil.parser.isoparse(time_str)
        dt = dt.replace(tzinfo=None) #Will return a "naive" datetime object
    except:
        pass
    else:
        return dt, False
    
    
    try:
        epoch = int(time_str)
    except:
        return None, False
    else:
        dt = datetime.datetime.utcfromtimestamp(epoch) #Returns a datetime object with UTC timezone (non-naive)
        return dt, True
    
    


def Create_hdf(host, db_name, start_time, end_time=None, outfilename='SC_data.h5', measurements=None, port=8086, time_delta='1h'):
    
    client = DataFrameClient(host=host, port=port, database=db_name)
    
    here_tz = tzlocal.get_localzone()
    utc_tz = pytz.timezone('UTC')
    if start_time.tzinfo is None:
        start_time = start_time.replace(here_tz)
        start_time = here_tz.localize(start_time, is_dst=True)
        
    
    #First localise the time in the local time and bring to UTC zone for better working with it later
    now_time = here_tz.localize(datetime.datetime.now(), is_dst=True).astimezone(utc_tz)
    
    if end_time is None:
        end_time = here_tz.localize(datetime.datetime.now(), is_dst=True).astimezone(utc_tz)
    else:
        if end_time.tzinfo is None:
            end_time = here_tz.localize(datetime.datetime.now(), is_dst=True)
            
    
    #Define the time steps between data pollings using the utc time
    dt_steps = pd.date_range(start=start_time.astimezone(utc_tz), end=end_time.astimezone(utc_tz), freq=time_delta)
    
    qr_pre_str = 'SELECT * FROM '
    
    if measurements is None:
        #Save all the measurements time series in the given database
        for meas in client.get_list_measurements():
            qr_pre_str += '"'+ meas['name'] +'",'
    else:
        #Save only the measurements time series asked by the user
        for meas in measurements:
            qr_pre_str += '"'+ meas +'",'
    
    #Delete the last ','
    qr_pre_str = qr_pre_str[:-1]
    
    coll = dict()

    for i in range(len(dt_steps)-1):
        t_low = dt_steps[i]
        t_up = dt_steps[i+1]
        qr_str = qr_pre_str + ' WHERE (time >= %s) AND (time < %s) '%(str(int(t_low.timestamp()))+'s', str(int(t_up.timestamp()))+'s')
        coll_tmp = client.query(qr_str)
        
        for meas in coll_tmp:
            if meas in coll:
                coll[meas] = pd.concat([coll[meas], coll_tmp[meas]])
            else:
                coll[meas] = coll_tmp[meas]
        
        time.sleep(0.5)
    
    #Save all the measurements into a database
    for meas in coll:
        coll[meas].to_hdf(outfilename, key=meas, mode='a', format='fixed')




def Update_hdf(host, db_name, filename, measurements=None, port=8086, time_delta='1h'):
    
    client = DataFrameClient(host=host, port=port, database=db_name)
    
    #Auxiliary dictionary for each group of measurements time series
    meas_dict = dict()
    
    #open the filename and determine which is the last datetime point
    if measurements is None:
        #Update all the measurements time series found in the file
        with h5py.File(filename, 'r') as f:
            for meas in list(f.keys()):
                meas_dict[meas] = None
    else:
        #Update only the measurements time series given by the user
        for meas in measurements:
            meas_dict[meas] = None
    
    #There should be an easier way to read the different dataframes using only one loop!!!!
    
    here_tz = tzlocal.get_localzone()
    utc_tz = pytz.timezone('UTC')
    
    #First localise the time in the local time and bring to UTC zone for better working with it later
    now_time = here_tz.localize(datetime.datetime.now(), is_dst=True).astimezone(utc_tz)
    
    for meas in meas_dict:
        
        print('Updating time series for the measurement "' + str(meas) + '":')
        
        meas_df = pd.read_hdf(filename, key=meas)
        
        
        if meas_df is not None:
            meas_dict[meas] = meas_df
        else:
            print('No dataframe present for the measurment "' + str(meas) + '"')
            continue
        
        
        #Move the datetimes from index to column
        meas_df.reset_index(inplace=True)
        meas_df.rename(columns={'index':'UTC'},inplace=True)
        meas_df.sort_values('UTC', inplace=True)
        
        #To be sure go 1 unit of "time_delta" back in time to have the proper start time
        #Duplicates will be removed later
        last_utc = meas_df['UTC'].iloc[-1] - pd.Timedelta(time_delta)
        
        print('\t last UTC time: ', last_utc, ' --> local time: ', pd.to_datetime(last_utc).astimezone(here_tz))
        
        #Define the time steps between data pollings using the utc time
        dt_steps = pd.date_range(start=last_utc, end=now_time, freq=time_delta)
        
        qr_pre_str = 'SELECT * FROM "' + str(meas) + '" '
        
        meas_new = None
        for i in range(len(dt_steps)-1):
            #print(i)
            t_low = dt_steps[i]
            t_up = dt_steps[i+1]
            qr_str = qr_pre_str + ' WHERE (time >= %s) AND (time < %s) '%(str(int(t_low.timestamp()))+'s', str(int(t_up.timestamp()))+'s')
            #print(qr_str)
            res = client.query(qr_str)
            
            if len(res)== 0:
                #In this case there is no data returned that satisfies the query
                continue
                
            if meas_new is None:
                meas_new = res[meas]
            else:
                meas_new = pd.concat([meas_new,res[meas]])
                
            time.sleep(0.2)
        
        #Make a last readout of the last chunk of data
        t_low = dt_steps[-1]
        t_up = t_low + pd.Timedelta(time_delta)
        qr_str = qr_pre_str + ' WHERE (time >= %s) AND (time < %s) '%(str(int(t_low.timestamp()))+'s', str(int(t_up.timestamp()))+'s')
        res = client.query(qr_str)
        if len(res) > 0:
            meas_new = pd.concat([meas_new,res[meas]])
        
        meas_new.reset_index(inplace=True)
        meas_new.rename(columns={'index':'UTC'},inplace=True)
        meas_new.sort_values('UTC', inplace=True)
            
        meas_df = pd.concat([meas_df, meas_new])
            
    #For each measurement find and remove duplicates
    for meas in meas_dict:
        if meas_dict[meas].duplicated().sum()>0:
            meas_dict[meas].drop_duplicates(inplace=True)
        
        #Reset the UTC time as index
        meas_dict[meas].set_index('UTC',inplace=True)
        meas_dict[meas].to_hdf(filename, key=meas, mode='a', format='fixed')
        
            
    


if __name__ == '__main__':
    
    #Default arguments that can be changed from the user
    update = False #Default in recreate mode
    db_name = 'singlemodule_nov2020'
    host = 'argoncube02.aec.unibe.ch'
    port = 8086
    time_delta='1h'
    start_time = None #This is mandatory in creation mode and not used in update mode
    is_unixtimestamp = False #If this is true it means that the time is provided from the user as a utc timestamp
    utc_format = False #If true the ISO-8601 string time is interpreted as a utc time otherwise as local time (taken from the machine local zone)
    measurements = ["PLC","LAr_Level","LAr_Level_pF","LN2_Level","LN2_Level_pF","temp","flow"]
    outfilename = 'SC_data.h5'
    filename = None
    
    
    #Processing of the options
    args = sys.argv[1:]
    
    iArg = 0
    for i in len(args):
        if i < iArg:
            continue
        iArg = i
        if arg[i] == '-r':
            update = False
            if (i<len(args)) and (arg[i+1][0]!='-'):
                outfilename = arg[i+1]
                iArg = i+1
            
        elif arg[i] == '-u':
            if (i<len(args)) and (arg[i+1][0]!='-'):
                filename = arg[i+1]
                update = True
                iArg = i+1
            else:
                print("ERROR: option -u must be followed by the name of the file to be updated")
                sys.exit()
        
        elif (arg[i] == '--meas') or (arg[i] == '-m'):
            if (i>=len(args)-1) or (arg[i+1][0]=='-'):
                measurements = list()
                iArg = i+1
                while (arg[iArg][0] !='-') and (iArg<len(args)):
                    measurements.append(arg[iArg])
                    iArg += 1
            else:
                print("ERROR: option --port must be followed by the port number of the influxdb server")
                sys.exit()
        
        elif (arg[i] == '--host'):
            if (i<len(args)) and (arg[i+1][0]!='-'):
                host = arg[i+1]
                iArg = i+1
            else:
                print("ERROR: option --host must be followed by the name of the host running the influxdb server")
                sys.exit()
        
        elif (arg[i] == '--port'):
            if (i<len(args)) and (arg[i+1][0]!='-'):
                port = int(arg[i+1])
            else:
                print("ERROR: option --port must be followed by the port number of the influxdb server")
                sys.exit()
        
        elif (arg[i]=='-t'):
            if (i<len(args)) and (arg[i+1][0]!='-'):
                time_str_list = list()
                iArg = i+1
                while (arg[iArg][0] !='-') and (iArg<len(args)):
                    time_str_list.append(arg[iArg])
                    iArg += 1
                start_time, is_unixtimestamp = parse_time_str(time_str_list)
                if start_time == None:
                    print("ERROR: The date format introduced after the -t option can not be parsed/recognised.")
                    sys.exit()
            else:
                print("ERROR: option -t must be followed by the datetime start of the time series to be saved.")
                sys.exit()
            
        elif (arg[i]=='--utc'):
            utc_format = True
        else:
            print('ERROR: option "' + args[i] + '" does not exist!')
    
    
    if start_time is not None:
        
        if (utc_format==False) and (is_unixtimestamp==False):
            start_time = start_time.replace(tzinfo=tz.tzlocal())
        elif (is_unixtimestamp==False):
            start_time = start_time.replace(tzinfo=dateutil.tz.gettz('UTC'))
        
        if update:
            if filename is None:
                print('ERROR: In update mode the file name must be given!')
                sys.exit()
            Update_hdf(host=host, db_name=db_name, measurements=measurements, filename=filename, start_time=start_time, port=port, time_delta=time_delta)
        else:
            Create_hdf(host=host, db_name=db_name, start_time=start_time, outfilename=outfilename, measurements=measurements, port=port, time_delta=time_delta)

            
