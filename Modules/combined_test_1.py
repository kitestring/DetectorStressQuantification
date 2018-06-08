import math
import os
import platform
import re
import numpy as np
import pandas as pd


def parce_DM_IonStatistics(row, stringPattern):
    # this function will return a string with the detector voltage and area per ion comma delimited
    # all columns which do not have this information will return "False,False"
        # row in the df row
        # stringPattern is the re formatted string pattern that will be matched
        
    compiled = re.compile(stringPattern)
    
    # Alogrithm is only applied to DM realted rows
    # Looking for DM rows where the Action column begins with Voltage=
    if row['Object'] == "Detector Measurement v5":
        
        if type(row['Action']) is str:
            result = compiled.match(row['Action'])
            if result != None:
                split_result = result.group(0).split(' ')
                return split_result[0].replace('Voltage=','') + ',' + split_result[1].replace('AreaPerIon=','')
            
    # Action column is not a string or does not match the pattern does not contain the ion statistics I'm looking for
    return "False,False"

def Convert12hrto24hrTime(row):
    twelve_hour_time_parced = row['Time'].split(" ")
    time_parced = twelve_hour_time_parced[0].split(":")
    
    if twelve_hour_time_parced[1] == "PM" and int(time_parced[0]) < 12:
        time_parced[0] = str(int(time_parced[0]) + 12)
    elif twelve_hour_time_parced[1] == "AM" and int(time_parced[0]) == 12:
        time_parced[0] = str(int(time_parced[0]) - 12)
        
    reformatted_time = '{h}:{m}:{s}'.format(h=time_parced[0],m=time_parced[1],s=time_parced[2])
    return reformatted_time


def LinkIonStats(row, df_ionstats):
    if row['Type'] == 'Detector Measurement' and row['Status'] == 'Done':
        df_ionstats = df_ionstats.dropna(subset=['DetectorVoltage'])
        df_ionstats['TimeDelta'] = abs(row['DateTime'] - df_ionstats['Time'])
        return df_ionstats['TimeDelta'].idxmin()
    else:
        return np.nan
        
    

# ----------------
# read csv files
log_csv_file = 'SystemLog_V5_Filtered_Test1.csv'
sample_csv_file = 'SampleLog_Test_1.csv'
if platform.system() == 'Windows':
    fp1 = os.path.join('C:\\DetectorStressQuantification\\txtFiles', log_csv_file)
    fp2 = os.path.join('C:\\DetectorStressQuantification\\txtFiles', sample_csv_file)
elif platform.system() == 'Linux':
    fp1 = os.path.join('/media/sf_DetectorStressQuantification/txtFiles', log_csv_file)
    fp2 = os.path.join('/media/sf_DetectorStressQuantification/txtFiles', sample_csv_file)
    
df_logfile = pd.read_csv(fp1)
df_samplelog = pd.read_csv(fp2)


# ----------------
# clean log df

# Extract Ion statistics from Detector Measurements
DM_IonStats_Series = df_logfile.apply(parce_DM_IonStatistics, args=("Voltage=.*AreaPerIon=.*",), axis=1)
DM_IonStats_lst = DM_IonStats_Series.tolist()

# Extract Ion statistics from Gain Optimizations
GO_index = df_logfile[(df_logfile['Object'] == "Gain Optimization v5") & (df_logfile['Action'] == "Gain Optimization v5 completed successfully")].index
stringPattern = "Voltage=.*AreaPerIon=.*"
compiled = re.compile(stringPattern)
for i in GO_index:
    x = 1
    while compiled.match(df_logfile['Action'].iloc[i+x])  == None:
        x += 1
    
    
    split_result = df_logfile.at[i+x,'Action'].split(' ')
    DM_IonStats_lst[i+x] = split_result[0].replace('Voltage=','') + ',' + split_result[1].replace('AreaPerIon=','')
    


# Add IonStats list to df_logfile as two new columns: DetectorVoltage , TuneAreaCounts then change type to float64 
temp_df = pd.Series(DM_IonStats_lst).str.split(pat=',', expand=True)
temp_df.columns = ['DetectorVoltage', 'TuneAreaCounts']
df_logfile = pd.concat([df_logfile, temp_df], axis=1)
df_logfile.replace(to_replace='False', value=np.nan, inplace=True)
df_logfile[['DetectorVoltage','TuneAreaCounts']] = df_logfile[['DetectorVoltage','TuneAreaCounts']].astype(dtype='float64')


# converting the time column to datetime dtype
df_logfile['Time'] = pd.to_datetime(df_logfile['Time'], format="%m/%d/%Y %H:%M")


print(df_logfile[['Time', 'Object', 'DetectorVoltage','TuneAreaCounts']].dropna())
exit()




# ----------------
# clean sample df

df_samplelog['Time24'] = df_samplelog.apply(Convert12hrto24hrTime, axis=1)

df_samplelog['DateTime'] = df_samplelog['Date'] + " " + df_samplelog['Time24']
df_samplelog['DateTime'] = pd.to_datetime(df_samplelog['DateTime'], format="%m/%d/%Y %H:%M:%S")



# ----------------
# link ion stats to detector measurement
# df_samplelog['DetectorVoltage'] = np.nan
# df_samplelog['TuneAreaCounts'] = np.nan

# print(df_logfile.dropna(subset=['DetectorVoltage']))

# print(df_samplelog[(df_samplelog['Type'] == 'Detector Measurement') & (df_samplelog['Status'] == 'Done')][['DateTime','Type','DetectorVoltage','TuneAreaCounts']])
test_series = df_samplelog.apply(LinkIonStats, args=(df_logfile.dropna(subset=['DetectorVoltage']),), axis=1)
# print(df_logfile.dropna(subset=['DetectorVoltage']))

# print('\n\n')

# make a pair of lists, then convert the lists to a series and add each as new columns to the df
dv = []
tac = []
for index in test_series.iteritems():
    if not math.isnan(index[1]):
        df_logfile_index = int(index[1])
#         df_samplelog['DetectorVoltage'].iloc[index[0]] = df_logfile.at[df_logfile_index,'DetectorVoltage']
#         df_samplelog['TuneAreaCounts'].iloc[index[0]] = df_logfile.at[df_logfile_index,'TuneAreaCounts']
        dv.append(df_logfile.at[df_logfile_index,'DetectorVoltage'])
        tac.append(df_logfile.at[df_logfile_index,'TuneAreaCounts'])
    else:
        dv.append(np.nan)
        tac.append(np.nan)
        
df_samplelog['DetectorVoltage'] = pd.Series(dv)
df_samplelog['TuneAreaCounts'] = pd.Series(tac)

# print(df_samplelog[df_samplelog['Type'] == "Detector Measurement"])
print(df_logfile[df_logfile['Object'] == 'Gain Optimization v5'][['Time','Action']].head(100))