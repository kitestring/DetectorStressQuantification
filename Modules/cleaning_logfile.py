import platform
import os
import pandas as pd
import re
import numpy as np

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

csv_file = 'SystemLog_V5_Filtered_Test1.csv'
if platform.system() == 'Windows':
    fp = os.path.join('C:\\DetectorStressQuantification\\txtFiles', csv_file)
elif platform.system() == 'Linux':
    fp = os.path.join('/media/sf_DetectorStressQuantification/txtFiles', csv_file)
    
df_logfile = pd.read_csv(fp)
DM_IonStats_Series = df_logfile.apply(parce_DM_IonStatistics, args=("Voltage.*",), axis=1)

temp_df = DM_IonStats_Series.str.split(pat=',', expand=True)
temp_df.columns = ['DetectorVoltage', 'TuneAreaCounts']
df_logfile = pd.concat([df_logfile, temp_df], axis=1)
df_logfile.replace(to_replace='False', value=np.nan, inplace=True)
df_logfile[['DetectorVoltage','TuneAreaCounts']] = df_logfile[['DetectorVoltage','TuneAreaCounts']].astype(dtype='float64')

# converting the time column to datetime dtype
df_logfile['Time'] = pd.to_datetime(df_logfile['Time'], format="%m/%d/%Y %H:%M")

print(df_logfile.info())