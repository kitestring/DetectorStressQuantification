import platform
import os
import pandas as pd
import datetime

def Convert12hrto24hrTime(row):
    twelve_hour_time_parced = row['Time'].split(" ")
    time_parced = twelve_hour_time_parced[0].split(":")
    
    if twelve_hour_time_parced[1] == "PM" and int(time_parced[0]) < 12:
        time_parced[0] = str(int(time_parced[0]) + 12)
    elif twelve_hour_time_parced[1] == "AM" and int(time_parced[0]) == 12:
        time_parced[0] = str(int(time_parced[0]) - 12)
        
    reformatted_time = '{h}:{m}:{s}'.format(h=time_parced[0],m=time_parced[1],s=time_parced[2])
    return reformatted_time

csv_file = 'SampleLog_Test_1.csv'
if platform.system() == 'Windows':
    fp = os.path.join('C:\\DetectorStressQuantification\\txtFiles', csv_file)
elif platform.system() == 'Linux':
    fp = os.path.join('/media/sf_DetectorStressQuantification/txtFiles', csv_file)
    
df_samplelog = pd.read_csv(fp)

df_samplelog['Time24'] = df_samplelog.apply(Convert12hrto24hrTime, axis=1)

df_samplelog['DateTime'] = df_samplelog['Date'] + " " + df_samplelog['Time24']
df_samplelog['DateTime'] = pd.to_datetime(df_samplelog['DateTime'], format="%m/%d/%Y %H:%M:%S")

# print(df_samplelog[df_samplelog['Type'] == 'Detector Measurement'][['Date','Time','Type']])
# print(df_samplelog.info())

d1 = datetime.datetime.now()

df_samplelog['TimeDelta'] = d1 - df_samplelog['DateTime']

print(df_samplelog[df_samplelog['TimeDelta'] == df_samplelog['TimeDelta'].min()])
print()
print(df_samplelog['TimeDelta'].idxmin())