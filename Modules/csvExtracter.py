# -*- coding: Latin-1 -*-
import os
import pandas as pd
import datetime

class Extract():
    
    def __init__(self, csvDirectory):
        
        self.csvDirectory = csvDirectory
        
    def extract_csv_data(self):
        # returns a data structure that is formatted for SQL uploading
        csv_dict = self.get_csv_filepaths()
        
        # read sample log dict and preform prelimary cleaning
        sample_log_filepath = os.path.join(self.csvDirectory, csv_dict['SampleLog'])
        df_samplelog = self.extract_sample_log_data(sample_log_filepath)
        print(df_samplelog['ParcedName'].head(100))
    
    def extract_sample_log_data(self, Sample_Log_file_path):
        # reads the sample log csv file
        # converts the date & time columns to a single column which is data time formatted
        # From the Name column creates both Instrument & DataSet Columns
        # then drops unnecessary columns
        
        df = pd.read_csv(Sample_Log_file_path)
        df['Time24'] = df.apply(self.Convert12hrto24hrTime, axis=1)

        df['DateTime'] = df['Date'] + " " + df['Time24']
        df['DateTime'] = pd.to_datetime(df['DateTime'], format="%m/%d/%Y %H:%M:%S")
        
        df.drop(columns=['QC Method', 'Folder', 'Date', 'Time', 'Time24', 'Vial', 'AS Method', 'Update Calibration', 'Cal. Std. to Replace','DP Method','Unnamed: 0'], inplace=True)
        df['ParcedName'] = df.apply(self.ParceSampleName, axis=1)
        
        return df[(df['Type'] == 'Sample') | (df['Type'] == 'Gain Optimization') | (df['Type'] == 'Detector Measurement')].copy()
    
    def get_csv_filepaths(self):
        # get a list of csv files in the defined directory
        # return a dictionary where the key is the csv file type & the value is
        # the csv file name.  If a particular type is not found in the CWD then None
        # will be the correspoinding value.
        
        csvFileslst = self.find_csv_filenames_remove_nonASCII()
        csvFileTypes = ['GC_Method.csv','idl.csv','InstrumentLog.csv','MS_Method.csv','SampleLog.csv']
        csvFiledict = {}
        
        for f in csvFileTypes:
            if f in csvFileslst:
                csvFileslst.remove(f)
                csvFiledict[f[:-4]] = f
            else:
                csvFiledict[f[:-4]] = False
                
        csvFiledict['PeakTable'] = csvFileslst
        
        return csvFiledict
        
        
    def find_csv_filenames_remove_nonASCII(self, suffix=".csv"):
        # This methods finds all csv files in the defined directory
        # If any of the filenames contain a µ character replace with an _
        # Return a list of all csv file names
        
        filenames = os.listdir(self.csvDirectory)
        filenames_fixed = []
        for filename in filenames:
            if filename.endswith(suffix) and 'µ' in filename:
                new_filename = filename.replace('µ', '_')
                os.rename(os.path.join(self.csvDirectory, filename), 
                    os.path.join(self.csvDirectory, new_filename))
                filenames_fixed.append(new_filename)
                
            elif filename.endswith(suffix):
                filenames_fixed.append(filename)
                
        return filenames_fixed 
    
    def Convert12hrto24hrTime(self, row):
        twelve_hour_time_parced = row['Time'].split(" ")
        time_parced = twelve_hour_time_parced[0].split(":")
        
        if twelve_hour_time_parced[1] == "PM" and int(time_parced[0]) < 12:
            time_parced[0] = str(int(time_parced[0]) + 12)
        elif twelve_hour_time_parced[1] == "AM" and int(time_parced[0]) == 12:
            time_parced[0] = str(int(time_parced[0]) - 12)
            
        reformatted_time = '{h}:{m}:{s}'.format(h=time_parced[0],m=time_parced[1],s=time_parced[2])
        return reformatted_time
    
    def ParceSampleName(self, row):
        # Example of Sample name format
        # Alk_+000v_a L2-0.025 pg/uL Split 5-1 (5 fg on Col) BT-PV2 1D:3
        
        if row['Type'] == 'Sample' and isinstance(row['Name'], str):
            # need to break up conditinoal because cannot do the below evaluation unless row['Name'] is a string
            if not 'blank' in row['Name'].lower():
                try:
                    name_split = row['Name'].split(' ')
                    SetPerfix = name_split[0] # Gets  = 'Alk_+000v_a'
                    instrument_name = name_split[-2].split('-')[-1] # Gets = 'PV2'
                    
                    if instrument_name == 'PV1' or instrument_name == 'PV2':
#                         return 'False,False'
                    
                        return SetPerfix + '_' + instrument_name + ',' + instrument_name
                except IndexError:
                    return 'False,False'
                            
        return 'False,False'
        
    
    
    