# -*- coding: Latin-1 -*-
import os
import pandas as pd
import datetime
import re
import pprint
import numpy as np
import math

class Extract():
    
    def __init__(self, csvDirectory):
        
        self.csvDirectory = csvDirectory
        
    def extract_csv_data(self):
        # returns a data structure that is formatted for SQL uploading
        
        # From the CWD get all the csv files and place them into the csv_dict
        csv_dict = self.get_csv_filepaths()
        
        # read Sample csv and preform prelimary data cleaning
        
        
        
        
        
        # Fuck, how do I label the GO's and the DM's
        # Don't really want to do the based upon the time stamp but I'm not coming up with anything else.
        
        
        
        sample_log_filepath = os.path.join(self.csvDirectory, csv_dict['SampleLog'])
        df_samplelog = self.extract_sample_log_data(sample_log_filepath)
        
        # read InstrumentLog csv and preform prelimary data cleaning
        instrument_log_filepath = os.path.join(self.csvDirectory, csv_dict['InstrumentLog'])
        df_instrumentlog = self.extract_instrument_log_data(instrument_log_filepath)
        
        # Add the ion stastics (detector voltage & AreaPerIon) from the df_instrumentlog to the 
        # corresponding row in the df_samplelog
        
        # Ion_Stats_linking_index_series = index will match that of the df_samplelog, 
        # and the the values are the corresponding index in the df_instrumentlog which have the ion stats data.
        # If a given row in the df_samplelog has no ion stats the value will be be np.nan 
        Ion_Stats_linking_index_series = df_samplelog.apply(self.IonStatsIndexLinking, args=(df_instrumentlog.copy(),), axis=1)
        df_Sample = self.InsertIonStatsIntoSampleLogDF(Ion_Stats_linking_index_series, df_samplelog, df_instrumentlog)
       
        # Extract peak table data from peak table csv file list and obtain SetName
        df_PeakTable, DataSet = self.extract_PeakTable_data(csv_dict['PeakTable'])
       
        # Drop all rows from the df_Sample that are not part of the DataSet
#         df_Sample = df_Sample[df_Sample[]]
        df_Sample = df_Sample[df_Sample['DataSet'] == DataSet].copy()
        print(df_Sample[['Name','AreaPerIon']].head(100))
            
    
    def extract_PeakTable_data(self, PeakTablecsvFilelst):
        
        # read all Peak Table csvs and concat into a single dataframe
        df = pd.DataFrame()
        for csvFilePath in PeakTablecsvFilelst:
            df_temp = self.readPeakTablecsvFile(os.path.join(self.csvDirectory,csvFilePath))
            df = pd.concat([df, df_temp], axis=0)
        
        # Extract the DataSet name, instrument name, and concentration from Sample column creating 3 new columns
        Series_ParcedSample = df.apply(self.ParcePeakTableSampleName, axis=1)
        df_ParcedSample_Split = Series_ParcedSample.str.split(pat=',', expand=True)
        df_ParcedSample_Split.columns = ['DataSet','Concentration_pg']
        df = pd.concat([df, df_ParcedSample_Split], axis=1)
        df['Concentration_pg'] = df['Concentration_pg'].astype(dtype='float64')
        
        # Check for multiple data sets in the list of Peak table csv files
        ds = df['DataSet'].unique()
        if len(ds) > 1:
            raise Exception('The set of peak table csv files contains multiple data sets')
        
        df.drop(columns=['DataSet'], inplace=True)
               
        return df,ds[0]
    
    def readPeakTablecsvFile(self, csvFile):
        # Reads the the csv file returns the resulting DataFrame
        try:
            df =  pd.read_csv(csvFile, encoding="Latin-1")
            return df
        except pd.errors.EmptyDataError:  # @UndefinedVariable
            return pd.DataFrame()
        except pd.errors.ParserError: # @UndefinedVariable
            raise Exception('This csv file is fucked up. You migh want to re-export it dude.', csvFile)
    
    def extract_instrument_log_data(self, Instrument_Log_file_path):
        df = pd.read_csv(Instrument_Log_file_path)
        
        # Extract Ion statistics from Detector Measurement rows
        DM_IonStats_Series = df.apply(self.parce_DM_IonStatistics, axis=1)
        IonStats_lst = DM_IonStats_Series.tolist()
        
        # Extract Ion statistics from Gain Optimization rows
        # First the indices for each row that indicates a GO has completed are extracted
        # Next, for each GO completion index the rows immidately following are checked for the string patter show below,
        # this string pattern corresponds with the GO row that has the final ion statistics for that particular GO.
        # The matching pattern will be within several rows of the GO completion row
        
        GO_index = df[(df['Object'] == "Gain Optimization v5") & (df['Action'] == "Gain Optimization v5 completed successfully")].index
        stringPattern = "Voltage=.*AreaPerIon=.*"
        compiled = re.compile(stringPattern)
        
        # Iterates through the GO completion index looking through the following rows for a pattern match 
        for i in GO_index:
            x = 1
            while compiled.match(df['Action'].iloc[i+x])  == None:
                x += 1
        
            # Once the matching index is found, the ion statistics are extracted from the string and inserted into the 
            # Ion stats list at the corresponding index.  It is added in a comma delimited format
            split_result = df.at[i+x,'Action'].split(' ')
            IonStats_lst[i+x] = split_result[0].replace('Voltage=','') + ',' + split_result[1].replace('AreaPerIon=','')
        
        
        # Add IonStats list to df_logfile as two new columns: DetectorVoltage , AreaPerIon then change type to float64 
        IonStats_df = pd.Series(IonStats_lst).str.split(pat=',', expand=True)
        IonStats_df.columns = ['DetectorVoltage', 'AreaPerIon']
        df = pd.concat([df, IonStats_df], axis=1)
        df.replace(to_replace='False', value=np.nan, inplace=True)
        df[['DetectorVoltage','AreaPerIon']] = df[['DetectorVoltage','AreaPerIon']].astype(dtype='float64')
        
        # Drop all rows that do not have ion statistics data
        df.dropna(subset=['DetectorVoltage'], inplace=True)
        
        # Drop all columns from the DataFrame that are not needed
        df.drop(columns=['Name','Details','User','Action'], inplace=True)
        
        # Convert the time column to datetime dtype
        df['Time'] = pd.to_datetime(df['Time'], format="%m/%d/%Y %H:%M")
        
        return df
        
        
    def extract_sample_log_data(self, Sample_Log_file_path):
        # reads the sample log csv file
        # converts the date & time columns to a single column which is data time formatted
        # From the Name column creates both Instrument & DataSet Columns
        # then drops unnecessary columns
        
        df = pd.read_csv(Sample_Log_file_path)
        
        # Convert time to 24 hour clock, combine date & time columns then convert dtype to datetime
        df['Time24'] = df.apply(self.Convert12hrto24hrTime, axis=1)
        df['DateTime'] = df['Date'] + " " + df['Time24']
        df['DateTime'] = pd.to_datetime(df['DateTime'], format="%m/%d/%Y %H:%M:%S")
        
        # Extract the DataSet name and instrument name from Name column creating 2 new columns       
        df['ParcedName'] = df.apply(self.ParceSampleLogName, axis=1)
        df_ParcedName_split = df['ParcedName'].str.split(pat=',', expand=True)
        df_ParcedName_split.columns = ['DataSet','Instrument']
        df = pd.concat([df, df_ParcedName_split], axis=1)
        
        # Drop all columns from the DataFrame that are not needed (won't work in windows because I can install this shit)
        df.drop(columns=['QC Method', 'Folder', 'Date', 'Time', 'Time24', 'Vial', 'AS Method', 'Update Calibration', 'Cal. Std. to Replace','DP Method','Unnamed: 0','ParcedName'], inplace=True)
        
        return df.copy()
    
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
    
    def ParceSampleLogName(self, row):
        # Returns the Set name and instrument comma delimited
        # Example of Sample name format
        # Alk_+000v_a L2-0.025 pg/uL Split 5-1 (5 fg on Col) BT-PV2 1D:3
        
        if row['Type'] == 'Sample' and isinstance(row['Name'], str):
            # need to break up conditional because cannot do the below evaluation unless row['Name'] is a string
            if not 'blank' in row['Name'].lower():
                try:
                    name_split = row['Name'].split(' ')
                    SetPerfix = name_split[0] # Gets  = 'Alk_+000v_a'
                    instrument_name = name_split[-2].split('-')[-1] # Gets = 'PV2'
                    
                    if instrument_name == 'PV1' or instrument_name == 'PV2':
                        return SetPerfix + '_' + instrument_name + ',' + instrument_name
                    
                except IndexError:
                    return 'False,False'
                            
        return 'False,False'
    
    def ParcePeakTableSampleName(self, row):
        # Returns the Set name, and concentration in pg comma delimited
        # Example of Sample name format
        # Alk_+000v_a L2-0.025 pg/uL Split 5-1 (5 fg on Col) BT-PV2 1D:3
        
        
        try:
            name_split = row['Sample'].split(' ')
            SetPerfix = name_split[0] # Gets  = 'Alk_+000v_a'
            instrument_name = name_split[-2].split('-')[-1] # Gets = 'PV2'
            conc = self.string_to_concentration(row['Sample'])
            
            return SetPerfix + '_' + instrument_name + ',{c}'.format(c=conc)
        
        except IndexError:
            return 'False,False,False'
        
    def string_to_concentration(self, str):
        # Example sample name 'Alk_+000v_a L2-0.025 pg/uL Split 5-1 (5 fg on Col) BT-PV2 1D:3'
        # This method gets the concentration value to the right of the "("
        # Then converts that value to pg.
        
        metricdict = {'fg': 0.001, 'pg': 1, 'ng': 1000}
        str_index = str.index('(') + 1
        concentration_lst = str[str_index:].split(' ')
        return float(concentration_lst[0]) * metricdict.get(concentration_lst[1], 0)
        
    
    
    def parce_DM_IonStatistics(self, row):
        # For each detector measurement this function will return a string with the detector voltage and area per ion comma delimited
        # All columns which do not have the detector measurement ion statistics information will return "False,False"
            # row in the df row
            # stringPattern is the re formatted string pattern that will be matched
        
        # Regular expression pattern below will be used to find each Detector Measurement row that contains ion statistics 
        stringPattern = "Voltage=.*AreaPerIon=.*"
        compiled = re.compile(stringPattern)
        
        # Alogorithm is only applied to DM realated rows
        # Looking for DM rows where the Action column begins with Voltage=
        if row['Object'] == "Detector Measurement v5":
            
            if type(row['Action']) is str:
                result = compiled.match(row['Action'])
                if result != None:
                    split_result = result.group(0).split(' ')
                    return split_result[0].replace('Voltage=','') + ',' + split_result[1].replace('AreaPerIon=','')
                
        # Action column is not a string or does not match the pattern does not contain the ion statistics I'm looking for
        return "False,False"
    
    def printDataStructure(self, ds):
        pp = pprint.PrettyPrinter(indent=4,width=200,depth=20)
        pp.pprint(ds)
        
    def IonStatsIndexLinking(self, row, df_ionstats):
        # Returns df_ionstats index with the minimum time delta from the time difference between the Sample Log row and Ion Stats row is.
        # This logic is used because corresponding entries in the sample log and the instrument log will have slightly (but very similar time stamps).
        # Finding the minimum time delta will likely link corresponding events from both logs.  The exception is if the corresponding event in the 
        # Instrument has been truncated out of the log.  In these cases the minimum time difference will > several minutes, possibly even days because the true
        # link is gone from the instrument.
         
        # Only allows Detector Measurement or Gain Optimization rows whose status is Done
        if (row['Type'] == 'Detector Measurement' or row['Type'] == 'Gain Optimization') and row['Status'] == 'Done':
            
            # For the given row time stamp, a new column is created with the Time delta for 
            df_ionstats['TimeDelta'] = abs(row['DateTime'] - df_ionstats['Time'])
            
            # If the smallest time difference is more than 3 minutes than the data is missing
            # from the logfile and this alogrithm will provide the wrong index link
            # The following conditional statement prevents this.
            if df_ionstats['TimeDelta'].min() > datetime.timedelta(minutes=3):
                return np.nan
            else:
                return df_ionstats['TimeDelta'].idxmin()
        else:
            return np.nan
        
        
    def InsertIonStatsIntoSampleLogDF(self, Stats_Index_Series, df_SL, df_IL):
        # Uses the Stats_Index_Series:
            # Stats_Index_Series = index will match that of the df_samplelog, 
            # and the the values are the corresponding index in the df_instrumentlog which have the ion stats data.
            # If a given row in the df_samplelog has no ion stats the value will be be np.nan
        # To insert the ion stats data from the df_instrumentlog 
        # Into the df_samplelog
        # The resulting df is returned
            # df_SL = df_samplelog
            # df_IL = df_instrumentlog
        
        # The lists below will be converted to a series and added to the returned df
            # The detectorvoltage data will be appended to dv
            # The AreaPerIon data will be appended to tac
        dv = []
        tac = []
        for index in Stats_Index_Series.iteritems():
            if not math.isnan(index[1]):
                df_logfile_index = int(index[1])
                dv.append(df_IL.at[df_logfile_index,'DetectorVoltage'])
                tac.append(df_IL.at[df_logfile_index,'AreaPerIon'])
            else:
                dv.append(np.nan)
                tac.append(np.nan)
        
        df = df_SL.copy()
              
        df['DetectorVoltage'] = pd.Series(dv)
        df['AreaPerIon'] = pd.Series(tac)
        
        return df