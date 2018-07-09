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
        # returns a library containing the following pandas data frame & the DataSet_id
        # {'PeakTable':df_PeakTable, 'Sample':df_Sample, 'IDL':df_IDL, 'MS':df_MS, 'GC':df_GC}
        # df_PeakTable - Columns: 
            # 'Name', 'Type', 'Area', 'Height', 'FWHH (s)', 'Similarity',
            # 'RT_1D', 'RT_2D', 'Peak S/N',
            # 'Quant S/N', 'Sample', 'Concentration_pg'
        # df_Sample - Columns:
            # 'index', 'Type', 'Name', 'Status', 'Chromatographic Method',
            # 'MS Method', 'DateTime', 'DataSet', 'Instrument', 'DetectorVoltage', 'AreaPerIon'
        # df_IDL - Columns:
            # 'Concentration', 'IDL'
        # df_GC - Columns:
            # 'GC_Method_id', 'SplitRatio', 'Chromatography', 'RunTime_min'
        # df_MS - Columns:
            # 'MS_Method_id', 'AcquisitionRate', 'MassRange_Bottom', 'MassRange_Top',
            # 'ExtractionFrequency', 'DetectorOffset_Volts',
       
        
        
        # From the CWD get all the csv files and place them into the csv_dict
        csv_dict = self.get_csv_filepaths()
        
        # read SampleLog csv and preform prelimary data cleaning
        sample_log_filepath = os.path.join(self.csvDirectory, csv_dict['SampleLog'])
        df_samplelog = self.extract_sample_log_data(sample_log_filepath)

        
        # read InstrumentLog csv and preform prelimary data cleaning
        instrument_log_filepath = os.path.join(self.csvDirectory, csv_dict['InstrumentLog'])
        df_instrumentlog = self.extract_instrument_log_data(instrument_log_filepath)
        
        # Add the ion stastics (detector voltage & AreaPerIon) from the df_instrumentlog to the 
        # corresponding row in the df_samplelog, below is a description of how that's done
        
            # Ion_Stats_linking_index_series: index matches that of the df_samplelog, 
            # and the the values are the corresponding index in the df_instrumentlog which have the ion stats data.
            # If a given row in the df_samplelog has no ion stats the value will be be np.nan 
        Ion_Stats_linking_index_series = df_samplelog.apply(self.IonStatsIndexLinking, args=(df_instrumentlog.copy(),), axis=1)
        df_Sample = self.InsertIonStatsIntoSampleLogDF(Ion_Stats_linking_index_series, df_samplelog, df_instrumentlog)
       
        # Extract peak table data from peak table csv files list and obtain SetName
        df_PeakTable, DataSet = self.extract_PeakTable_data(csv_dict['PeakTable'])
        
        # The df_Sample['Type'] == 'Detector Measurement' and df_Sample['Type'] == 'Gain Optimization' are not labeled with the 
        # DataSet name because the data set is generated from the Sample name (example: 'Alk_+000v_a L2-0.025 pg/uL Split 5-1 (5 fg on Col) BT-PV2 1D:3')
        # Using the df_Sample['Type'] == 'Source and Analyzer Focus' as bracketing rows around the df_Sample['Type'] == 'Sample' which do have 
        # a DataSet label, the  df_Sample['Type'] == 'Detector Measurement' and df_Sample['Type'] == 'Gain Optimization' will be properly labeled.
        df_Sample['DataSet'] = self.LabelDataSet_DM_GO(DataSet, df_Sample)
        
        # Drop all rows from the df_Sample that are not part of the DataSet
        # Then only retain injected samples (not blanks),
        df_Sample = self.SampleDF_Hygene(DataSet, df_Sample)
#         print(df_Sample[['Name','DetectorVoltage','AreaPerIon']].head(150))

        # Get read method data & IDL data
        df_GC, df_MS, df_IDL  = self.extract_Method_IDL_data(csv_dict['GC_Method'], csv_dict['MS_Method'], csv_dict['idl'])
        
        return {'PeakTable':df_PeakTable, 'Sample':df_Sample, 'IDL':df_IDL, 'MS':df_MS, 'GC':df_GC}, DataSet
    
    def extract_Method_IDL_data(self, GCFilePath, MSFilePath, IDLFilePath):
        # Returns the method & IDL data as pandas DataFrames, if the file(s) are not present then returns None
        
        csvdata = []
        
        for f in [GCFilePath, MSFilePath, IDLFilePath]:
            if f == None:
                # IF the file was not initially found in the get_csv_filepaths
                csvdata.append(None)
            else:
                # Probably a bit redundant...
                # The conditional below verifies that the file path is valid before reading the csv into pandas
                fp = os.path.join(self.csvDirectory,f)
                if os.path.isfile(fp):
                    csvdata.append(pd.read_csv(fp))
                else:
                    csvdata.append(None)
                
        return csvdata[0], csvdata[1], csvdata[2]
    
    def extract_PeakTable_data(self, PeakTablecsvFilelst):
        
        # read all Peak Table csvs and concat into a single dataframe
        df = pd.DataFrame()
        for csvFilePath in PeakTablecsvFilelst:
            df_temp = self.readPeakTablecsvFile(os.path.join(self.csvDirectory,csvFilePath))
            df = pd.concat([df, df_temp], axis=0)
        
        # Extract the DataSet name and concentration from Sample column creating 2 new columns
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
        
        # Since TAF data is not paired with a library hit all Similarity Scores for TAF data is set = 0
        df['Similarity'] = df.apply(self.ZeroTAFSimilarity, axis=1)
        
        # Rename the R.T. columns
        df = df.rename(index=str, columns={'1st Dimension Time (s)':'RT_1D', '2nd Dimension Time (s)':'RT_2D'})
               
        return df,ds[0]
    
    def ZeroTAFSimilarity(self,row):
        # If a given analyte was found using TAF the similarity value will be set to 0
        # otherwise the original similarity score will be retained
        if row['Type'] == 'Target':
            return 0
        elif row['Type'] == 'Unknown':
            return row['Similarity']
        else:
            raise Exception("When setting TAF analytes' similarity scores to 0 an unexpected analyte type was encountered. Sample name - {sn}".format(sn=row['Sample']))
            
    
    def readPeakTablecsvFile(self, csvFile):
        # Reads the the csv file returns the resulting DataFrame
        try:
            df =  pd.read_csv(csvFile, encoding="Latin-1")
            return df
        except pd.errors.EmptyDataError:  # @UndefinedVariable
            return pd.DataFrame()
        except pd.errors.ParserError: # @UndefinedVariable
            raise Exception('This csv file is fucked up. You might want to re-export it dude.', csvFile)
    
    def extract_instrument_log_data(self, Instrument_Log_file_path):
        df = pd.read_csv(Instrument_Log_file_path)
        
        # Extract Ion statistics from Detector Measurement rows
        DM_IonStats_Series = df.apply(self.parce_DM_IonStatistics, axis=1)
        IonStats_lst = DM_IonStats_Series.tolist()
        
        # Extract Ion statistics from Gain Optimization rows
        # First the indices for each row that indicates a GO has completed are extracted
        # Next, for each GO completion index the rows immediately following are checked for the string patter show below,
        # this string pattern corresponds with the GO row that has the final ion statistics for that particular GO.
        # However, when an offset is used the final ion statistics will be WITH the offset not the GO optimized
        # to a TuneAreaCount of 110.  As such each matching patter will be checked that the TuneAreaCount = 110 (+/- 4)
        # The matching pattern will be within several rows of the GO completion row
        
        GO_index = df[(df['Object'] == "Gain Optimization v5") & (df['Action'] == "Gain Optimization v5 completed successfully")].index
        stringPattern = "Voltage=.*AreaPerIon=.*"
        compiled = re.compile(stringPattern)
        
        # Iterates through the GO completion index looking through the following rows for a pattern match 
        for i in GO_index:
            x = 1
            GO_IonStats_Found = False
            
            while not GO_IonStats_Found:
                
                if not compiled.match(df['Action'].iloc[i+x])  == None:
                    # If a match is found check if the TuneAreaCount is within the 110 (+/- 4) threshold
                    split_result = df.at[i+x,'Action'].split(' ') # example: ['Voltage=2283.800000', 'AreaPerIon=446.952505']
                    DetectorVoltage_comma_AreaPerIon_str = split_result[0].replace('Voltage=','') + ',' + split_result[1].replace('AreaPerIon=','')
                    # Example: 2283.800000,446.952505
                    DetectorVoltage_AreaPerIon_lst = DetectorVoltage_comma_AreaPerIon_str.split(',') # Example: ['2283.800000', '446.952505']
                    API = float(DetectorVoltage_AreaPerIon_lst[1]) # Example: 446.952505
                    
                    if API >= 106 and API <= 114:
                        # Once the matching index is found, the ion statistics are extracted 
                        # from the string and inserted into the 
                        # Ion stats list at the corresponding index.  
                        # It is added in a comma delimited string format
                        IonStats_lst[i+x] = DetectorVoltage_comma_AreaPerIon_str
                        GO_IonStats_Found = True
                
                # If not a match add 1 to x so the next row can be checked
                if GO_IonStats_Found == False: x += 1
        
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
                csvFiledict[f[:-4]] = None
                
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
    
    def LabelDataSet_DM_GO(self, DataSetLabel, df_sample):
        # Note, at this point the only the data samples are properly labeled with the DataSet Label
        # This function uses the starting and ending indices of those samples as starting places to 
        # locate the 'Source and Analyzer Focus' that is the starting point of the DataSet & 'Source and Analyzer Focus'
        # that is the starting place for the next set.  Use those indices to properly label the GOs & DMs within the DataSet
        # Note, if the current DataSetLabel is the last set in the df_sample then an IndexError will be thrown, this 
        # except will be caught
        
        DataSet_lst = df_sample['DataSet'].tolist()
        
        # This will get me the first and last index for the data set
        df_sliced = df_sample[df_sample['DataSet'] == DataSetLabel]
        sample_index_brackets = df_sliced.iloc[[0,-1],].index
        
        # Runs through the recursion loop twice
        # The first time it begins iterating from the first (lowest) index in the DataSet and counts backward (down)
        # Next time it begins iterating from the first (lowest) index in the DataSet and counts forward (up)
        # Its looking for the index with the first occurrence where df_sample['Type'] == 'Source and Analyzer Focus' in both recursion loops.
        # The two resulting indices will bracket the beginning and end of the DataSet
        SAF_index_brackets = []
        for n, start_index in enumerate(sample_index_brackets):
            Found_Source_and_Analyzer_Focus = False
            i = 0
            while not Found_Source_and_Analyzer_Focus:
                # If were checking from the 1st index in the data set we want to count backwards
                # if were checking for the last index in the data set we want to count forwards
                if n == 0:
                    i -= 1 
                elif n == 1:
                    i += 1
                
                check_index = start_index + i
                
                try:
                    if df_sample['Type'].iloc[check_index] == 'Source and Analyzer Focus':
                        SAF_index_brackets.append(check_index)
                        Found_Source_and_Analyzer_Focus = True
                except IndexError:
                    SAF_index_brackets.append(check_index)
                    Found_Source_and_Analyzer_Focus = True
                    
        for n in range(SAF_index_brackets[0], SAF_index_brackets[1]):
            DataSet_lst[n] = DataSetLabel
        
        return pd.Series(DataSet_lst)
    
    def SampleDF_Hygene(self, DataSetLabel, df_sample):
        # Drop all rows from the df_Sample that are not part of the DataSet
        # Then only retain injected samples (not blanks),
        
        # Slice DF to retain members of the DataSet only that are either  Samples, GO, or DM
        df_sample = df_sample[(df_sample['DataSet'] == DataSetLabel) & ((df_sample['Type'] == 'Sample') | (df_sample['Type'] == 'Gain Optimization') | (df_sample['Type'] == 'Detector Measurement'))].copy()
    
        # From the remaining rows delete all samples that are Blanks
        stringPattern = ".*Blank.*"
        f = df_sample['Name'].str.contains(stringPattern)
        df_sample = df_sample[~f].copy()
        
        # Convert DateTime to a object(string)
#         df_sample['DateTime'] = df_sample['DateTime'].strftime("%m/%d/%Y %H:%M:%S")
        df_sample['DateTime'] = df_sample['DateTime'].apply(lambda t: t.strftime("%m/%d/%Y %H:%M:%S"))
        
        # reindex the df and return the result
        return df_sample.reset_index()
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    