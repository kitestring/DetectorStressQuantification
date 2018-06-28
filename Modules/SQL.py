import pandas as pd
import psycopg2

class Postgres():
    
    def __init__(self, db_name, Postgres_user, Postgres_pw):
        
        self.conn_psql = psycopg2.connect(dbname=db_name, user=Postgres_user, host="/tmp/", password=Postgres_pw)
        self.cur_psql= self.conn_psql.cursor()
    
    def DataUploadTest(self):
        table = 'IDL'
#         row = ("DEFAULT", 0.415, 0.002)
        self.cur_psql.execute("INSERT INTO %s VALUES (DEFAULT, %s) RETURNING %s;" % (table, '0.0415, 0.003', 'IDL_id'))
        IDL_id_row =  self.cur_psql.fetchall()
        return IDL_id_row[0][0]
    
    def QueryTest(self):
        table = 'IDL'
        self.cur_psql.execute("SELECT * FROM IDL")
        IDL_id_row =  self.cur_psql.fetchall()
        print(IDL_id_row)
    
    def IsMethodUnique(self, MethodType, MethodID):
        # MethodType must be GC or MS only
        if MethodType != 'GC' and  MethodType != 'MS':
            raise Exception('MethodType must be "GC" or "MS" only.')
        
        tab = MethodType + "_Method"
        col = tab + '_id'
        
        # Uncomment to print the query
        # print("SELECT {c1} FROM {t} WHERE {c2} = '{i}';".format(c1=col,t=tab,c2=col,i=MethodID))
        self.cur_psql.execute("SELECT {c1} FROM {t} WHERE {c2} = '{i}';".format(c1=col,t=tab,c2=col,i=MethodID))
        result = self.cur_psql.fetchall()
        
        if result == []:
            return True
        else:
            return False
            
    def UploadData(self, DF_Dict, DataSet_id):
        # Load IDL row and get IDL_id which is the primary key
        # this will be used as a foreign key value for the DataSet table
        IDL_row_values_as_list = DF_Dict['IDL'][['IDL','Concentration']].iloc[0].tolist()
        IDL_row_id = self.UploadTableRow_ReturnSerialID('IDL', IDL_row_values_as_list, 'IDL_id')
        
        # If extracted and not already in the DB Load the GC & MS method
        if type(DF_Dict['GC']) is pd.core.frame.DataFrame:
            if self.IsMethodUnique('GC', DF_Dict['GC']['GC_Method_id'].iloc[0]):
                self.UploadTableRow('GC_Method', DF_Dict['GC'].iloc[0].tolist())
            
        if type(DF_Dict['MS']) is pd.core.frame.DataFrame:
            if self.IsMethodUnique('MS', DF_Dict['MS']['MS_Method_id'].iloc[0]):
                self.UploadTableRow('MS_Method', DF_Dict['MS'].iloc[0].tolist())
                
        # Transform then load DataSet table data
        DataSetlst = self.TransfromDataSetData(DF_Dict['Sample'], DataSet_id, IDL_row_id)
        self.UploadTableRow('DataSet', DataSetlst)
        
        # Load IonStats row and get IonStats_id which is the primary key
        # this will be used as a foreign key value for the Sample table
        DF_Dict['Sample']['IonStats_id'] = self.LoadIonsStats(DF_Dict['Sample'])
        
        # Upload Sample table data and PeakTable data
        self.LoadSampleAndPeakTableData(DF_Dict['Sample'], DF_Dict['PeakTable'], DataSet_id)
        
    def LoadSampleAndPeakTableData(self, df_Sample, df_PeakTable, DataSet_id):
        
        for sr in df_Sample.iterrows():
            DateTimeStamp = sr[1]['DateTime']
            t = sr[1]['Type']
            name = sr[1]['Name']
            IonStats_id = sr[1]['IonStats_id']
            row_list = [DateTimeStamp, t, name, IonStats_id, DataSet_id]
            Sample_id = self.UploadTableRow_ReturnSerialID('Sample', row_list, 'Sample_id')
            
            # Load the PeakTable values that link to this sample 
            df_PeakTable_sliced = df_PeakTable[df_PeakTable['Sample'] == name].copy()
            self.LoadPeakTableData(df_PeakTable_sliced, Sample_id)
            
    def LoadPeakTableData(self, df_PeakTable_sliced, Sample_id):
        # df_PeakTable - Columns: 
            # 'Name', 'Type', 'Area', 'Height', 'FWHH (s)', 'Similarity',
            # 'RT_1D', 'RT_2D', 'Peak S/N',
            # 'Quant S/N', 'Sample', 'Concentration_pg'
            
        for pr in df_PeakTable_sliced.iterrows():
            Anal = pr[1]['Name']
            DP = pr[1]['Type']
            A = pr[1]['Area']
            H = pr[1]['Height']
            F = pr[1]['FWHH (s)']
            S = pr[1]['Similarity']
            RT1 = pr[1]['RT_1D']
            RT2 = pr[1]['RT_2D']
            PSN = pr[1]['Peak S/N']
            QSN = pr[1]['Quant S/N']
            C = pr[1]['Concentration_pg']
            PeakTablelist = [Anal,DP,A,H,F,S,RT1,RT2,PSN,QSN,C,Sample_id]
            PeakTable_id = self.UploadTableRow_ReturnSerialID('PeakTable', PeakTablelist, 'PeakTable_id')
            
    
    def LoadIonsStats(self, df_Sample):
        # Loads IonStats data and returns a pd.Series to append to the DF_Dict['Sample']
        # that will contain the keys the links the IonStats table & the Sample table
        IonStats_id_foreignkeys = []
        
        # Iterates over each row in the df. Each row where the type is a DM or GO
        # Upload the ionstats data and append the returned IonStats_id to the IonStats_id_foreignkeys list
        # For every other row append None IonStats_id_foreignkeys list
        
        for r in df_Sample.iterrows():
            t = r[1]['Type']
            if t == "Detector Measurement" or t == "Gain Optimization":
                DetectorVoltage = float(r[1]['DetectorVoltage'])
                AreaPerIon = float(r[1]['AreaPerIon'])
                IonStats_id_foreignkeys.append(self.UploadTableRow_ReturnSerialID('IonStats', [DetectorVoltage, AreaPerIon], 'IonStats_id'))
            else:
                IonStats_id_foreignkeys.append("")
                
        return IonStats_id_foreignkeys
    
    def TransfromDataSetData(self, df_Sample, DataSet_id, IDL_row_id):
        # df_Sample - Columns:
            # 'index', 'Type', 'Name', 'Status', 'Chromatographic Method',
            # 'MS Method', 'DateTime', 'DataSet', 'Instrument', 'DetectorVoltage', 'AreaPerIon'
        GC_Method_id = df_Sample['Chromatographic Method'].unique()[0]
        MS_Method_id = df_Sample['MS Method'].unique()[0]
        
        # There will be 2 possible values in this column, 'False' or the instrument name
        for i in df_Sample['Instrument'].unique():
            if i != 'False':
                Instrument = i

        return [DataSet_id, Instrument, IDL_row_id, GC_Method_id, MS_Method_id]
    
    def UploadTableRow(self, table, row_values_as_list):
        # Uncomment to print the query, example query:
        # INSERT INTO MS_Method VALUES ('1D OFN +250 Volts', 17, 50, 500, 30, 250);
        # print("INSERT INTO %s VALUES %s;" % (table, tuple(row_values_as_list)))
        self.cur_psql.execute("INSERT INTO %s VALUES %s;" % (table, tuple(row_values_as_list)))
        
    def UploadTableRow_ReturnSerialID(self, table, row_values_as_list, columnid):
        q1 = "INSERT INTO %s VALUES %s RETURNING %s;" % (table, tuple(row_values_as_list), columnid)
        i = q1.find('(') + 1
        query_statement = q1[:i] + 'DEFAULT, ' + q1[i:]
        
        # This corrects values that should be Null but in the string conversion are ''
        query_statement = query_statement.replace("''","NULL")
        
        # Uncomment to print the query, example query: 
        # INSERT INTO IDL VALUES (DEFAULT, 0.004696, 0.02) RETURNING IDL_id;
        # print(query_statement)
        self.cur_psql.execute(query_statement)
        id_row =  self.cur_psql.fetchall()
        return id_row[0][0]
    
    def close_all_connections(self):
        self.conn_psql.commit()
        self.conn_psql.close()
        
        
