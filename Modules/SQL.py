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
        table = 'IDL' #@UnusedVariable
        self.cur_psql.execute("SELECT * FROM IDL")
        IDL_id_row =  self.cur_psql.fetchall()
        print(IDL_id_row)
        
    def UniqueConcentrations(self, Analyte):
        sql_statement = """
            SELECT DISTINCT(Concentration_pg)
            FROM PeakTable
            WHERE Analyte = '%s'
            Order By Concentration_pg ASC;
        """ % Analyte
        
        return pd.read_sql_query(sql_statement, self.conn_psql)
        
    def Analyte_SingleConcentration_Results(self, concentration_pg, Analyte_tup):
        sql_statement = """
            WITH Flatten_PeakTableData as (
                SELECT
                    DataSet.Instrument as Inst,
                    MS_Method.DetectorOffset_Volts as Det_Offset,
                    PeakTable.DataProcessingType as DP_Type,
                    PeakTable.Concentration_pg as Conc,
                    Case
                        WHEN PeakTable.DataProcessingType = 'Unknown' THEN NULL
                        ELSE PeakTable.Area
                    END as Area,
                    Case
                        WHEN PeakTable.DataProcessingType = 'Unknown' THEN NULL
                        ELSE PeakTable.Height 
                    END as Height,
                    Case
                        WHEN PeakTable.DataProcessingType = 'Unknown' THEN NULL
                        ELSE PeakTable.Quant_SN 
                    END as Quant_SN,
                    Case
                        WHEN PeakTable.DataProcessingType = 'Target' THEN NULL
                        ELSE PeakTable.similairity 
                    END as Similarity
                From PeakTable
                Inner Join Sample ON Sample.Sample_id = PeakTable.Sample_id
                Inner Join DataSet ON DataSet.DataSet_id = Sample.DataSet_id
                Inner Join MS_Method ON MS_Method.MS_Method_id = DataSet.MS_Method_id
                WHERE 
                    PeakTable.Analyte IN %s
                    AND
                    PeakTable.Concentration_pg = '%s'
            ),
            
            AVG_STDEV as (
                SELECT 
                    Inst,
                    Det_Offset,
                    AVG(Area) as Ave_Area,
                    STDDEV(Area) as StdDev_Area,
                    AVG(Height) as Ave_Height,
                    STDDEV(Height) as StdDev_Height,
                    AVG(Quant_SN) as Ave_Quant_SN,
                    STDDEV(Quant_SN) as StdDev_Quant_SN,
                    AVG(Similarity) as Ave_Similarity,
                    STDDEV(Similarity) as StdDev_Similarity
                FROM Flatten_PeakTableData
                GROUP BY
                    Inst,
                    Det_Offset
                ORDER BY
                    Det_Offset,
                    Inst ASC
            )
            
            SELECT
                Inst,
                Det_Offset,
                ROUND(Ave_Area,0) AS Ave_Area,
                ROUND(StdDev_Area,0) AS StdDev_Area,
                ROUND(StdDev_Area/Ave_Area * 100,2) AS RSD_Area,
                ROUND(Ave_Height,0) AS Ave_Height,
                ROUND(StdDev_Height,0) As StdDev_Height,
                ROUND(StdDev_Height/Ave_Height * 100,2) AS RSD_Height,
                ROUND(Ave_Quant_SN,2) as Ave_Quant_SN,
                ROUND(StdDev_Quant_SN,3) as StdDev_Quant_SN,
                ROUND(StdDev_Quant_SN/Ave_Quant_SN * 100,2) AS RSD_Quant_SN,
                ROUND(Ave_Similarity,1) as Ave_Similarity,
                ROUND(StdDev_Similarity,1) as StdDev_Similarity,
                ROUND(StdDev_Similarity/Ave_Similarity * 100,2) AS RSD_Similarity
            FROM AVG_STDEV
            ORDER BY
                Det_Offset,
                Inst ASC;
        """ % (Analyte_tup, concentration_pg)
        
        return pd.read_sql_query(sql_statement, self.conn_psql)
    
    def OFN_SingleConcentration_Results(self, concentration_pg):
        sql_statement = """
            WITH Flatten_C10_PeakTableData as (
                SELECT
                    DataSet.DataSet_id as SetName,
                    DataSet.Instrument as Inst,
                    MS_Method.DetectorOffset_Volts as Det_Offset,
                    PeakTable.DataProcessingType as DP_Type,
                    PeakTable.Concentration_pg as Conc,
                    SUBSTR(PeakTable.Analyte,1,11) AS Analyte,
                    Case
                        WHEN PeakTable.DataProcessingType = 'Unknown' THEN NULL
                        ELSE PeakTable.Area
                    END as Area,
                    Case
                        WHEN PeakTable.DataProcessingType = 'Unknown' THEN NULL
                        ELSE PeakTable.Height 
                    END as Height,
                    Case
                        WHEN PeakTable.DataProcessingType = 'Unknown' THEN NULL
                        ELSE PeakTable.Quant_SN 
                    END as Quant_SN,
                    Case
                        WHEN PeakTable.DataProcessingType = 'Target' THEN NULL
                        ELSE PeakTable.similairity 
                    END as Similarity
                From PeakTable
                Inner Join Sample ON Sample.Sample_id = PeakTable.Sample_id
                Inner Join DataSet ON DataSet.DataSet_id = Sample.DataSet_id
                Inner Join MS_Method ON MS_Method.MS_Method_id = DataSet.MS_Method_id
                WHERE 
                    PeakTable.Analyte IN ('OFN','Perfluoronaphthalene')
                    AND
                    PeakTable.Concentration_pg = '%s'
            ),
            
            C10_AVG_STDEV as (
                SELECT 
                    Inst,
                    Det_Offset,
                    AVG(Area) as Ave_Area,
                    STDDEV(Area) as StdDev_Area,
                    AVG(Height) as Ave_Height,
                    STDDEV(Height) as StdDev_Height,
                    AVG(Quant_SN) as Ave_Quant_SN,
                    STDDEV(Quant_SN) as StdDev_Quant_SN,
                    AVG(Similarity) as Ave_Similarity,
                    STDDEV(Similarity) as StdDev_Similarity
                FROM Flatten_C10_PeakTableData
                GROUP BY
                    Inst,
                    Det_Offset
                ORDER BY
                    Det_Offset,
                    Inst ASC
            )
            
            SELECT
                Inst,
                Det_Offset,
                ROUND(Ave_Area,0) AS Ave_Area,
                ROUND(StdDev_Area,0) AS StdDev_Area,
                ROUND(StdDev_Area/Ave_Area * 100,2) AS RSD_Area,
                ROUND(Ave_Height,0) AS Ave_Height,
                ROUND(StdDev_Height,0) As StdDev_Height,
                ROUND(StdDev_Height/Ave_Height * 100,2) AS RSD_Height,
                ROUND(Ave_Quant_SN,2) as Ave_Quant_SN,
                ROUND(StdDev_Quant_SN,3) as StdDev_Quant_SN,
                ROUND(StdDev_Quant_SN/Ave_Quant_SN * 100,2) AS RSD_Quant_SN,
                ROUND(Ave_Similarity,1) as Ave_Similarity,
                ROUND(StdDev_Similarity,1) as StdDev_Similarity,
                ROUND(StdDev_Similarity/Ave_Similarity * 100,2) AS RSD_Similarity
            FROM C10_AVG_STDEV
            ORDER BY
                Det_Offset,
                Inst ASC;
        """ % concentration_pg
        return pd.read_sql_query(sql_statement, self.conn_psql)
        
    def Tetradecane_500fg_results(self):
        sql_statement = """
            WITH Flatten_C10_PeakTableData as (
                SELECT
                    DataSet.DataSet_id as SetName,
                    DataSet.Instrument as Inst,
                    MS_Method.DetectorOffset_Volts as Det_Offset,
                    PeakTable.DataProcessingType as DP_Type,
                    PeakTable.Concentration_pg as Conc,
                    SUBSTR(PeakTable.Analyte,1,11) AS Analyte,
                    Case
                        WHEN PeakTable.DataProcessingType = 'Unknown' THEN NULL
                        ELSE PeakTable.Area
                    END as Area,
                    Case
                        WHEN PeakTable.DataProcessingType = 'Unknown' THEN NULL
                        ELSE PeakTable.Height 
                    END as Height,
                    Case
                        WHEN PeakTable.DataProcessingType = 'Unknown' THEN NULL
                        ELSE PeakTable.Quant_SN 
                    END as Quant_SN,
                    Case
                        WHEN PeakTable.DataProcessingType = 'Target' THEN NULL
                        ELSE PeakTable.similairity 
                    END as Similarity
                From PeakTable
                Inner Join Sample ON Sample.Sample_id = PeakTable.Sample_id
                Inner Join DataSet ON DataSet.DataSet_id = Sample.DataSet_id
                Inner Join MS_Method ON MS_Method.MS_Method_id = DataSet.MS_Method_id
                WHERE 
                    PeakTable.Analyte IN ('Tetradecane','Tetradecane (C14)')
                    AND
                    PeakTable.Concentration_pg = '0.5'
            ),
            
            C10_AVG_STDEV as (
                SELECT 
                    Inst,
                    Det_Offset,
                    AVG(Area) as Ave_Area,
                    STDDEV(Area) as StdDev_Area,
                    AVG(Height) as Ave_Height,
                    STDDEV(Height) as StdDev_Height,
                    AVG(Quant_SN) as Ave_Quant_SN,
                    STDDEV(Quant_SN) as StdDev_Quant_SN,
                    AVG(Similarity) as Ave_Similarity,
                    STDDEV(Similarity) as StdDev_Similarity
                FROM Flatten_C10_PeakTableData
                GROUP BY
                    Inst,
                    Det_Offset
                ORDER BY
                    Det_Offset,
                    Inst ASC
            )
            
            SELECT
                Inst,
                Det_Offset,
                ROUND(Ave_Area,0) AS Ave_Area,
                ROUND(StdDev_Area,0) AS StdDev_Area,
                ROUND(StdDev_Area/Ave_Area * 100,2) AS RSD_Area,
                ROUND(Ave_Height,0) AS Ave_Height,
                ROUND(StdDev_Height,0) As StdDev_Height,
                ROUND(StdDev_Height/Ave_Height * 100,2) AS RSD_Height,
                ROUND(Ave_Quant_SN,2) as Ave_Quant_SN,
                ROUND(StdDev_Quant_SN,3) as StdDev_Quant_SN,
                ROUND(StdDev_Quant_SN/Ave_Quant_SN * 100,2) AS RSD_Quant_SN,
                ROUND(Ave_Similarity,1) as Ave_Similarity,
                ROUND(StdDev_Similarity,1) as StdDev_Similarity,
                ROUND(StdDev_Similarity/Ave_Similarity * 100,2) AS RSD_Similarity
            FROM C10_AVG_STDEV
            ORDER BY
                Det_Offset,
                Inst ASC;
        """
        return pd.read_sql_query(sql_statement, self.conn_psql)
        
    def AlkAveDMData(self):
        sql_statement = """
            WITH DMIonStatsData AS (
                SELECT
                    Sample.DateTimeStamp as TS,
                    DataSet.DataSet_id as SetName,
                    IonStats.Voltage as DetectorVoltage,
                    IonStats.AreaPerion as API
                FROM Sample
                INNER JOIN DataSet ON DataSet.DataSet_id = Sample.DataSet_id
                INNER JOIN IonStats ON IonStats.IonStats_id =  Sample.IonStats_id
                 WHERE 
                    Sample.SampleType = 'Detector Measurement' AND
                    DataSet.DataSet_id LIKE 'Alk%'
                Order By SetName ASC
            ),
            
            DMIonStatsData_rownums AS (
                SELECT * FROM (
                    SELECT
                        TS,
                        SetName,
                        DetectorVoltage,
                        API,
                        row_number()
                            over(partition by SetName Order By TS ASC) AS rownum FROM DMIonStatsData
                    ) AS DMTemp
                ORDER BY SetName, TS ASC
            )
            
            
            SELECT
                SetName,
                DetectorVoltage,
                AVG(API) AS Ave_API,
                FLOOR((rownum - 1) / 3) AS DM_API_Group
            FROM DMIonStatsData_rownums
            Group By SetName, DM_API_Group, DetectorVoltage
            ORDER BY SetName, DM_API_Group ASC;
        """
        
        return pd.read_sql_query(sql_statement, self.conn_psql)
        
    def AlkInjectionReps(self):
        sql_statement = """
            WITH SampleData AS (
                SELECT
                    Sample.DateTimeStamp as DT,
                    Sample.DataSet_id as SetName,
                    CASE
                        WHEN RIGHT(Sample.SampleName,2) LIKE ':%' THEN LEFT(Sample.SampleName,-2)
                        ELSE Sample.SampleName
                    END AS S_Name
                FROM Sample
                WHERE 
                    Sample.SampleName LIKE 'Alk%'
                ORDER BY SetName, DT ASC
            ),
            
            SampleData_rownums AS ( 
                SELECT * FROM (
                    SELECT
                        DT,
                        SetName,
                        S_Name,
                        row_number()
                            over (partition by SetName Order By DT ASC) as rownum from SampleData
                    ) as SampleTemp
                ORDER BY SetName, DT ASC
            )
            
            SELECT 
                SetName,
                DataSet.Instrument as Inst,
                MS_Method.DetectorOffset_Volts as Offset_volts,
                S_Name,
                COUNT(S_Name) as Reps,
                SUM(rownum) as Seq
            FROM SampleData_rownums
            INNER JOIN DataSet ON DataSet.DataSet_id = SetName
            INNER JOIN MS_Method ON MS_Method.MS_Method_ID = DataSet.MS_Method_id
            Group by SetName, S_Name, Inst, Offset_volts
            ORDER BY SetName, Seq;        
        """
        
        return pd.read_sql_query(sql_statement, self.conn_psql)
        
    def AlkGOIonStats(self):
        sql_statement = """
            WITH GO_IonStats AS (
                 SELECT
                    Sample.DateTimeStamp as DT,
                    DataSet.DataSet_id as SetName,
                    DataSet.Instrument as Inst,
                    MS_Method.DetectorOffset_Volts as Offset_volts,
                    IonStats.Voltage as GO_DetVoltage,
                    IonStats.AreaPerIon as GO_AreaPerIon
                FROM Sample
                INNER JOIN DataSet ON DataSet.DataSet_id = Sample.DataSet_id
                INNER JOIN IonStats ON IonStats.IonStats_id = Sample.IonStats_id
                INNER JOIN MS_Method ON MS_Method.MS_Method_ID = DataSet.MS_Method_id
                WHERE 
                    Sample.SampleType = 'Gain Optimization' AND
                    DataSet.DataSet_id LIKE 'Alk%'
            ),
            
            GO_IonStats_rownums AS (
                SELECT * FROM (
                            SELECT
                                DT,
                                SetName,
                                Inst, 
                                Offset_volts, 
                                GO_DetVoltage, 
                                GO_AreaPerIon, 
                                row_number() 
                                    over (partition by SetName Order By DT ASC) as rownum from GO_IonStats
                            ) as GOtemp
                Order By SetName, rownum ASC
            ),
            
            GO_IonStats_Start_End_Separated AS (
                SELECT
                    SetName,
                    Inst,
                    Offset_volts,
                    CASE
                        WHEN rownum = 1 THEN GO_DetVoltage
                        ELSE NULL
                    END as Starting_GO_DV,
                    CASE
                        WHEN rownum = 1 THEN GO_AreaPerIon
                        ELSE NULL
                    END as Starting_GO_API,
                    CASE
                        WHEN rownum = 2 THEN GO_DetVoltage
                        ELSE NULL
                    END as Ending_GO_DV,
                    CASE
                        WHEN rownum = 2 THEN GO_AreaPerIon
                        ELSE NULL
                    END as Ending_GO_API
                FROM GO_IonStats_rownums
            )
            
            SELECT
                SetName,
                Inst,
                Offset_volts,
                SUM(Starting_GO_DV) AS Starting_GO_DV,
                SUM(Starting_GO_API) AS Starting_GO_API,
                SUM(Ending_GO_DV) AS Ending_GO_DV,
                SUM(Ending_GO_API) AS Ending_GO_API
            FROM GO_IonStats_Start_End_Separated
            GROUP BY Inst, Offset_volts, SetName
            ORDER BY Inst, Offset_volts ASC;
        """
        
        return pd.read_sql_query(sql_statement, self.conn_psql)
        
    def OFNIonStats(self):
        # for help with the Top N Per Group in SQL or Grouped LIMIT in PostgreSQL: show the first N rows for each group
        # https://gist.github.com/tototoshi/4376938
        # https://spin.atomicobject.com/2016/03/12/select-top-n-per-group-postgresql/

        sql_statement = """
            WITH DM_IonStats AS ( 
                 SELECT
                    Sample.DateTimeStamp as DT,
                    DataSet.DataSet_id as SetName,
                    DataSet.Instrument as Inst,
                    MS_Method.DetectorOffset_Volts as Offset_volts,
                    IonStats.Voltage as DM_DetVoltage,
                    IonStats.AreaPerIon as DM_AreaPerIon
                FROM Sample
                INNER JOIN DataSet ON DataSet.DataSet_id = Sample.DataSet_id
                INNER JOIN IonStats ON IonStats.IonStats_id = Sample.IonStats_id
                INNER JOIN MS_Method ON MS_Method.MS_Method_ID = DataSet.MS_Method_id
                WHERE 
                    Sample.SampleType = 'Detector Measurement' AND
                    DataSet.DataSet_id LIKE 'OFN%'
            ),
            
            DM_IonStats_rownums AS (
                SELECT * FROM (
                            SELECT
                                DT,
                                SetName,
                                Inst, 
                                Offset_volts, 
                                DM_DetVoltage, 
                                DM_AreaPerIon, 
                                row_number() 
                                    over (partition by SetName Order By DT ASC) as rownum from DM_IonStats
                            ) as DMtemp
                Order By SetName, rownum ASC
            ),
                        
            DM_IonStats_Start_End_Separated AS (
                SELECT
                    SetName,
                    Inst,
                    Offset_volts,
                    DM_DetVoltage,
                    CASE
                        WHEN rownum <= 3 THEN DM_AreaPerIon
                        ELSE NULL
                    END as Starting_DM_API,
                    CASE
                        WHEN rownum >= 4 THEN DM_AreaPerIon
                        ELSE NULL
                    END as Ending_DM_API
                FROM DM_IonStats_rownums
            ),
            
            DM_IonStats_Final AS (
                SELECT
                    SetName,
                    Inst,
                    Offset_volts,
                    DM_DetVoltage,
                    CAST(AVG(Starting_DM_API) as FLOAT(1)) as Ave_Starting_DM_API,
                    CAST(AVG(Ending_DM_API) as FLOAT(1)) as Ave_Ending_DM_API
                FROM DM_IonStats_Start_End_Separated
                GROUP BY Inst, Offset_volts, DM_DetVoltage, SetName
                ORDER BY Offset_volts, Inst ASC
            ),
                        
            GO_IonStats AS (
                 SELECT
                    Sample.DateTimeStamp as DT,
                    DataSet.DataSet_id as SetName,
                    DataSet.Instrument as Inst,
                    MS_Method.DetectorOffset_Volts as Offset_volts,
                    IonStats.Voltage as GO_DetVoltage,
                    IonStats.AreaPerIon as GO_AreaPerIon
                FROM Sample
                INNER JOIN DataSet ON DataSet.DataSet_id = Sample.DataSet_id
                INNER JOIN IonStats ON IonStats.IonStats_id = Sample.IonStats_id
                INNER JOIN MS_Method ON MS_Method.MS_Method_ID = DataSet.MS_Method_id
                WHERE 
                    Sample.SampleType = 'Gain Optimization' AND
                    DataSet.DataSet_id LIKE 'OFN%'
            ),
            
            GO_IonStats_rownums AS (
                SELECT * FROM (
                            SELECT
                                DT,
                                SetName,
                                Inst, 
                                Offset_volts, 
                                GO_DetVoltage, 
                                GO_AreaPerIon, 
                                row_number() 
                                    over (partition by SetName Order By DT ASC) as rownum from GO_IonStats
                            ) as GOtemp
                Order By SetName, rownum ASC
            ),
            
            GO_IonStats_Start_End_Separated AS (
                SELECT
                    SetName,
                    Inst,
                    Offset_volts,
                    CASE
                        WHEN rownum = 1 THEN GO_DetVoltage
                        ELSE NULL
                    END as Starting_GO_DV,
                    CASE
                        WHEN rownum = 1 THEN GO_AreaPerIon
                        ELSE NULL
                    END as Starting_GO_API,
                    CASE
                        WHEN rownum = 2 THEN GO_DetVoltage
                        ELSE NULL
                    END as Ending_GO_DV,
                    CASE
                        WHEN rownum = 2 THEN GO_AreaPerIon
                        ELSE NULL
                    END as Ending_GO_API
                FROM GO_IonStats_rownums
            ),
            
            GO_IonStats_Final AS (
                SELECT
                    SetName,
                    Inst,
                    Offset_volts,
                    SUM(Starting_GO_DV) AS Starting_GO_DV,
                    SUM(Starting_GO_API) AS Starting_GO_API,
                    SUM(Ending_GO_DV) AS Ending_GO_DV,
                    SUM(Ending_GO_API) AS Ending_GO_API
                FROM GO_IonStats_Start_End_Separated
                GROUP BY Inst, Offset_volts, SetName
                ORDER BY Offset_volts, Inst ASC            
            )
            
            SELECT 
                DM_IonStats_Final.Inst,
                DM_IonStats_Final.Offset_volts,
                GO_IonStats_Final.Starting_GO_DV as Start_GO_DV,
                GO_IonStats_Final.Starting_GO_API as Start_GO_API,
                DM_IonStats_Final.Ave_Starting_DM_API as Ave_Start_DM_API,
                DM_IonStats_Final.Ave_Ending_DM_API as Ave_End_DM_API,
                GO_IonStats_Final.Ending_GO_DV as End_GO_DV,
                GO_IonStats_Final.Ending_GO_API as End_GO_API
            FROM DM_IonStats_Final
            INNER JOIN GO_IonStats_Final ON GO_IonStats_Final.SetName = DM_IonStats_Final.SetName
            ORDER BY DM_IonStats_Final.Offset_volts, DM_IonStats_Final.Inst;
        """
        
        return pd.read_sql_query(sql_statement, self.conn_psql)
        
    def OFNSensitivityData_20fg(self):
        sql_statement = '''
            WITH OFN_Sensitivity AS (    
                SELECT
                    Sample.DataSet_id as SetName,
                    DataSet.Instrument as Inst,
                    MS_Method.DetectorOffset_Volts as Offset_volts,
                    CASE
                        WHEN PeakTable.Analyte = 'Perfluoronaphthalene' THEN 'OFN'
                        ELSE PeakTable.Analyte
                    END as Analyte,
                    CASE PeakTable.DataProcessingType
                        WHEN 'Target' THEN 'TAF'
                        ELSE 'PF'
                    END as DP_TYPE,
                    PeakTable.Concentration_pg as pg_OnColumn,
                    CASE  
                        WHEN PeakTable.DataProcessingType = 'Target' THEN PeakTable.Area
                        ELSE NULL
                    END as Area,
                    CASE 
                        WHEN PeakTable.DataProcessingType = 'Target' THEN PeakTable.Quant_SN
                        ELSE NULL
                    END as Quant_SN ,
                    NULLIF(PeakTable.Similairity,0) as Similarity
                FROM PeakTable
                INNER JOIN Sample ON Sample.Sample_id = PeakTable.Sample_id 
                INNER JOIN DataSet ON DataSet.DataSet_ID = Sample.DataSet_ID
                INNER JOIN MS_Method ON MS_Method.MS_Method_ID = DataSet.MS_Method_id
                WHERE 
                    PeakTable.Concentration_pg = '0.02' AND (
                    PeakTable.Analyte = 'OFN' OR
                    PeakTable.Analyte = 'Perfluoronaphthalene')
            ),
            
            OFN_Sensitivity_Aggregation as (
                SELECT
                    SetName,
                    Inst,
                    Offset_volts,
                    Analyte,
                    pg_OnColumn,
                    AVG(Area) as Ave_Area,
                    STDDEV(Area) as StdDev_Area,
                    AVG(Quant_SN) as Ave_Quant_SN,
                    STDDEV(Quant_SN) as StdDev_Quant_SN,
                    AVG(Similarity) as Ave_Similarity,
                    STDDEV(Similarity) as StdDev_Similarity
                FROM OFN_Sensitivity
                GROUP BY 1,2,3,4,5
            )    
                
            SELECT
                Inst,
                Offset_volts,
                IDL.IDL_Value as IDL,
                Ave_Area,
                StdDev_Area,
                ROUND(StdDev_Area/Ave_Area*100,3) as RSD_Area,
                Ave_Quant_SN,
                StdDev_Quant_SN,
                ROUND(StdDev_Quant_SN/Ave_Quant_SN*100,3) as RSD_Quant_SN,
                Ave_Similarity,
                StdDev_Similarity,
                ROUND(StdDev_Similarity/Ave_Similarity*100,3) as RSD_Similarity
            FROM OFN_Sensitivity_Aggregation
            INNER JOIN DataSet ON DataSet.DataSet_id = SetName
            INNER JOIN IDL ON IDL.IDL_id = DataSet.IDL_id
            ORDER BY 2,1 ASC;
        '''
        
        return pd.read_sql_query(sql_statement, self.conn_psql)
    
    def OFNLinearityData(self):
        
        sql_statement= '''
            SELECT
                DataSet.Instrument as Inst,
                MS_Method.DetectorOffset_Volts as Offset_volts,
                DynamicRange.OrdersOfMagnitude as Orders,
                DynamicRange.ConcRange_pg_Low as ConcRange_pg_Low,
                DynamicRange.ConcRange_pg_High as ConcRange_pg_High,
                DynamicRange.Correlation_Coefficient_r as r
            FROM DataSet
            INNER JOIN MS_Method ON MS_Method.MS_Method_id = DataSet.MS_Method_id
            INNER JOIN DynamicRange ON DynamicRange.DR_id = DataSet.DR_id;
        '''
        
        return pd.read_sql_query(sql_statement, self.conn_psql)
    
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
        # If extracted != to None, and thus is a pd.DataFrame then
        # Load IDL row and get IDL_id which is the primary key
        # this will be used as a foreign key value for the DataSet table
        if type(DF_Dict['IDL']) is pd.core.frame.DataFrame: #@UndefinedVariable
            IDL_row_values_as_list = DF_Dict['IDL'][['IDL','Concentration']].iloc[0].tolist()
            IDL_row_id = self.UploadTableRow_ReturnSerialID('IDL', IDL_row_values_as_list, 'IDL_id')
        else:
            IDL_row_id = ""
            
        if type(DF_Dict['DR']) is pd.core.frame.DataFrame: #@UndefinedVariable
            DR_row_values_as_list = DF_Dict['DR'][['OrdersOfMagnitude','ConcRange_pg_Low', 'ConcRange_pg_High', 'Correlation_Coefficient_r']].iloc[0].tolist()
            DR_row_id = self.UploadTableRow_ReturnSerialID('DynamicRange', DR_row_values_as_list, 'DR_id')
        else:
            DR_row_id = ""
        
        # If extracted and not already in the DB Load the GC & MS method
        if type(DF_Dict['GC']) is pd.core.frame.DataFrame: #@UndefinedVariable
            if self.IsMethodUnique('GC', DF_Dict['GC']['GC_Method_id'].iloc[0]):
                self.UploadTableRow('GC_Method', DF_Dict['GC'].iloc[0].tolist())
            
        if type(DF_Dict['MS']) is pd.core.frame.DataFrame: #@UndefinedVariable
            if self.IsMethodUnique('MS', DF_Dict['MS']['MS_Method_id'].iloc[0]):
                self.UploadTableRow('MS_Method', DF_Dict['MS'].iloc[0].tolist())
                
        # Transform then load DataSet table data 
        # Note, I've not tested this when DF_Dict['IDL'] == None or DF_Dict['DR'] == None
        DataSetlst = self.TransfromDataSetData(DF_Dict['Sample'], DataSet_id, IDL_row_id, DR_row_id)
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
            PeakTable_id = self.UploadTableRow_ReturnSerialID('PeakTable', PeakTablelist, 'PeakTable_id') #@UnusedVariable
            
    
    def LoadIonsStats(self, df_Sample):
        # Loads IonStats data and returns a pd.Series to append to the DF_Dict['Sample']
        # that will contain the keys the links the IonStats table & the Sample table
        IonStats_id_foreignkeys = []
        
        # Iterates over each row in the df. Each row where the type is a DM or GO
        # Upload the ionstats data and append the returned IonStats_id to the IonStats_id_foreignkeys list
        # For every other row append an empty string "" IonStats_id_foreignkeys list
        
        for r in df_Sample.iterrows():
            t = r[1]['Type']
            if t == "Detector Measurement" or t == "Gain Optimization":
                DetectorVoltage = float(r[1]['DetectorVoltage'])
                AreaPerIon = float(r[1]['AreaPerIon'])
                IonStats_id_foreignkeys.append(self.UploadTableRow_ReturnSerialID('IonStats', [DetectorVoltage, AreaPerIon], 'IonStats_id'))
            else:
                IonStats_id_foreignkeys.append("")
                
        return IonStats_id_foreignkeys
    
    def TransfromDataSetData(self, df_Sample, DataSet_id, IDL_row_id, DR_row_id):
        # df_Sample - Columns:
            # 'index', 'Type', 'Name', 'Status', 'Chromatographic Method',
            # 'MS Method', 'DateTime', 'DataSet', 'Instrument', 'DetectorVoltage', 'AreaPerIon'
        GC_Method_id = df_Sample['Chromatographic Method'].unique()[0]
        MS_Method_id = df_Sample['MS Method'].unique()[0]
        
        # There will be 2 possible values in this column, 'False' or the instrument name
        for i in df_Sample['Instrument'].unique():
            if i != 'False':
                Instrument = i

        return [DataSet_id, Instrument, IDL_row_id, GC_Method_id, MS_Method_id, DR_row_id]
    
    def UploadTableRow(self, table, row_values_as_list):
        # This corrects values that should be Null but in the string conversion are ''
        query_statement = "INSERT INTO %s VALUES %s;" % (table, tuple(row_values_as_list))
        query_statement = query_statement.replace("''","NULL")
        
        # Uncomment to print the query, example query:
        # INSERT INTO MS_Method VALUES ('1D OFN +250 Volts', 17, 50, 500, 30, 250);
        # print(query_statement)
        
        self.cur_psql.execute(query_statement)
        
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
        
        
