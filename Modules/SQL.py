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
        print(result == [])
            
    def UploadData(self, DF_Dict):
        # Load IDL row and get IDL_row_id with is the primary key row
        # this will be used as a foreign key value for the DataSet table load
        IDL_row_values_as_list = DF_Dict['IDL'][['IDL','Concentration']].iloc[0].tolist()
        IDL_row_id = self.db.UploadTableRow_ReturnSerialID('IDL', IDL_row_values_as_list, 'IDL_id')
        
        # If extracted and not already in the DB (checks will be added later) Load the GC & MS method
        
        
        
    def UploadTableRow_ReturnSerialID(self, table, row_values_as_list, columnid):
        row_values_as_csv_str = ",".join(map(str, row_values_as_list))
        # Uncomment to print the query
        # print("INSERT INTO %s VALUES (DEFAULT, %s) RETURNING %s;" % (table, row_values_as_csv_str, columnid))
        self.cur_psql.execute("INSERT INTO %s VALUES (DEFAULT, %s) RETURNING %s;" % (table, row_values_as_csv_str, columnid))
        id_row =  self.cur_psql.fetchall()
        return id_row[0][0]
    
    def close_all_connections(self):
        self.conn_psql.commit()
        self.conn_psql.close()
        
        
