from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import pymysql

def MysqlConnectAlchemy(HostDB, UserDB, db, PasswordDB, spark=None):
    """Connect to the RDS using SQLAlchemy"""
    try:
        connection_string = f"mysql+pymysql://{UserDB}:{PasswordDB}@{HostDB}/{db}"
        
        engine = create_engine(connection_string)
        
        with engine.connect() as conn:
            print("Connected to database successfully")
        
        return engine
    
    except SQLAlchemyError as e:
        print(f"Error connecting to database: {e}")

        return None
    
def MysqlConnectPy(HostDB, UserDB, db, PasswordDB, spark=None):
    """Connect to the RDS pymysql"""
    conn = pymysql.connect(
        host = f"{HostDB}",
        user = f"{UserDB}",
        database = f"{db}",
        password = f"{PasswordDB}"
    )
    if conn:
        print("Connected to database sucessfully")

        return conn
    else:
        print("Error")

def connectSQLServer(HostDB, db, UserDB, PasswordDB, spark=None):
    '''
    connect to sqlserver database using jdbc
    '''
    port = 1433
    jdbc_url = f"jdbc:sqlserver://{HostDB}:{port};databaseName={db};encrypt=true;trustServerCertificate=true;loginTimeout=30"

    connection_properties = {
        "user": UserDB,
        "password": PasswordDB,
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }

    try:
        test_df = spark.read.jdbc(
            url=jdbc_url, 
            table="(SELECT 1 as test_col) as test_query", 
            properties=connection_properties
        )
        test_df.collect() 
        print("Connected to SQL Server database successfully")
    except Exception as e:
        print(f"Error connecting to SQL Server database: {e}")

    return jdbc_url, connection_properties



