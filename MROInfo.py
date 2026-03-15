# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import time
import dbconnect
import MROModule as MROModule
import pyspark.sql.functions as F

catalog = "analyticadebuddha.mro"
pk = [
    F.col("table_name").alias("TableName"), F.col("pk_name").alias("PkName"), F.col("pk_columns").alias("PkColumns"), F.col("is_composite").alias("IsComposite")
]

fk = [
    F.col("Table_with_fk").alias("TableName"), F.col("references_table").alias("ReferencesTable"), F.col("num_foreign_keys").alias("NumForeignKeys"), F.col("column_mappings").alias("ColumnMappings")
]

tabledic = []
tablenondate = []

def getTableStatus(table, spark, jdbc_url, connection_properties):
    '''
    Get table status information using Spark JDBC connection
    '''
    # tabledic = []
    # tablenondate = []
    # lentable = len(tables)

    # for table in tables:
    print(f"Processing table {table}")
    
    t1query = f"""
        SELECT 
            COUNT(*) AS Count, 
            MIN(CreationTime) as FirstDate, 
            MAX(CreationTime) as LatestDate 
        FROM [{table}]
    """

    t2query = f"""
        SELECT 
            COUNT(*) AS Count
        FROM [{table}]
    """
    
    try:
        result_df = spark.read.jdbc(
            url=jdbc_url,
            table=f"({t1query}) as status_query",
            properties=connection_properties
        )
        
        result = result_df.collect()[0]
        
        COUNTS = result['Count']
        FIRSTDATE = result['FirstDate']
        LATESTDATE = result['LatestDate']

        DICTABLE = {
            "TABLENAME": table,
            "COUNT": COUNTS,
            "FIRSTDATE": FIRSTDATE,
            "upDatedUpTo": LATESTDATE
        }
        tabledic.append(DICTABLE)
        
        print(
            f"The total number of records in table {table} is: {COUNTS} "
            f"which contains data since {FIRSTDATE} and the latest date is {LATESTDATE} based on CreationTime \n"
        )

    except Exception as e:
        print(f"Error processing table {table}: {e}")

        result_df = spark.read.jdbc(
            url=jdbc_url,
            table=f"({t2query}) as status_query",
            properties=connection_properties
        )
                
        result = result_df.collect()[0]
                
        COUNTS = result['Count']

        tablenondate.append({
            "TABLENAME": table,
            "COUNT": COUNTS
        })

    time.sleep(5)

    return None

def getNoneNullTables(spark, jdbc_url, connection_properties):
    '''
    Get non-empty tables using Spark JDBC connection
    '''
    try:
        tables_query = """
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE = 'BASE TABLE'
        """
        
        tables_df = spark.read.jdbc(
            url=jdbc_url,
            table=f"({tables_query}) as tables_query",
            properties=connection_properties
        )

        tables = [row['TABLE_NAME'] for row in tables_df.collect()]
        
        non_empty_count = 0
        tablenonemp = []
        empytable = []
        
        for table_name in tables:
            try:
                count_query = f"SELECT COUNT(*) as row_count FROM [{table_name}]"
                count_df = spark.read.jdbc(
                    url=jdbc_url,
                    table=f"({count_query}) as count_query",
                    properties=connection_properties
                )
                
                row_count = count_df.collect()[0]['row_count']
                
                if row_count > 0:
                    print(f"  {table_name} ({row_count:,} rows)")
                    non_empty_count += 1
                    getTableStatus(table_name, spark, jdbc_url, connection_properties)
                    tablenonemp.append(table_name)
                else:
                    print(f"  {table_name} is empty")
        
            except Exception as e:
                print(f"Error checking table {table_name}: {e}")
                continue
        
        if non_empty_count == 0:
            print(" No non-empty tables found")
        else:
            print(f"\nTotal non-empty tables: {non_empty_count}")
            
        return None
        
    except Exception as e:
        print(f"Error getting tables: {e}")
        return None

def getStatus(tabledic, tablenondate):
    '''
    '''
    if tabledic:
        sdf = spark.createDataFrame(tabledic)
    else:
        schema = StructType([
            StructField("TABLENAME", StringType(), True),
            StructField("COUNT", IntegerType(), True),
            StructField("FIRSTDATE", TimestampType(), True),
            StructField("upDatedUpTo", TimestampType(), True)
        ])
        sdf = spark.createDataFrame([], schema)
    
    if tablenondate:
        sdf1 = spark.createDataFrame(tablenondate)
    else:
        schema1 = StructType([StructField("tablename", StringType(), True)])
        sdf1 = spark.createDataFrame([], schema1)
    
    df = sdf.withColumn("HaveDate", F.lit("YES")).unionByName(sdf1.withColumn("HaveDate", F.lit("NO")), allowMissingColumns=True)
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.tablestatus")

    return None

def getAuditTable(status, dfpk):
    '''
    '''
    audit = status.alias("df1").join(
        dfpk.alias("df2"), F.col("df1.TableName") == F.col("df2.TableName"), how = "left"
    ).select("df1.*","df2.PkColumns", "df2.IsComposite")

    audit.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.audit")

    return None

def main():
    '''
    '''
    HostDB = '13.228.112.54'
    db = dbutils.secrets.get(scope="mro", key="db_name")
    UserDB= dbutils.secrets.get(scope="mro", key="db_user")
    PasswordDB = dbutils.secrets.get(scope="mro", key="db_password")
    jdbc_url, connection_properties = dbconnect.connectSQLServer(HostDB, db, UserDB, PasswordDB, spark)

    getNoneNullTables(spark, jdbc_url, connection_properties)
    getStatus(tabledic, tablenondate)

    status = spark.table(f"{catalog}.tablestatus")
    dfpk = MROModule.getPks(jdbc_url, connection_properties, spark).select(*pk)
    dffk = MROModule.getFks(jdbc_url, connection_properties, spark).select(*fk)
    dffk.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.tablefk")
    getAuditTable(status, dfpk)

    return None

if __name__ == "__main__":
    main()