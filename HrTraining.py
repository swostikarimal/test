# Databricks notebook source
import dbconnect
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
import pyspark.sql.functions as F
import pymysql
import time
import datetime

# COMMAND ----------

import requests
public_ip = requests.get("https://api.ipify.org").text
print(public_ip)

# COMMAND ----------

# user: bt_data
# host: 13.228.112.54
# db: hrtraining_data
# pwd: TV3669WZTpWDo3uP


HostDB= "13.228.112.54"
UserDB= "bt_data"
PasswordDB= "TV3669WZTpWDo3uP"
db = "hrtraining_data"

engine = dbconnect.MysqlConnectAlchemy(HostDB, UserDB, db, PasswordDB, spark=None)

# COMMAND ----------

dbconnect.MysqlConnectPy(HostDB, UserDB, db, PasswordDB, spark=None)

# COMMAND ----------

fk_summary_query = """
    SELECT 
        kcu.table_name AS table_with_fk,
        kcu.referenced_table_name AS references_table,
        COUNT(*) as num_foreign_keys,
        GROUP_CONCAT(
            CONCAT(kcu.column_name, ' -> ', kcu.referenced_column_name) 
            SEPARATOR ', '
        ) AS column_mappings
    FROM information_schema.key_column_usage kcu
    WHERE kcu.table_schema = DATABASE()
    AND kcu.referenced_table_name IS NOT NULL
    GROUP BY kcu.table_name, kcu.referenced_table_name
    ORDER BY kcu.table_name
"""

def getPks(engine):
    """
    """
    inspector = inspect(engine)
    
    table_names = inspector.get_table_names()
    
    print("=== PRIMARY KEYS USING INSPECTOR ===")
    
    pk_info = []
    
    for table_name in table_names:
        try:
            pk_constraint = inspector.get_pk_constraint(table_name)
            
            if pk_constraint and pk_constraint['constrained_columns']:
                pk_columns = pk_constraint['constrained_columns']
                pk_name = pk_constraint.get('name', 'PRIMARY')
                
                with engine.connect() as conn:
                    count_query = text(f"SELECT table_rows FROM information_schema.tables WHERE table_name = :table_name AND table_schema = DATABASE()")
                    result = conn.execute(count_query, {'table_name': table_name})
                    row_count = result.fetchone()
                    estimated_rows = row_count[0] if row_count and row_count[0] else 0
                
                pk_info.append({
                    'table_name': table_name,
                    'columnspks': pk_columns,
                    'key_constraints_name': pk_name,
                    'column_count': len(pk_columns),
                    'estimated_rows': estimated_rows,
                    'is_composite': len(pk_columns) > 1
                })
                
                pk_display = ', '.join(pk_columns)
                status = "COMPOSITE" if len(pk_columns) > 1 else "SINGLE"
                print(f"  {table_name}: [{pk_display}] ({status}) - ~{estimated_rows} rows")
                
            else:
                print(f"  {table_name}: NO PRIMARY KEY FOUND")
                pk_info.append({
                    'table_name': table_name,
                    'columnspks': [],
                    'cons_name': None,
                    'column_count': 0,
                    'estimated_rows': 0,
                    'is_composite': False
                })
                
        except Exception as e:
            print(f"  {table_name}: ERROR - {e}")
            continue

    df = spark.createDataFrame(pd.DataFrame(pk_info))

    df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("analyticadebuddha.businesstable.hrtrainingPks")

    return df

df = getPks(engine)

df.display()

# COMMAND ----------

tables = df.filter((F.col("table_name").contains("training")) & (F.col("estimated_rows") > 0)).select("table_name").collect()
tables

# COMMAND ----------

fk_summary_query = """
    SELECT 
        kcu.table_name AS table_with_fk,
        kcu.referenced_table_name AS references_table,
        COUNT(*) as num_foreign_keys,
        GROUP_CONCAT(
            CONCAT(kcu.column_name, ' -> ', kcu.referenced_column_name) 
            SEPARATOR ', '
        ) AS column_mappings
    FROM information_schema.key_column_usage kcu
    WHERE kcu.table_schema = DATABASE()
    AND kcu.referenced_table_name IS NOT NULL
    GROUP BY kcu.table_name, kcu.referenced_table_name
    ORDER BY kcu.table_name
"""

# COMMAND ----------



# COMMAND ----------

def getNoneNullTables(conn):
    '''
    '''
    cursor = conn.cursor()
    cursor.execute("SHOW TABLES")

    tables = []
    while True:
        table = cursor.fetchone()
        if table is None:
            break
        tables.append(table[0])

    print("Non empty tables:")
    non_empty_count = 0

    for table_name in tables:
        cursor.execute(f"SELECT 1 FROM `{table_name}` LIMIT 1")
        result = cursor.fetchone()
        
        if result:
            print(f"  {table_name}")
            non_empty_count += 1

    if non_empty_count == 0:
        print(" No non-empty tables found")
    else:
        print(f"\nTotal non-empty tables: {non_empty_count}")

    cursor.close()

    return tables

def getTableStatus(tables, conn, database, date1, date2):
    '''
    '''
    tabledic = []
    tablenondate = []
    lentable = len(tables)

    for table in tables:
        print(f"Procession for table {table}")
        t1query = f"""
            SELECT count(*) AS Count, Min({date}) as FirstDate, 
            Max({date}) as LatestDate FROM `{database}`.`{table}`
        """
        try:
            t1 = pd.read_sql(t1query, conn)
            COUNTS = t1['Count'].iloc[0]
            FIRSTDATE = t1['FirstDate'].iloc[0]
            LATESTDATE = t1['LatestDate'].iloc[0]

            DICTABLE = {
                "TABLENAME": table,
                "COUNT": COUNTS,
                "FIRSTDATE": FIRSTDATE,
                "upDatedUpTo": LATESTDATE

            }
            tabledic.append(DICTABLE)
            print(
                f"The total numbers of records in table {table} is: {COUNTS} which contains the data since {FIRSTDATE} and the latest date is {LATESTDATE} based on CreatedDate \n"
            )

        except Exception as e:
            print(f"Error: {e}")
            tablenondate.append({
                "tablename": table
            })

        lentable -=1
        print(f"Remaining tables to iter: {lentable}")
       
        time.sleep(3)

        if lentable == 0:
            print("All tables processed")
            sdf = spark.createDataFrame(pd.DataFrame(tabledic))
            sdf1 = spark.createDataFrame(pd.DataFrame(tablenondate))

            sdf.write.mode("overwrite").format("delta").saveAsTable(f"{catalog}.{tabledate}")

            sdf1.write.mode("overwrite").format("delta").saveAsTable(f"{catalog}.{tablenodate}")

    return sdf, sdf1


sdf, sdf1 = getTableStatus(getNoneNullTables(conn), conn)

# COMMAND ----------

engineconn = engine.connect()
table = "externals"
query = f"""
            SELECT * 
            FROM `{table}`
        """
df = pd.read_sql(text(query), engineconn)
# df

# COMMAND ----------

from TrackAndUpdate import Mergers, unsupported_dtype

for tablename in tables:
    table = tablename[0]
    fullpath = "/mnt/hrt" + "/raw/" + table
    print(fullpath)

    query = f"""
                SELECT * 
                FROM `{table}`
            """
    df = pd.read_sql(text(query), engineconn)
    sdf = spark.createDataFrame(df)
    sdf = unsupported_dtype(df=sdf)

    sdf.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save(fullpath)

# COMMAND ----------

spark.createDataFrame(pd.read_sql(text(fk_summary_query), engine.engine.connect())).display()

# COMMAND ----------

budgetcols = [
    "Id", "training_schedule_id", "tada_estimation", "tada", "venue_budget", "miscellaneous_price", "miscellaneous_report", "total_budget", "organization_budget","total_final_budget", "sod", "instructor_allowance"
]
secheduleCols = [
    "Id", "training_id","type", "status", "is_complete", "certificate_issued", 
    "instructor_allowance_released", "disapproval_report"
]

trainingcols = [
    "id", "title", "code", "type", "is_mandatory", "is_recurring", "status", "expiry_time_frame", "last_concluded"
]

# COMMAND ----------

import pyspark.sql.functions as F
isdeleted = (F.col("deleted_at").isNull())

training = spark.read.format("delta").load("/mnt/hrt/raw/trainings").filter(isdeleted).select(*trainingcols)
trainingdates = spark.read.format("delta").load("/mnt/hrt/raw/training_dates") #.filter(isdeleted)
trainingparties = spark.read.format("delta").load("/mnt/hrt/raw/training_participants").filter(isdeleted)
trainingbudget = spark.read.format("delta").load("/mnt/hrt/raw/training_budgets").filter(isdeleted).select(*budgetcols)
trainingsechedule = spark.read.format("delta").load("/mnt/hrt/raw/training_schedules").filter(isdeleted) #.select(*secheduleCols)
approvredSecheduled  = trainingsechedule.filter(F.col("status").contains("approved"))

categorycols = ["Id", "Name", "division_id", "is_onetime", "has_hour", "post_rest_days", "pre_rest_days"]
category = spark.read.format("delta").load("/mnt/fo/raw/training_categories").filter(isdeleted).select(*categorycols)

# COMMAND ----------

training.display()

# COMMAND ----------



# COMMAND ----------

training.display()

# COMMAND ----------



# COMMAND ----------

trainingsechedule.display()

# COMMAND ----------

trainingbudget.display()

# COMMAND ----------

trainingdate = trainingdates.withColumns(
    {
        "StartTime": F.split(F.split("start_time", "'")[1], " ")[1],
        "EndTime": F.split(F.split("end_time", "'")[1], " ")[1]
    }
).select("training_schedule_id", "date", "StartTime", "EndTime")

trainingdate.display()

# COMMAND ----------

training.display()

# COMMAND ----------

trainingSechedule = approvredSecheduled.alias("df1").join(
    training.alias("df2"), F.col("df1.training_id") == F.col("df2.id"), how="left"
).select(
    "df1.*", "df2.title", "df2.type", "df2.is_mandatory", "df2.is_recurring", 
    "df2.status", "df2.expiry_time_frame", "df2.last_concluded"
)

trainingSechedule.alias("df1").join(
    trainingparties.alias("df2"), F.col("df2.training_schedule_id") == F.col("df1.id"), how="left"
).select("df2.traineeable_id", "df2.traineeable_type", "df2.training_schedule_id", "df1.*").filter(F.col("traineeable_id").contains("1693")).display()


# approvredSecheduled.alias("df1").join(
#     trainingdate.alias("df2"), F.col("df1.Id") == F.col("df2.training_schedule_id") 628
# ).select("df1.*", "df2.date", "df2.StartTime", "df2.EndTime")
# 
trainingSechedule.display()

# COMMAND ----------

trainingparties.display()  

# COMMAND ----------

training.display()

# COMMAND ----------

employee = spark.table("analyticadebuddha.flightoperation.employeeinfo")

# COMMAND ----------

# trainingparties.select(F.col("traineeable_id")).distinct().alias("df1").join(
#     employee.alias("df2"), F.col("df1.traineeable_id") == F.col("df2.EmployeeIdMain"), how="inner"
# ).display()

trainingparties.filter(F.col("deleted_at").isNull()).display()

# COMMAND ----------

trainingbudget.display()

# COMMAND ----------



# COMMAND ----------

trainingparties.alias("df1").join(
    trainingsechedule.alias("df2"), F.col("df1.training_schedule_id") == F.col("df2.id")
).display()