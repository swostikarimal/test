import pyspark.sql.functions as F

def path_exists(fullPath, dbutils, spark=None, delta=False):
    """Check if the path exists in DBFS."""
    if delta:
        try:
            existing_df = spark.read.format("delta").load(fullPath).limit(1)
            return True
        except Exception as e:
            print(f"Delta table check failed: {e}")
            return False
  
    try:
        dbutils.fs.ls(fullPath)
        return True
    except Exception as e:
        print(f"Path check failed: {e}")
        return False

def CDC(df, destination_schema, tablename, PKColumns, lake=False, spark=None, dbutils=None):
    """Captures the data changes"""

    if spark is None:
        print("Error: spark session not provided. Please pass the spark session as a parameter.")
        return 
    
    if dbutils is None:
        print("Error: dbutils not provided. Please pass dbutils as a parameter.")
        return
    try:
        if lake:
            fullPath = f"{destination_schema}/{tablename}"
            if path_exists(fullPath, dbutils):
                try:
                    olddata = spark.read.parquet(fullPath)
                    if olddata.count() > 0:
                        olddata = olddata.withColumn(
                            "HashKey", F.sha2(F.concat_ws("-", *PKColumns), 256)
                        )
                        df = df.withColumn(
                            "HashKey", F.sha2(F.concat_ws("-", *PKColumns), 256)
                        )
                        newdata = df.alias("df1").join(
                            olddata.alias("df2"), F.col("df1.HashKey") == F.col("df2.HashKey"), how="left_anti"
                        ).select("df1.*").drop("HashKey")
                        #withColumn(
                        #     "UpDatedOn", F.current_timestamp()
                        # )
                        if newdata.count() > 0:
                            print(f"The {newdata.count()} new records being appended as parquet to {fullPath}")
                            newdata.write.mode("append").option("mergeSchema", "true").parquet(fullPath)
                            print(f"Leftover new data {newdata.count()}")
                        else:
                            print(f"There has been no new update on {tablename} data and nothing to append")
                    else:
                        print(f"No old data found on {fullPath}---> Writing new data.")
                        if df.count() > 0:
                            df.write.mode("overwrite").option("overwriteSchema", "true").parquet(fullPath)
                            print(f"New data has been written to {destination_schema} as {tablename}.parquet file")
                        else:
                            print(f"The new file has no data.")
                except Exception as e:
                    print(f"Error occurred while reading parquet: {str(e)}")
            else:
                print(f"The directory {destination_schema} does not exist.")
                if df.count() > 0:
                    df.write.mode("overwrite").option("overwriteSchema", "true").parquet(fullPath)
                    print(f"The Directory {fullPath} has been created and the first data has been written to the respective path")
                else:
                    print(f"The new file has no data.")

        else:
            fullPath = f"{destination_schema}.{tablename}"
            if spark.catalog.tableExists(fullPath):
                olddata = spark.table(fullPath)
                if olddata.count() > 0:
                    olddata = olddata.withColumn(
                        "HashKey", F.sha2(F.concat_ws("-", *PKColumns), 256)
                    )
                    df = df.withColumn(
                        "HashKey", F.sha2(F.concat_ws("-", *PKColumns), 256)
                    )
                    newdata = df.alias("df1").join(
                        olddata.alias("df2"), F.col("df1.HashKey") == F.col("df2.HashKey"), how="left_anti"
                    ).select("df1.*").drop("HashKey")
                    # .withColumn(
                    #     "UpDatedOn", F.current_timestamp()
                    # ).drop("HashKey")

                    if newdata.count() > 0:
                        print(f"The {newdata.count()} new records being appended to {fullPath}")
                        newdata.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(fullPath)
                        print(f"Leftover new data {newdata.count()}")
                    else:
                        print(f"There has been no new update on {tablename} data and nothing to write")
                else:
                    print(f"No old data found in table {fullPath}. Writing new data.")
                    if df.count() > 0:
                        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(fullPath)
                        print(f"New data has been written to {destination_schema} as {tablename}.delta")
                    else:
                        print(f"The new file has no data.")
            else:
                print(f"The table {fullPath} does not exist.")
                if df.count() > 0:
                    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(fullPath)
                    print(f"New data has been written to {destination_schema} as {tablename}.delta")
                else:
                    print(f"The new file has no data.")

        return None
    except Exception as e:
        print(f"Error occurred: {str(e)}")