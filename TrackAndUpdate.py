import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql.types import DayTimeIntervalType
from CDC import path_exists

def unsupported_dtype(df):
    '''This function works with unsupported_dtype'''
    for field in df.schema.fields:
        if str(field.dataType) == "voidType()" or str(field.dataType) == "NullType()" or isinstance(field.dataType, DayTimeIntervalType):
            print(f"Casting column {field.name} from {field.dataType} to StringType")
            df = df.withColumn(field.name, df[field.name].cast('String'))

    return df

def Mergers(newDF, fullPath, PKColumns=None, spark=None, dbutils=None, matchcon=None):
  """
  """
  # hashformula = F.sha2(F.concat_ws("-", *[F.coalesce(F.col(c), F.lit("NULL")) for c in PKColumns]), 256)
  if path_exists(fullPath=fullPath,spark=spark, dbutils=dbutils, delta=True):
    deltaTable = DeltaTable.forPath(spark, fullPath)

    (deltaTable.alias("df1")
    .merge(newDF.alias("df2"), matchcon)
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
    )

    print(f"Merge completed successfully in {fullPath}")


  else:
    print(f"Path {fullPath} does not exist. Writing the first data to the destination as Delta Table")
    newDF.write.mode("overwrite").option("mergeSchema", "true").format("delta").save(fullPath)

  return None

def trackAndTrace(
  df, destinatation_Schema:None, subfolder:None, filename:None, 
  PKColumns:list, spark=None, dbutils=None, partitionBy=False, partitionCol=None
):
    """
    Tracks and traces changes in the DataFrame, implementing SCD Type 1.

    Args:
        df (DataFrame): The new data.
        destinatation_Schema (str): Base path for the destination.
        subfolder (str): Subfolder within the schema.
        filename (str): Name of the file.
        PKColumns (list): List of primary key columns for tracking changes.
        dbutils: Databricks utilities object.
    
    Returns:
        None
    """
     
    if dbutils is None:
        raise ValueError("dbutils must be passed as an argument.")

    fullPath = f"{destinatation_Schema}{subfolder}/{filename}"

    if path_exists(fullPath, dbutils):
      print(f"Path {fullPath} exists. Processing changes...")

      cols = list(df.columns)
      olddf = spark.read.parquet(fullPath)
      olddf = olddf.withColumn(
        "HashKey", F.sha2(F.concat_ws("||", *PKColumns), 256)
      )

      new = df.withColumn(
        "HashKey", F.sha2(F.concat_ws("||", *PKColumns), 256)
      )

      completenew = new.alias("df1").join(
        olddf.alias("df2"), F.col("df1.HashKey") == F.col("df2.HashKey"), "left_anti"
      ).drop("HashKey")

      updated = new.alias("df1").join(
        olddf.alias("df2"), F.col("df1.HashKey") == F.col("df2.HashKey"), "inner"
      ).select("df1.*").drop("HashKey").withColumn(
        "FullHash", F.sha2(F.concat_ws("||", *cols), 256)
      )

      oldupdated = olddf.drop("HashKey").withColumn(
        "FullHash", F.sha2(F.concat_ws("||", *cols), 256)
      )

      actualupdated = updated.alias("df1").join(
        oldupdated.alias("df2"), F.col("df1.FullHash") == F.col("df2.FullHash"), "left_anti"
      )

      updatedone = actualupdated.drop("FullHash").subtract(oldupdated.drop("FullHash"))

      
      oldunchanged = oldupdated.withColumn(
              "Hashkey", F.sha2(F.concat_ws("||", *PKColumns), 256)
            ).alias("df1").join(
              updatedone.withColumn(
              "Hashkey", F.sha2(F.concat_ws("||", *PKColumns), 256)
            ).alias("df2"), F.col("df1.HashKey") == F.col("df2.HashKey"), "left_anti"
            ).drop("Hashkey", "FullHash")

      if completenew.isEmpty() & updatedone.isEmpty():
        print("No new data found and avoid the writing")
      else:
        final = updatedone.unionByName(completenew.drop("FullHash")).unionByName(oldunchanged)
        print(f"Checked for new data found:{completenew.count()}  \n old Updated data found:{updatedone.count()} \nOld unchanged data updated found:{oldunchanged.count()} \nand writing them all togher to the destination")

        if partitionBy:
          final.write.mode("overwrite").option("overwriteSchema", "ture").partitionBy(f"{partitionCol}").parquet(fullPath)
        else:
          final.write.mode("overwrite").option("overwriteSchema", "ture").parquet(fullPath)

    else:
      print("The file does not exist yet. Writting the first data to the destination")
      df.write.mode("overwrite").option("overwriteSchema", "ture").parquet(fullPath)


    return None