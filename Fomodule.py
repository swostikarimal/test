import pyspark.sql.functions as F

def getfordate(df1, df2, joincon, selectcon):
    fordate = df1.alias("df1").join(
        df2.alias("df2"), joincon, how="left"
    ).select(*selectcon)

    return fordate

def filterdeleted(df):
    return df.filter(F.col("deleted_at").isNull()).drop("deleted_at")

def toPascalCase(col):
    parts = col.split('_')
    return ''.join(word.capitalize() for word in parts)

def getAlias(df):
    columns = df.columns
    collist = []
    for col in columns:
        alias = toPascalCase(col)
        collist.append(F.col(col).alias(alias))

    return df.select(*collist)

def getFlightInfo(itemdf, flightnumber, flightname):
    '''
    '''
    iteminfodf = itemdf.alias("df1").join(
        flightnumber.alias("df2"), F.col("df1.FlightNumberId") == F.col("df2.id"), how="left"
    ).join(
        flightname.alias("df3"), F.col("df1.AircraftRegistrationId") == F.col("df3.id"), how="left"
    ).select("df1.*", "df2.FlightNo", "df3.Name", "df3.AircraftTypeId", "df3.IsActive").distinct()

    return iteminfodf

def tolake(config, fullpath):
    '''
    '''
    for name, df in config.items():
        path = fullpath + name
        df.write.mode("overwrite").format("delta").save(path)
        print(f"The file has been written to {path}")

    return None