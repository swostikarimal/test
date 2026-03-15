# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.window import Window

schema = "analyticadebuddha.customer"
pathsectorcatagory = "analyticadebuddha.businesstable.sectorcategories"
favsector = "customerSector"
tableuserinfo = "userinformation"
rmfcurrent = "rmfcurrent"
tablehist = 'userhist'
lakepath = "/mnt/integration/mailing/"

sectorwiserecent = Window.partitionBy(
    "Identifier","Currency", "SectorPair"
).orderBy(F.desc("Latest"))

recent = Window.partitionBy("Identifier", "Currency").orderBy(F.desc("Latest"))
frequent = Window.partitionBy("Identifier", "SectorPair", "Currency").orderBy(F.desc("Latest"))

singlemulti = F.when(
    F.col("NumSectors") > 1, "Multi"
).otherwise("Single")

emailcontactcase = F.when(
    F.col("Identifier").contains("@"), F.lit("Email")
).otherwise(F.lit("Contact"))

def getSectorNum(sector, pair):
    '''
    '''
    sectorpair = sector.alias("df1").join(
        F.broadcast(pair).alias("df2"), (
            (
                F.col("df1.Sectors") ==  F.col("df2.Sector")
            ) 
            |(
                F.col("df1.Sectors") ==  F.col("df2.Category")
            )
        ), how ="left"
    ).select("df1.*", "df2.*").dropDuplicates(
        subset=[
            "identifier", "Bookedon", "Sectors", "Currency"
        ]
    )
    grouped = sectorpair.orderBy(F.desc("Bookedon")).groupby(
        "Identifier", "Sector", "Category", "Currency"
    ).agg(
        F.count("identifier").alias("Repeat"), 
        F.first("Bookedon").alias("Latest")
    ).withColumn(
        "SectorPair", F.concat("Sector" , F.lit("  "), "Category")
    ).drop("Sector", "Category")

    numsec = grouped.groupBy(
        "Identifier", "Currency"
    ).agg(
        F.count("SectorPair").alias("NumSectors")
    ).withColumn("NSectors", singlemulti)

    return grouped, numsec

def userType(df):
    base_names = [
        "gmail", "yahoo", "ymail", "rocketmail", "outlook", "hotmail", "live", "msn", "icloud", 
        "aol", "gmx", "yandex", "rediffmail", "q", "inbox", "seznam", "googlemail", "mac", "privaterelay"
    ]

    Domain = F.lower(F.concat_ws(".", F.slice(F.split(F.split(F.col("Identifier"), "@")[1], "\\."), 1, 1)))

    UserType = F.when(
        (
            (F.trim("Domain").isin(base_names)) | (F.col("TypeIdentifier") == "Contact")
        ), "Individual"

    ).otherwise("Business")

    df = df.withColumn(
        "Domain", Domain
    ).withColumn(
        "UserType", UserType
    ).drop("Domain")

    return df

def tomart(df, schema, tablename):
    '''
    '''
    df.write.mode(
        "overwrite"
    ).option(
        "mergeSchema", "true"
    ).option(
        "overwriteSchema", "true"
    ).saveAsTable(
        f"{schema}.{tablename}"
    )

def getBusinessName(df):
    '''
    '''
    df = df.withColumns(
        {
            "Domains": F.when(
                F.col("UserType") == "Business",
                F.split(F.split("identifier", "@")[1], "\.")[0]
            ).otherwise(
                F.col("Identifier")
            ),
            "Domain": F.initcap(F.col("Domains"))
            
        }
    ).drop("Domains")

    usercol = ["Nsectors","TypeIdentifier", "UserType", "Domain"]

    sectorcols = list(set(df.columns) - set(usercol) 
    )

    usercols = ["identifier", "Currency"] + usercol

    userinfo = df.select(*usercols).dropDuplicates(subset=usercol)
    sectorinfo = df.select(*sectorcols).dropDuplicates(subset=sectorcols)

    tomart(df=sectorinfo, schema=schema, tablename=favsector)
    tomart(df=userinfo, schema=schema, tablename=tableuserinfo)

    return None

def getFavSector(grouped, numsec):
    '''
    '''
    sectorpaired = F.broadcast(grouped).alias("df1").join(
        numsec.alias("df2"), (
            (
                F.col("df1.Identifier") == F.col("df2.Identifier")
            ) 
            & (
                F.col("df1.Currency") == F.col("df2.Currency")
            )
        ), how="left"
    ).select("df1.*", "df2.NSectors", "df2.NumSectors")

    sectorpaired = sectorpaired.withColumns(
        {
            "SectorRecent": F.concat(
                F.lit("Recent"), F.lit(' '), F.row_number().over(sectorwiserecent),
            ), 'RecentAll': F.concat(
                F.lit("Recent"), F.lit(' '), F.rank().over(recent)
            ), 
            "FrequentSector": F.concat(
                F.lit('Frequent'), F.lit(' '),
                F.rank().over(
                Window.partitionBy("Identifier").orderBy(
                    F.desc(
                        F.sum("Repeat").over(frequent)
                    )
                )
            ),
            ),
            "TypeIdentifier": emailcontactcase
        }
    ).drop("NumSectors")

    sectorpaired1 = userType(df=sectorpaired)

    getBusinessName(df=sectorpaired1)

    return sectorpaired1

def getCustomerData(sectorpaired, current):
    '''
    '''
    latest = current.drop("UserType").alias("df1").join(
    sectorpaired.alias("df2"),(
        (
            F.col("df1.Identifier") == F.col("df2.Identifier")
        ) 
        & (
            F.col("df1.Currency") == F.col("df2.Currency")
        )
    ), how = "left"
    ).select(
        "df1.*", "df2.SectorPair", F.col("df2.Repeat").alias("SectorRepeat"), 
        "df2.NSectors", 'df2.SectorRecent','df2.RecentAll', 'df2.FrequentSector', "df2.TypeIdentifier", "df2.UserType"
    )

    latest.write.mode(
        "overwrite"
    ).option(
        "overwriteSchema", "true"
    ).parquet(
        f"{lakepath}{rmfcurrent}"
    )

    print(f"The current customer file has been written to the {lakepath}{rmfcurrent}")

    return None

def main():
    '''
    '''
    sector = spark.table(
        f"{schema}.{tablehist}"
    ).withColumnsRenamed(
        {"email": "Identifier", "Date": "Bookedon"}
    ).filter(
        ~F.col("Sectors").isNull()
    )

    current = spark.table(
        f"{schema}.{rmfcurrent}"
    )

    pair = spark.table(
        pathsectorcatagory
    ).filter(
        F.col("InOut").contains("Out")
    )

    grouped, numsec = getSectorNum(sector, pair)
    sectorpaired = getFavSector(grouped, numsec)
    getCustomerData(sectorpaired, current)

    return None

if __name__ == "__main__":
    main()

# COMMAND ----------

spark.table("analyticadebuddha.customer.userhist").display()

# COMMAND ----------

