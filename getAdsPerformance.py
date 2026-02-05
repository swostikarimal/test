import pyspark.sql.functions as F

def concat_(df, config):
    '''
    '''
    colname = config.get("colsname")
    cols = config.get("colsconcat")
    df1 = df.withColumn(
        colname, F.when(
            (~F.col(cols[0]).startswith(
                F.col(cols[1]))), F.concat(
                    F.col(cols[1]), F.lit("_"), F.col(cols[0])
                )
        ).otherwise(F.col(cols[0]))
    )
    
    df1 = df1.withColumn(
        colname + "1",  F.initcap(
            F.array_join(F.split(colname, "_"), " ")
        )
    )

    return df1

def getActionAndValues(actions, eventname, value=False, dim=False, config=dict, spark=None, dbutils=None, naming=None, catalog=None, case1=None, concat = False):
    '''
    '''
    column = config.get("cols")
    dimcol = config.get("cols1")

    df = actions.alias("df1").join(
        eventname.alias("df2"), (
            case1
        ), how='inner'
    ).select(
        "df1.*", F.lower("df2.name").alias("name"), 
        F.lower("df2.eventName").alias("eventName"), F.col("df2.id")
    ).filter(
        (F.col("action_type") == F.col("eventName")) 
        | (F.col("eventName") == F.col("id"))
    ).withColumn(
        "eventName", naming
    )
    # .filter(
    #     (
    #         (
    #             F.col("action_type").contains(F.lower("eventname"))
    #         ) 
    #         |(
    #         (
    #             F.lower("name").contains("sector") 
    #         ) & ~(
    #             F.col("eventName") == "purchase"
    #         )
    #         ) | (
    #             F.col("action_type").contains("link_click") 
    #         )
    #     )
    # )
    
    
    if concat:
        linkclick = actions.filter(F.col("action_type").contains("link_click")).withColumn("eventName", F.lit("Link Click")).drop("action_type")
        df = df.unionByName(linkclick, allowMissingColumns = True)
        df = concat_(df, config)
        column = config.get("cols") + [config.get("colsname")]
        dimcol = config.get("cols1") + [config.get("colsname") + "1"] + [config.get("colsname")]

    m1 = df.select(*column).distinct()

    if dim:
        m2 = df.select(*dimcol).distinct()
        m2.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{config.get('valuedimcat')}") 

    if value:
        m1.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{config.get('actionandvaluevaluecat')}")
        
        print(f"The table for ad actionvalueNum and actiondimwritten has been written to {catalog}")

    else:
        m1.filter(~F.col("eventName").contains("Link Click")).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{config.get('actionvalCurr')}")
        print(f"The table for ad currancy value has been written to {catalog}.{config.get('actionvalCurr')}")

    return None

def getAdsPerformance(df1, df2=None, dim=False, concat = False, config=dict, spark=None, dbutils=None, catalog=None):
    """
    """
    df1cols = (
        [F.col("date").alias("Date"), F.col("ad_id").alias("AdId")] 
        + [col.capitalize() for col in (df1.drop(*["date", "ad_id"]).columns)] 
    )

    if concat:
        df1 = concat_(df=df1, config=config)

        df1cols = df1cols + [config.get("colsname")]

    if dim:
        if concat:
            dim = df1.select(*config.get("cols") + [config.get("colsname")] + [config.get("colsname") + "1"]).distinct()
        else:
            dim = df1.select(*config.get("cols")).distinct()
        
        if "platform" in dim.columns:
            dim = dim.withColumn("platform", F.initcap(F.array_join(F.split("platform", "_"), " ")))
        
                                 
        dim.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{config.get('dimtable')}")

    if not df2 is None:
        df2cols = (
            [F.col("date").alias("Date"), F.col("ad_id").alias("AdId")] 
            + [col.capitalize() for col in (df2.drop(*["date", "ad_id"]).columns)] 
        )
        df2.select(*df2cols).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{config.get('mul')}")

    df1val = df1.select(*df1cols)
    df1val.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{config.get('valtable')}")
    
    return None