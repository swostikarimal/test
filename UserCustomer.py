# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import *

# Removed: bookingpath = "/mnt/bronze/flight_booking"
detailpath = "/mnt/bronze/flight_detail"
paymentpath = "/mnt/bronze/payment"
ticketpath = "/mnt/bronze/ticket"
userpath = "/mnt/bronze/user"

location = "/mnt/silver/AbandonedCustomer"

# Removed: bdcols - no longer needed without booking

ticketcol = [
    "id", "flight_booking_id", "Sector","pnr_no", "title", "pax_no", "nationality", "agency_name", 
    "ticket_no", "currency","fare", "surcharge", "tax", "cashback", "discount", "netfare", "created_at"
]


colummdetail = [
    F.col("df2.created_at").alias("CreatedAt"),  F.col("df2.booking_id").alias("BD"), F.col("df2.type").alias("PassedType"), F.col("df2.SectorPair")
]

subcoldetail = ["CreatedAt", "BD", "PassedType", "SectorPair"]

nondettail = [
    F.first("UserId").alias("UserId"),F.countDistinct("booking_id").alias("FailedCount"), F.countDistinct("BD").alias("PassCount"), F.first("Ticketed").alias("Ticketed"),
    F.collect_set("created_at").alias("FailedAt"),F.collect_set("CreatedAt").alias("PassedAt"), F.collect_set("booking_id").alias("FailedBookings"),
    F.collect_set("BD").alias("PassBookings"), F.collect_set("ticketid").alias("TicketId"), F.collect_set("type").alias("FailedOnDevice"), 
    F.collect_set("PassedType").alias("PassedOnDevice"),  
    F.collect_set("SectorPair").alias("PassedSector"), F.collect_set("Email").alias("People"), F.first("Similarity").alias("Similarity")
]

notnondetail = [
    F.first("UserId").alias("UserId"), F.countDistinct("booking_id").alias("FailedCount"), F.countDistinct("BD").alias("PassCount"), 
    F.first("Ticketed").alias("Ticketed"), F.collect_set("created_at").alias("FailedAt"), F.collect_set("CreatedAt").alias("PassedAt"),
    F.collect_set("booking_id").alias("FailedBookings"), 
    F.collect_set("BD").alias("PassBookings"), F.collect_set("ticketid").alias("TicketId"),F.collect_set("type").alias("FailedOnDevice"), 
    F.collect_set("PassedType").alias("PassedOnDevice"), F.collect_set("SectorPair").alias("PassedSector"), F.first("Similarity").alias("Similarity")
]

coltodetail_2nd = [
    'Date','Email','Contact','UserId','FailedCount','PassCount','Ticketed','FailedAt','PassedAt','FailedBookings',
    'PassBookings','TicketId','FailedOnDevice', 'PassedOnDevice','PassedSector','People','Similarity'
]

groupcoltodetail_failed = ["Date", "type", "Country", "Similarity"]
aggtodetail_failed = [F.countDistinct("booking_id").alias("FailedCount"), F.countDistinct("Identifier").alias("UsersCount")]

catalog = "analyticadebuddha.failedcus"

####### TOpayment 
groupcoltopayment_failed = [
    "Date", "type", "Country", "Similarity","class_id", "Sector"
]

aggtopayment_failed=[
    F.countDistinct("booking_id").alias("FailedCount"), F.countDistinct("Identifier").alias("UsersCount")
]

column_payment = [F.col("df2.payment_mode").alias("PaymentMode"), ("df2.Currency"), F.col("df2.booking_id").alias("BD"), F.col("df2.PaymentDate"), F.col("df2.Sector").alias("sector2")]

subcolpayment = ["PaymentMode", "Currency", "BD", "PaymentDate", "Sector2"]

nonpayment = [
    F.first("UserId").alias("UserId"),F.countDistinct("booking_id").alias("FailedCount"),F.countDistinct("BD").alias("PassCount"),F.first("Ticketed").alias("Ticketed"), 
    F.first("currency").alias("Currency"), F.collect_set("created_at").alias("FailedAt"),F.collect_set("PaymentDate").alias("PassedAt"),
    F.collect_set("booking_id").alias("FailedBookings"), F.collect_set("BD").alias("PassBookings"),F.collect_set("ticketid").alias("TicketId"),
    F.collect_set("Sector").alias("FailedSectors"), F.collect_set("sector2").alias("PassedSectors"), F.collect_set("PaymentMode").alias("PaymentMode"), 
    F.collect_set("Email").alias("People"),F.first("Similarity").alias("Similarity")
]

notnonpayment = [
    F.first("UserId").alias("UserId"), F.countDistinct("booking_id").alias("FailedCount"), F.countDistinct("BD").alias("PassCount"),F.first("Ticketed").alias("Ticketed"),
    F.first("currency").alias("Currency"), F.collect_set("created_at").alias("FailedAt"), F.collect_set("PaymentDate").alias("PassedAt"), 
    F.collect_set("booking_id").alias("FailedBookings"),
    F.collect_set("BD").alias("PassBookings"), F.collect_set("ticketid").alias("TicketId"),
    F.collect_set("Sector").alias("FailedSectors"), F.collect_set("sector2").alias("PassedSectors"),
    F.collect_set("PaymentMode").alias("PaymentMode"), F.first("Similarity").alias("Similarity")
]

finalcolspayment = [
    'Date','Contact','Email','UserId', "Currency",'FailedCount','PassCount', 'Ticketed','FailedAt','PassedAt', 
    'FailedBookings','PassBookings', "TicketId",'PaymentMode','People','Similarity'
]

####

###to ticket
column_ticket = [
    F.col("nationality").alias("nationality"),F.col("df2.sector").alias("Sector2"), F.col("df2.flight_booking_id").alias("BD"), F.col("df2.agency_name").alias("AgencyName"), F.col("df2.created_at").alias("ticketDate")
]
subcolticket = ["nationality", "BD", "BD", "AgencyName", "ticketDate", "Sector2"]


noncolticket = [
    F.first("UserId").alias("UserId"),F.first("country").alias("Country"),F.countDistinct("flight_booking_id").alias("FailedCount"),
    F.countDistinct("BD").alias("PassCount"),F.collect_set("payment_mode").alias("FailedPaymentMode"),F.collect_set("AgencyName").alias("PassAgencyName"),
    F.collect_set("PaymentDate").alias("FailedAt"),F.collect_set("ticketdate").alias("PassedAt"),F.collect_set("class_id").alias("ClassId"), 
    F.collect_set("flight_booking_id").alias("FailedBookings"),F.collect_set("BD").alias("PassBookings"), 
    F.collect_set("Sector").alias("FailedSector"), F.collect_set("Sector2").alias("PassSector"), 
    F.collect_set("type").alias("type"), F.collect_set("Email").alias("People"),F.first("Similarity").alias("Similarity")
]

nonenoncolticket = [
            F.first("UserId").alias("UserId"),F.first("country").alias("Country"),F.countDistinct("flight_booking_id").alias("FailedCount"),
            F.countDistinct("BD").alias("PassCount"),F.collect_set("payment_mode").alias("FailedPaymentMode"), F.collect_set("AgencyName").alias("PassAgencyName"),
            F.collect_set("PaymentDate").alias("FailedAt"),F.collect_set("ticketdate").alias("PassedAt"),F.collect_set("class_id").alias("ClassId"),
            F.collect_set("Sector").alias("FailedSector"), F.collect_set("Sector2").alias("PassSector"),
            F.collect_set("flight_booking_id").alias("FailedBookings"),F.collect_set("BD").alias("PassBookings"),
            F.collect_set("type").alias("type"), F.first("Similarity").alias("Similarity")
]

groupcoltoticket_failed = ["date", "Country", "type", "class_id", "sector", "Similarity"]
aggtoticket_failed =[F.countDistinct("booking_id").alias("FailedCount"), F.countDistinct("identifier").alias("UsersCount")]

finalcolsticket = [
    'Date','Contact','Email','UserId','FailedCount','PassCount','FailedAt','PassedAt','FailedBookings', 
    F.col('PassBookings').alias("TicketId"), 'PassSector', 'FailedSector', "FailedPaymentMode", "PassAgencyName",'People','Similarity'
]
######

case1 = F.when(
    F.col("Similarity") == "Both", F.col("email")
).when(
    F.col("Similarity") == "Email", F.col("email")
).when(
    F.col("Similarity") == "Contact", F.col("Contact")
).when(
    F.col("Similarity") == "BothDiff",
    F.when(
        F.col("user_id") != "0", 
        F.when(
            F.col("UserEmail") != "", F.col("UserEmail")
        ).when(
            (
                (F.col("UserEmail") == "") & (F.col("email").isNotNull())
            ), F.col("email")
        ).otherwise(F.col("user_id"))
    ).otherwise(F.col("email"))
).otherwise(F.col("UserEmail"))


def toMart(df, catalog=None, table=None, location=None, mart=True):
    '''This function writes the data to mart or lake as per the params'''
    if mart:
        (
            df.write.mode(
                "overwrite"
            ).option(
                "overwriteSchema", "true").saveAsTable(
                    f'{catalog}.{table}'
                )
        )
    else:
        (
            df.write.mode("overwrite").option(
                "overwriteSchema", "true"
            ).parquet(
                f"{location}/{table}"
            )
        )

    print(f"The table {table} has been written to the catalog {catalog} successfully")

# Removed: firstIter function - depends on booking

def getUserInfo(df, user):
    '''This function is used to get the user information from the user table.'''
    df = df.alias("df1").join(
        F.broadcast(user).alias("df2"), ((F.col("df1.user_id") == F.col("df2.id")) 
        | (F.trim(F.lower("df1.email")) == F.trim(F.lower("df2.email"))) 
        | (F.col("df1.contact") == F.col("df2.contact"))), how = "left"
    ).select(
        "df1.*", F.trim(F.lower(F.col("df2.email"))).alias("UserEmail"), 
        F.col("df2.contact").alias("UserContact"), 
        F.col("df2.id").alias("UserID"), 
    ).withColumn("email", F.trim(F.lower("email")))

    return df

def getFrames(reinetiateddtop):
    '''Segregates the frames on the basic of given conditions'''
    reinetiateddtop = reinetiateddtop.fillna({"UserEmail": "", "UserContact": "", "user_id": 0, "email":"", "contact":""})
    
    reinetiateddtop = reinetiateddtop.withColumn(
        "email", F.trim(F.lower("email"))
    ).withColumn(
        "UserContact", F.trim(F.lower("UserContact"))
    ).withColumn(
        "email", F.lower(F.trim("email"))
    ).withColumn(
        "UserEmail", F.lower(F.trim("UserEmail"))
    )

    bothdiff = reinetiateddtop.filter(
        (
            (
                F.col("email") != F.col("UserEmail") 
            ) & (
                F.col("contact") != F.col("UserContact")
            ) &
            (
                ((F.col("email") != "") | (F.col("UserEmail") != "")) &
                ((F.col("contact") != "") | (F.col("UserContact") != ""))
            )
        )
    ).withColumn(
        "Similarity",F.lit("BothDiff")
    )

    same= reinetiateddtop.filter(
        (
            (
                F.col("email") == F.col("UserEmail")
            ) & (
                F.col("contact") == F.col("UserContact")
            ) &
            (
                (F.col("email") != "") & (F.col("contact") != "")
            )
        )
    ).withColumn("Similarity",F.lit("Both")) 

    contactdiff = reinetiateddtop.filter(
        (
            (
                (F.col("email") == F.col("UserEmail")) & (F.col("email") != "")
            ) & (
                F.col("contact") != F.col("UserContact")
            ) 
        )
    ).withColumn("Similarity",F.lit("Email"))
    
    emaildiff = reinetiateddtop.filter(
        (
            (
                F.col("email") != F.col("UserEmail")
            ) & (
                (F.col("contact") == F.col("UserContact")) & (F.col("contact") != "") 
            )
        )
    ).withColumn("Similarity",F.lit("Contact"))
    
    blanks = reinetiateddtop.filter(
        (
            (
                (F.col("email") == "") & (F.col("UserEmail") == "")
            ) |
            (
                (F.col("contact") == "") & (F.col("UserContact") == "")
            )
        )
    ).withColumn("Similarity",F.lit("blanks"))

    return bothdiff, same, contactdiff, emaildiff, blanks

def bothDiffer(df1, df2, df2cols):
    '''
    This functions check if the user(that has different contact and email) of table df1 made it to table df2
    '''
    df = df1.alias("df1").join(
        df2.alias("df2"),
        (
            (
                (F.col("df1.email") == F.col("df2.email")) |
                (F.col("df1.UserEmail") == F.col("df2.email")) |
                (F.col("df1.UserContact") == F.col("df2.contact"))
            ) &
            (
                F.col("df1.date") == F.col("df2.date")
            )
        ), how="left"
    ).select("df1.*", *df2cols)

    return df


def bothSame(df1, df2, df2cols):
    '''
    This functions check if the user(that has same contact and email) of table df1 made it to table df2
    '''
    df = df1.alias("df1").join(
        df2.alias("df2"),
        (
            (
                (F.col("df1.email") == F.col("df2.email")) | 
                (F.col("df1.contact") == F.col("df2.contact"))
            ) &
            (
                F.col("df1.date") == F.col("df2.date")
            )
        ), how="left"
    ).select("df1.*", *df2cols)

    return df

def contactDiffer(df1, df2, df2cols):
    '''
    This functions check if the user(that has different contact and same email) of table df1 made it to table df2
    '''
    df = df1.alias("df1").join(
        df2.alias("df2"),
        (
            (
                (F.col("df1.email") == F.col("df2.email")) | 
                (F.col("df1.contact") == F.col("df2.contact")) |
                (F.col("df1.UserContact") == F.col("df2.contact"))
            ) &
            (
                F.col("df1.date") == F.col("df2.date")
            )
        ), how="left"
    ).select("df1.*", *df2cols)

    return df

def emailDiffer(df1, df2, df2cols):
    '''
    This functions check if the user(that has same contact and different email) of table df1 made it to table df2
    '''
    df = df1.alias("df1").join(
        df2.alias("df2"),
        (
            (
                (F.col("df1.email") == F.col("df2.email")) | 
                (F.col("df1.contact") == F.col("df2.contact")) |
                (F.col("df1.UserEmail") == F.col("df2.email")) 
            ) &
            (
                F.col("df1.date") == F.col("df2.date")
            )
        ), how="left"
    ).select("df1.*", *df2cols)

    return df

def blanks(df1, df2, df2cols):
    '''
    This functions check if the user(that has useremail and userid but other email and contact is blank) of table df1 made it to table df2
    '''
    df = df1.alias("df1").join(
        df2.alias("df2"),
        (
            (
                (F.col("df1.UserEmail") == F.col("df2.email")) |
                (F.col("df1.user_id") == F.col("df2.user_id"))
            ) &
            (
                F.col("df1.date") == F.col("df2.date")
            )
        ), how="left"
    ).select("df1.*", *df2cols)

    return df


def failedAndPassed(df, cols, filtercol, info=False, ticketdata = None, id=None):
    '''This segregates the pass and failed customer'''
    failed = df.filter(filtercol.isNull()).drop(*cols)
    passed = df.filter(filtercol.isNotNull())
    
    if info:
        passed = passed.alias("df1").join(
            ticketdata.alias("df2"), filtercol == id,
            how="left"
        ).select(
            "df1.*", F.col("df2.flight_booking_id").alias("ticketid")
        ).withColumn(
            "Ticketed", F.when(
                F.col("ticketid").isNotNull(), F.lit(True)
            ).otherwise(F.lit(False))
        )

    return failed, passed

def agg(df, groupcol=None, col=None, col1=None, alias=None, aggcol=None):
    '''
    This aggregates the data and returns a dataframe with the groupcol and the col and col1 as a list of people
    '''
    if col1 is None:
        col1 = col
    if not col is None:
        addcol = [
            F.first(col).alias(alias),
            F.collect_set(col1).alias("People")
        ]

        aggcol = addcol + aggcol
        
    return df.groupBy(groupcol).agg(*aggcol)

def getUnionFiled(dfs, case=False):
    '''
    This checks if the dfs are the same and returns the union of the data
    '''
    frames = []
    for df in dfs:
        if not df.isEmpty():
            frames.append(df)
    
    if len(frames) > 1:
        df = frames[0]
        for d in frames[1:]:
            df = df.unionByName(d, allowMissingColumns = True)
    else:
        df = frames[0]
        
    if case:
        return df.withColumn("Identifier", case1)
    else:
        return df
    
# Removed: getTraceable function - depends on booking data (searchtodetailfailed)

# Removed: getTicketInfo function - depends on bridge which comes from booking

def main():
    current = F.date_add(F.date_format(F.current_date(), "y-MM-dd"), -2)
    filteryear = (F.date_format("created_at", "y-MM-dd") >= current)
    
    # Removed: booking = spark.read.parquet(bookingpath)...
    detail = spark.read.parquet(detailpath).filter(filteryear)
    payment = spark.read.parquet(paymentpath).filter(filteryear)
    ticket = spark.read.parquet(ticketpath).select(ticketcol).filter(filteryear)
    
    user = spark.read.option("mergeSchema", "false").parquet(userpath)
    
    # Removed: firstIter(booking, detail, payment, ticket, user)
    # Removed: bridge = spark.table(f"{catalog}.bridge")
    # Removed: getTicketInfo(ticket=ticket, bridge=bridge)
    
    print("⚠️  WARNING: flight_booking functionality has been removed")
    print("This script now only loads detail, payment, ticket, and user data")
    print("All booking-dependent functions (firstIter, getTraceable, getTicketInfo) have been removed")
    
    return None

if __name__ == "__main__":
    main()
