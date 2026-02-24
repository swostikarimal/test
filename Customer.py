# Databricks notebook source
'''
Business logic to create the main user consolidated file and user tables required by business

@PARAMS
@CREATED BY : Aayush Kumar Jagri <aayushja@buddhatech.info>
@Collaboration: Sujan Thapaliya
@CREATED    : 08/21/2024
@VERSION	: 1.0

@HISTORY	:
 1.0 - Initial version
'''
import pyspark.sql.functions as F
from pyspark.sql.window import Window

PathConsolidated = "/mnt/silver/consolidated/sales"
PathUser = "/mnt/bronze/user"

toIdentifyRepeat = [
    "flight_booking_id", F.lower("email").alias("email"), "contact", "created_at", "country_id", "user_id", 
    "Gender", "Nationality", "sector", "NetFare", "Currency", "FlightDate", "type"
]

UserCol = ["id", F.lower("email").alias("email"), "contact", "dob", "title", "created_at"]
Duplicates = ["flight_booking_id", "sector", "class_id"]


MainCols = [
    F.col("Flight_booking_id").alias("FlightBookingId"), "Email", "Contact", F.col("Created_at").alias("CreatedAt"), 
    "UserCreationDate", F.col("User_id").alias("UserId"),"Currency", "Gender", "Nationality", "Sector", "TransactionMode",
    "UserStatus","DualUserStatus", "Deviate","CustomerStatus", "Repetition", "Identifier", "UserType", "Age", "AgeGroup",
    "NetFare", "DateToday", F.col("type").alias("DeviceType")
]

FinalCols = [
    F.col("FlightBookingId"), F.date_format("CreatedAt", "y-MM-dd").alias("Date"), "UserCreationDate", F.col("sector").alias("Sector"), 
    F.col("Nationality"),F.col("Gender"),"Email","Contact",F.col("UserId").alias("UserId"), F.col("Identifier"), F.col("CustomerStatus"),
    "TransactionMode", F.col("UserStatus"),"DualUserStatus", "Deviate", F.col("UserType"), F.col("AgeGroup"), "NetFare", "Currency", 
    F.col("DeviceType"), "Repetition"
]

def userLoginGuest(repetation, user):
    """Guest - Login identification"""
    LogIn = repetation.alias("df1").join(
        user.alias("df2"), (F.lower(F.trim("df1.email")) == F.lower(F.trim("df2.email"))) 
        | ((F.trim("df1.contact")) == F.trim("df2.contact"))
        | ((F.trim("df1.user_id")) == F.trim("df2.id")), how ="inner"
    ).select(
        "df1.*",F.col("df2.title"), F.col("df2.email").alias("UserEmail"), F.col("df2.contact").alias("UserContact"), F.col("df2.dob").alias("Dob")
    ).withColumn(
        "UserStatus", F.lit("Login") 
    )

    Guest = repetation.alias("df1").join(
        user.alias("df2"), (F.lower(F.trim("df1.email")) == F.lower(F.trim("df2.email"))) 
        | ((F.trim("df1.contact")) == F.trim("df2.contact"))
        | ((F.trim("df1.user_id")) == F.trim("df2.id")), how ="left_anti"
    ).select(
        "df1.*"
    ).withColumn(
        "UserStatus", F.lit("Guest") 
    ).withColumn(
        "Dob", F.lit(None) 
    )

    Male = ["Mr.", "Mstr", "Mstr.", "Mr"]
    Female = ["Miss", "Miss.", "Mrs", "Mrs.", "Ms", "Ms."]

    gender = F.when(
        F.col("title").isin(Male), "Male"
        ).when(
            F.col("title").isin(Female), "Female"
        ).otherwise(F.col("Gender"))

    LogIn = LogIn.withColumn(
        "Gender", gender
    ).drop("title")

    return LogIn, Guest

def GuestCS(Guest):
    """Identifies Repeat or New Customer for Guest"""
    GuestGroup = Guest.groupBy(F.lower("email").alias("email"), "contact", F.trim("user_id").alias("user_id")).agg(
        F.countDistinct("flight_booking_id").alias("CountEmail")
    )
    GuestGroup = GuestGroup.withColumn(
        "CountEmail", F.max("CountEmail").over(Window.partitionBy(F.lower("email")))
    ).withColumn(
        "CountContact", F.max("CountEmail").over(Window.partitionBy("contact"))
    ).withColumn(
        "CountUserId", F.max("CountEmail").over(Window.partitionBy("user_id"))
    )

    Guestjoined = Guest.alias("df1").join(
        GuestGroup.alias("df2"), 
        (F.lower("df1.email") == F.lower("df2.email")) 
        & (F.trim("df1.contact") == F.trim("df2.contact")) 
        & (F.trim("df1.user_id") == F.trim("df2.user_id")),  how = "left"
    ).select("df1.*", F.col("df2.CountEmail"), F.col("df2.CountContact"), F.col("df2.CountUserId"))

    GuestRepeat = F.when(
        (F.col("CountEmail") > 1) | (F.col("CountContact") > 1) | ((F.col("CountUserId") > 1) & (F.col("user_id") != 0)), "Repeat"
    ).otherwise("New")

    Guestjoined = Guestjoined.withColumn(
        "CustomerStatus", GuestRepeat
    )

    return Guestjoined

def GuestIdentifier(Guest):
    """Guest Identifier"""
    FlagEmail = (
        (F.col("CountEmail") >= F.col("CountContact")) &
        F.when(
            F.col("user_id") != '0',
            F.col("CountEmail") > F.col("CountUserId")
        ).otherwise(True) 
    )

    FlagContact = (
        (F.col("CountEmail") < F.col("CountContact")) &
        F.when(
            F.col("user_id") != '0',
            F.col("CountContact") > F.col("CountUserId")
        ).otherwise(True) 
    )

    FlagUserId = (F.col("user_id") != '0') & (F.col("CountUserId") > F.col("CountEmail")) & (F.col("CountUserId") >= F.col("CountContact"))

    Identifier = F.when(
            F.col("CustomerStatus") == "Repeat",    
            F.when(
                FlagEmail, F.lower("Email")
            ).when(
                FlagContact, F.lower("Contact")
            ).when(
                FlagUserId, F.col("User_id")
            )
        ).otherwise(F.lower("email"))
    
    repetation = F.when(
            F.col("CustomerStatus") == "Repeat",    
            F.when(
                FlagEmail, F.col("CountEmail")
            ).when(
                FlagContact, F.col("CountContact")
            ).when(
                FlagUserId, F.col("CountUserId")
            )
        ).otherwise(F.lit(1))

    Guest = Guest.withColumn(
        "Identifier", Identifier
    ).withColumn(
        "Repetition", repetation
    ).drop("CountEmail", "CountContact", "CountUserId")

    return Guest

def LogInCS(LogIn):
    """Identifyies Repeat or New Customer for LoggedIn"""
    LogInGroup = LogIn.groupBy(F.lower("UserEmail").alias("UserEmail")).agg(
        F.count_distinct("flight_booking_id").alias("CountEmail")
    ).withColumn(
        "CountEmail", F.max("CountEmail").over(Window.partitionBy(F.lower("UserEmail")))
    )
    
    LogInjoined = LogIn.alias("df1").join(
        LogInGroup.alias("df2"), F.trim("df1.UserEmail") ==  F.trim("df2.UserEmail"), how="left"
    ).select("df1.*", F.col("df2.CountEmail"))

    LoginRepeat = F.when(
        (F.col("CountEmail") > 1), "Repeat"
    ).otherwise("New")

    LogInjoined = LogInjoined.withColumn(
        "CustomerStatus", LoginRepeat
    ).withColumn(
        "Identifier",  F.lower("UserEmail")
        # F.when(
        #     F.col("CustomerStatus") == "Repeat",
        #     F.lower("UserEmail")
        # ).otherwise(F.lower("email"))
    ).withColumn(
        "Repetition", F.when(
            F.col("CustomerStatus") == "Repeat",F.col("CountEmail")
        ).otherwise(F.lit(1))
    ).drop("CountEmail", "UserEmail", "UserContact")

    return LogInjoined

def Dob(Customer):
    """Extracts the date from jave Gorgiancalender class"""
    Customer = Customer.withColumn(
        "year", F.regexp_extract("dob", r"YEAR=(\d+)", 1)
    ).withColumn(
        "month", F.regexp_extract("dob", r"MONTH=(\d+)", 1)
    ).withColumn(
        "day", F.regexp_extract("dob", r"DAY_OF_MONTH=(\d+)", 1)
    ).withColumn(
        "Dob", F.concat_ws("-", F.col("year"), F.col("month"), F.col("day"))
    ).withColumn(
        "Dob", F.date_format("Dob", "y-MM-d")
    ).withColumn(
        "Dob", F.to_date("Dob", "y-MM-d")
    ).drop("year", "month", "day")

    return Customer

def UserType(Customer):
    """Identifies the Business and Individual users"""
    base_names = [
        "gmail", "yahoo", "ymail", "rocketmail", "outlook", "hotmail", "live", "msn", "icloud", 
        "aol", "gmx", "yandex", "rediffmail", "q", "inbox", "seznam", "googlemail", "mac"
    ]
    Domain = F.lower(F.concat_ws(".", F.slice(F.split(F.split(F.col("email"), "@")[1], "\\."), 1, 1)))

    UserType = F.when(
        F.trim("Domain").isin(base_names), "Individual"
    ).otherwise("Business")

    Customer = Customer.withColumn(
        "Domain", Domain
    ).withColumn(
        "UserType", UserType
    ).drop("Domain")

    return Customer

def getRepeatCustomer(df):
    '''
    '''
    df = df.withColumn(
        "Identifier", F.when(
            F.col("Identifier").isNull(), F.lit("unidentified")
        ).otherwise(F.col("Identifier"))
    )
     
    actualrepeat = df.select(
        F.trim(F.lower("Identifier")).alias("Identifier")
    ).groupBy("Identifier").agg(
        F.count("Identifier").alias("Repetition")
    )

    status = actualrepeat.withColumn(
        "CustomerStatus", 
        F.when(
            F.col("Repetition") > 1, "Repeat"
        ).otherwise("New")
    )

    duplicates = df.drop("CustomerStatus", "Repetition", "dob").alias("df1").join(
        F.broadcast(status).alias("df2"), 
        F.trim(F.lower("df1.Identifier")) == F.trim(F.lower("df2.Identifier")), how="inner"
    ).select("df1.*", "df2.CustomerStatus", F.col("df2.Repetition"))


    return duplicates

def userDeviation(df,user):
    '''Return the deviation of loginusers'''
    deviate = F.when(
        (F.col("UserStatus") == "Login") & (F.col("TransactionMode") == "Guest") 
        & (F.col("UserCreationDate") < F.col("created_at")), 1
    ).otherwise(0)

    df = df.alias("df1").join(
        user.alias("df2"), (
            F.trim(F.lower("df1.Identifier")) == F.trim(F.lower("df2.email"))
        ) | (
            F.trim("df1.Identifier") == F.trim("df2.contact")
        ), how='left'

    ).select(
        "df1.*", F.col("df2.email").alias("UserEmail"), 
        F.col("df2.created_at").alias("UserCreationDate"), "df2.dob"
    ).withColumn(
        "UserCreationDate",  F.date_format("UserCreationDate", "y-MM-dd")
    ).withColumn(
        "DualUserStatus", F.col("UserStatus")
    ).withColumn(
        "UserStatus", F.when(
            F.col("UserEmail").isNull(), "Guest"
        ).otherwise("Login")
    ).withColumn(
        "Created_at", F.date_format("created_at", "y-MM-dd")
    )
    df = df.withColumn(
        "TransactionMode", 
        F.when(
            F.col("User_id") != '0', "Login"
        ).otherwise(F.lit("Guest"))
    ).withColumn(
        "Deviate",
        deviate
    )
    
    return df

def AgeGroup(df):
    """Age catagorization"""
    AgeSeG = F.when(
        F.col("Age").between(18,24), "18-24"
    ).when(
        F.col("Age").between(25,34), "25-34"
    ).when(
        F.col("Age").between(35,44), "35-44"
    ).when(
        F.col("Age").between(45,54), "45-54"
    ).when(
        F.col("Age").between(55,64), "55-64"
    ).when(
        F.col("Age") > 64, "64+"
    ).otherwise("Unknown")

    df = df.withColumn(
            "DateToday", F.current_date()
    ).withColumn(
        "DateToday", F.date_format("DateToday", "y-MM-d")
    ).withColumn(
        "Dob", F.date_format("Dob", "y-MM-d")
    ).withColumn(
        "Age", F.round(
            (F.unix_timestamp(F.to_timestamp("DateToday")) -  F.unix_timestamp(F.to_timestamp("Dob")))/ (3600*24*365.25)
        )
    ).withColumn(
        "AgeGroup", AgeSeG
    ).filter(F.col("created_at").isNotNull()).drop("Dob").dropDuplicates(subset=["Flight_booking_id", "Sector", "Email"])

    df.select(MainCols).write.mode("overwrite").option("overwriteSchema", "true").parquet("/mnt/silver/consolidated/users")
    df = spark.read.parquet("/mnt/silver/consolidated/users").select(FinalCols)
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("analyticadebuddha.reporting.user") 

    return None

def main():
    """Call all thr functions"""
    repetation = spark.read.parquet(PathConsolidated).orderBy("created_at").dropDuplicates(Duplicates).select(toIdentifyRepeat)
    user = spark.read.parquet(PathUser).select(UserCol).distinct()
    LogIn, Guest = userLoginGuest(repetation, user)
    Guest = GuestCS(Guest)
    Guest = GuestIdentifier(Guest)
    LogIn = LogInCS(LogIn)
    Customer = LogIn.unionByName(Guest)
    Customer =  UserType(Customer)
    df = getRepeatCustomer(df=Customer)
    df =  userDeviation(df, user)
    df= Dob(Customer=df)
    AgeGroup(df)

    return None

if __name__ == "__main__":
    main()