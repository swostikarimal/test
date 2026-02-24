# Databricks notebook source
import pyspark.sql.functions as F
from AbandonedLogic import *

catalog = "analyticadebuddha.failedcus"

def toDetail2nd(
    bothdiff, same, contactdiff, emaildiff,blanksdf, searchtodetailsucessed, ticketinfo
):
    '''
    This creates the booking to detail failed customer file for 1st and 2nd iter, 2nd iter sucessed customr, agg file for failed customer, agg file for pass customer
    '''
    bothdiffdetails = bothDiffer(df1=bothdiff, df2=searchtodetailsucessed, df2cols = colummdetail)
    bothsamedetails = bothSame(df1=same, df2=searchtodetailsucessed, df2cols=colummdetail)
    contactdiffdetails = contactDiffer(df1=contactdiff, df2=searchtodetailsucessed, df2cols=colummdetail)
    emaildiffdetails = emailDiffer(df1=emaildiff, df2=searchtodetailsucessed, df2cols=colummdetail)
    blanksdetails = blanks(df1=blanksdf, df2=searchtodetailsucessed, df2cols=colummdetail)


    bothdiff_failed, bothdiff_passed = failedAndPassed(
        df=bothdiffdetails, cols=subcoldetail, filtercol=F.col("BD"),
        info=True, ticketdata = ticketinfo, id = F.col("flight_booking_id"), spark=spark
    )

    bothdifflog = bothdiff_passed.filter((F.col("User_id") != "0"))
    bothdiffguest = bothdiff_passed.filter((F.col("User_id") == "0"))

    same_failed, same_passed = failedAndPassed(
        df=bothsamedetails, cols=subcoldetail, filtercol=F.col("BD"),
        info=True, ticketdata = ticketinfo, id = F.col("flight_booking_id"), spark=spark
    )

    contactdiff_failed, contactdiff_passed = failedAndPassed(
        df=contactdiffdetails, cols=subcoldetail, filtercol=F.col("BD"),
        info=True, ticketdata = ticketinfo, id = F.col("flight_booking_id"), spark=spark
    )

    emaildiff_failed, emaildiff_passed = failedAndPassed(
        df=emaildiffdetails, cols=subcoldetail, filtercol=F.col("BD"),
        info=True, ticketdata = ticketinfo, id = F.col("flight_booking_id"), spark=spark

    )

    emaildiff_failed, emaildiff_passed = failedAndPassed(
        df=emaildiffdetails, cols=subcoldetail, filtercol=F.col("BD"),
        info=True, ticketdata = ticketinfo, id = F.col("flight_booking_id"), spark=spark

    )

    blank_failed, blank_passed = failedAndPassed(
        df=blanksdetails, cols=subcoldetail, filtercol=F.col("BD"),
        info=True, ticketdata = ticketinfo, id = F.col("flight_booking_id"), spark=spark

    )

    aggemail = agg(
        df=emaildiff_passed, groupcol =["Date", "Contact"], col="UserEmail", col1="Contact", alias="Email", aggcol=notnondetail
    )
    aggcontact = agg(
        df=contactdiff_passed, groupcol =["Date", F.col("UserEmail").alias("Email")], col="contact", col1="email", alias="Contact", aggcol=notnondetail
    )
    aggsame = agg(
        df=same_passed, groupcol =["Date","email", "Contact"], col=None, alias=None, aggcol=nondettail
    )

    both = bothdifflog.filter((F.col("contact") != "") & (F.col("email") != ""))
    none = bothdifflog.filter((F.col("contact") == "") & (F.col("email") == ""))

    agglog_both = agg(
        df=both, groupcol =["Date","Email"], col="Contact", col1="UserEmail", alias="Contact", aggcol=notnondetail
    )
    agglog_none = agg(
        df=none, groupcol =["Date", F.col("UserEmail").alias("Email")], col="UserContact", col1="UserEmail", alias="Contact", aggcol=notnondetail
    )

    aggguest = agg(
        df=bothdiffguest, groupcol =["Date", "email"], col="Contact", alias="Contact", aggcol=notnondetail
    )

    aggblank = agg(
        df = blank_passed, groupcol =["Date", F.col("UserEmail").alias("Email")], col="Contact", col1 = "UserEmail", alias="Contact", aggcol=notnondetail
    )

    aggbothdiff = getUnionFiled(dfs = [agglog_none, aggguest])

    dfsfinal = [aggemail, aggcontact, aggsame, aggbothdiff, aggblank]

    finalbookingtodetailpass_2nd= getUnionFiled(dfs = dfsfinal)

    dfs = [bothdiff_failed, same_failed, contactdiff_failed, emaildiff_failed]

    finalbookingtodetailfailed_2nd = getUnionFiled(dfs=dfs, case=True)

    toMart(df=finalbookingtodetailfailed_2nd, mart=False, location=location, table="bookingtodetailfailed_2nd")
    toMart(df=finalbookingtodetailpass_2nd, catalog=catalog, table="bookingdetailpass_2nd")

    failed = spark.read.parquet(f"{location}/bookingtodetailfailed_2nd")
    failedagg = agg(df=failed, groupcol=groupcoltodetail_failed, aggcol=aggtodetail_failed)
    toMart(df=failedagg, catalog=catalog, table="aggtodetailfailed_2nd")

    return None

def main():
    searchtodetailsucessed = spark.table(f"{catalog}.searchtodetailsucessed")
    user = spark.table(f"{catalog}.user")
    searchtodetailfailed = spark.table(f"{catalog}.searchtodetailfailed_1st")
    ticketinfo = spark.table(f"{catalog}.ticketinfo")

    traceable, traceablenot = getTraceable(searchtodetailfailed=searchtodetailfailed, user=user, spark=spark)
    bothdiff, same, contactdiff, emaildiff, blanksdf = getFrames(reinetiateddtop=traceable)
    toDetail2nd(
        bothdiff=bothdiff, same=same, contactdiff=contactdiff, emaildiff=emaildiff,blanksdf=blanksdf, searchtodetailsucessed=searchtodetailsucessed, ticketinfo=ticketinfo
    )

    return None

if __name__=="__main__":
    main()