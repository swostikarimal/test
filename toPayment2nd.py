# Databricks notebook source
import pyspark.sql.functions as F
from AbandonedLogic import *

catalog = "analyticadebuddha.failedcus"

def toPayment2nd(
    bothdiffpaymemnt, samepayment, contactdiffpayment, emaildiffpayment, blankpayment, detailedpayment, ticketinfo
):
    '''
    This creates the detail to payment failed customer file for 1st and 2nd iter, 2nd iter sucessed customr, agg file for failed customer, agg file for pass customer
    '''
    bothdiff_paymemnt = bothDiffer(
        df1=bothdiffpaymemnt, df2=detailedpayment, df2cols = column_payment
    )

    same_payment = bothSame(
        df1=samepayment, df2=detailedpayment, df2cols=column_payment
    )

    emaildiff_payment = contactDiffer(
        df1=emaildiffpayment, df2=detailedpayment, df2cols=column_payment
    )
    contactdiff_payment = emailDiffer(
        df1=contactdiffpayment, df2=detailedpayment, df2cols=column_payment
    )
    blanks_payment= blanks(
        df1=blankpayment, df2=detailedpayment, df2cols=column_payment
    )

    bothdiff_failed, bothdiff_passed = failedAndPassed(
        df=bothdiff_paymemnt, cols=subcolpayment, filtercol=F.col("BD"),
        info=True, ticketdata = ticketinfo, id = F.col("flight_booking_id")
    )

    bothdifflog = bothdiff_passed.filter(
        (F.col("User_id") != "0")
    )
    bothdiffguest = bothdiff_passed.filter(
        (F.col("User_id") == "0")
    )

    same_failed, same_passed = failedAndPassed(
        df=same_payment, cols=subcolpayment, filtercol=F.col("BD"),
        info=True, ticketdata = ticketinfo, id = F.col("flight_booking_id")
    )

    contactdiff_failed, contactdiff_passed = failedAndPassed(
        df=contactdiff_payment, cols=subcolpayment, filtercol=F.col("BD"),
        info=True, ticketdata = ticketinfo, id = F.col("flight_booking_id")
    )

    emaildiff_failed, emaildiff_passed = failedAndPassed(
        df=emaildiff_payment, cols=subcolpayment, filtercol=F.col("BD"),
        info=True, ticketdata = ticketinfo, id = F.col("flight_booking_id")

    )

    blank_failed, blank_passed = failedAndPassed(
        df=blanks_payment, cols=subcolpayment, filtercol=F.col("BD"),
        info=True, ticketdata = ticketinfo, id = F.col("flight_booking_id")

    )

    aggemail = agg(
        df=emaildiff_passed, groupcol =["Date", "Contact"], col="email", alias="Email", aggcol=notnonpayment
    )
    aggcontact = agg(
        df=contactdiff_passed, groupcol =["Date", "email"], col="contact", alias="Contact", aggcol=notnonpayment
    )
    aggsame = agg(
        df=same_passed, groupcol =["Date","email", "Contact"], col=None, alias=None, aggcol=nonpayment
    )

    agglog = agg(
        df=bothdifflog, groupcol =["Date", F.col("UserEmail").alias("Email")], col="Contact", col1="UserContact", alias="Contact", aggcol=notnonpayment
    )
    aggguest = agg(
        df=bothdiffguest, groupcol =["Date", "email"], col="Contact", alias="Contact", aggcol=notnonpayment
    )

    aggblank = agg(
        df = blank_passed, groupcol =["Date", F.col("UserEmail").alias("Email")], col="Contact", col1 = "UserEmail", alias="Contact", aggcol=notnonpayment
    )

    aggbothdiff = getUnionFiled(dfs = [agglog, aggguest])

    dfsfinal = [aggemail, aggcontact, aggsame, aggbothdiff, aggblank]

    finaldetailtopaymentpass_2nd= getUnionFiled(dfs = dfsfinal).select(*finalcolspayment)

    dfs = [bothdiff_failed, same_failed, contactdiff_failed, emaildiff_failed]

    finaldetailtopayment_2nd = getUnionFiled(dfs=dfs, case=True)

    toMart(df=finaldetailtopayment_2nd, mart=False, location=location, table="detailtopaymentfailed_2nd")
    toMart(df=finaldetailtopaymentpass_2nd, catalog=catalog, table="detailtopaymentpass_2nd")

    failed = spark.read.parquet(f"{location}/detailtopaymentfailed_2nd")
    failedagg = agg(df=failed, groupcol=groupcoltopayment_failed, aggcol=aggtopayment_failed)
    toMart(df=failedagg, catalog=catalog, table="aggtopaymentfailed_2nd")

    return None

def main():
    user = spark.table(f"{catalog}.user")
    ticketinfo = spark.table(f"{catalog}.ticketinfo")
    detailedpayment = spark.table(f"{catalog}.detailedpaymentsucessed")
    detailtopaymentfailed = spark.table(f"{catalog}.detailtopaymentfailed_1st")

    reinetiateddtop = getUserInfo(df=detailtopaymentfailed, user=user)
    bothdiffpaymemnt, samepayment, contactdiffpayment, emaildiffpayment, blankpayment = getFrames(reinetiateddtop=reinetiateddtop)
    toPayment2nd(
        bothdiffpaymemnt=bothdiffpaymemnt, samepayment=samepayment, contactdiffpayment=contactdiffpayment, emaildiffpayment=emaildiffpayment, blankpayment=blankpayment, detailedpayment=detailedpayment, ticketinfo=ticketinfo
    )

if __name__ == "__main__":
    main()