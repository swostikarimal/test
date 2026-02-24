# Databricks notebook source
import pyspark.sql.functions as F
from AbandonedLogic import *

catalog = "analyticadebuddha.failedcus"

def toTicket2nd(
    bothdiffticket, sameticket, contactdiffticket, emaildiffticket, ticketinfo
):
    '''
    This creates the payment to ticket failed customer file for 1st and 2nd iter, 2nd iter sucessed customr, agg file for failed customer, agg file for pass customer
    '''
    bothdiff_ticket = bothDiffer(df1=bothdiffticket, df2=ticketinfo, df2cols = column_ticket)
    same_ticket = bothSame(df1=sameticket, df2=ticketinfo, df2cols=column_ticket)
    emaildiff_ticket = contactDiffer(df1=emaildiffticket, df2=ticketinfo, df2cols=column_ticket)
    contactdiff_ticket = emailDiffer(df1=contactdiffticket, df2=ticketinfo, df2cols=column_ticket)

    bothdiff_failed, bothdiff_passed = failedAndPassed(
        df=bothdiff_ticket, cols=subcolticket, filtercol=F.col("BD")
    )

    bothdifflog = bothdiff_passed.filter((F.col("User_id") != "0"))
    bothdiffguest = bothdiff_passed.filter((F.col("User_id") == "0"))

    same_failed, same_passed = failedAndPassed(
        df=same_ticket, cols=subcolticket, filtercol=F.col("BD")
    )

    contactdiff_failed, contactdiff_passed = failedAndPassed(
        df=contactdiff_ticket, cols=subcolticket, filtercol=F.col("BD")
    )


    emaildiff_failed, emaildiff_passed = failedAndPassed(
        df=emaildiff_ticket, cols=subcolticket, filtercol=F.col("BD")

    )

    aggemail = agg(df=emaildiff_passed, groupcol =["Date", "Contact"], col="email", alias="Email", aggcol=nonenoncolticket)
    aggcontact = agg(df=contactdiff_passed, groupcol =["Date", "email"], col="contact", alias="Contact", aggcol=nonenoncolticket)
    aggsame = agg(df=same_passed, groupcol =["Date","email", "Contact"], col=None, alias=None, aggcol=noncolticket)

    agglog = agg(df=bothdifflog, groupcol =["Date", F.col("UserEmail").alias("Email")], col="Contact", col1="UserContact", alias="Contact", aggcol=nonenoncolticket)
    aggguest = agg(df=bothdiffguest, groupcol =["Date", "email"], col="Contact", alias="Contact", aggcol=nonenoncolticket)

    aggbothdiff = getUnionFiled(dfs = [agglog, aggguest])

    dfsfinal = [aggemail, aggcontact, aggsame, aggbothdiff]

    finalpaymenttoticket = getUnionFiled(dfs = dfsfinal).select(finalcolsticket).withColumn("ticketed", F.lit(True))

    dfs = [bothdiff_failed, same_failed, contactdiff_failed, emaildiff_failed]

    failedpaymenttoticket_2nd = getUnionFiled(dfs=dfs, case=True)

    toMart(df=failedpaymenttoticket_2nd, mart=False, location=location, table="payment_to_ticketfailed_2nd")
    toMart(df=finalpaymenttoticket, catalog=catalog, table="paymenttoticketpass_2nd")

    failed = spark.read.parquet(f"{location}/payment_to_ticketfailed_2nd")
    failedagg = agg(df=failed, groupcol=groupcoltoticket_failed, aggcol=aggtoticket_failed)
    toMart(df=failedagg, catalog=catalog, table="aggtoticketfailed_2nd")

    return None
def main():
    user = spark.table(f"{catalog}.user")
    ticketinfo = spark.table(f"{catalog}.ticketinfo")
    paymenttoticketfailed = spark.table(f"{catalog}.paymenttoticketfailed_1st")

    paymenttoticketfailed1 = getUserInfo(df=paymenttoticketfailed, user=user).withColumn("date", F.date_format("PaymentDate", "y-MM-dd"))

    bothdiffticket, sameticket, contactdiffticket, emaildiffticket, emailblank= getFrames(reinetiateddtop=paymenttoticketfailed1)

    toTicket2nd(
        bothdiffticket=bothdiffticket, sameticket=sameticket, contactdiffticket=contactdiffticket, emaildiffticket=emaildiffticket, ticketinfo=ticketinfo
    )

    return None

if __name__ == "__main__":
    main()