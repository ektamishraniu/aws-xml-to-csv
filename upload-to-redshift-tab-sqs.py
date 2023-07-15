from datetime import datetime
import boto3
import time
import psycopg2
import collections
import re
import csv
import fileHelper
import os
import json
from functools import reduce
import itertools
import ast

def PushCSVtoRedshiftTab(srcBucket, srcFile, ro_num, ro_ts):
    mytable = srcFile.split('/')[0]
    print("working for file: "+srcBucket+"/"+srcFile)
    print("May be inserted to table: "+mytable+"  for ro_num and ro_ts  "+str(ro_num)+"  "+str(ro_ts))
    conn=psycopg2.connect(dbname='dev', host='redshift-cluster-1.c1nljb0ecvas.us-east-2.redshift.amazonaws.com', port='5439', user='tbguser', password='Tbguser12')
    cur=conn.cursor()
    cur.execute("begin;")
    
    queryS = "select repair_order_num from "+ mytable +" where repair_order_num =" + "'"+str(ro_num)+"'" + " ;"        
    print(queryS)
    cur.execute(queryS)
    rows = cur.fetchall()
    if not len(rows):
        print("repair_order_num: "+str(ro_num)+" is not in DB, Insert then EXIT! ")
        executeCom = "copy "+ mytable + " from 's3://" + srcBucket + "/" + srcFile + "' iam_role 'arn:aws:iam::526892378978:role/redshiftaccesstos3' DELIMITER '|' EMPTYASNULL IGNOREHEADER 1 TIMEFORMAT 'auto';"
        cur.execute(executeCom)
        cur.execute("commit;")
        cur.close()
        conn.close()
        return
    else:
        print("repair_order_num: "+str(ro_num)+" is in DB, good for next step...")
        queryS = "select src_created_ts from "+ mytable +" where repair_order_num =" + "'" + str(ro_num) + "'" +" and src_created_ts >= " +"'" + str(ro_ts) + "'" +" ;"
        print(queryS)
        cur.execute(queryS)
        rows = cur.fetchall()
        if len(rows):
            print("Current TS: "+ro_ts+" is less or equal to DB TS: "+str(rows))
            print("Donot do anything!! EXIT")
            cur.close()
            conn.close()
            return
        else:
            print("Current TS: "+ro_ts+" is greater than atleast one of Database TS: "+str(rows))
            print("Delete All entries for repair_order_num: "+str(ro_num))
            delQuery = "delete from "+ mytable +" where repair_order_num = " + str(ro_num) + ";" 
            cur.execute(delQuery)
            conn.commit()
            print("Insert current entry for current TS:     "+str(ro_ts)+"  then Exit!")
            executeCom = "copy "+ mytable + " from 's3://" + srcBucket + "/" + srcFile + "' iam_role 'arn:aws:iam::526892378978:role/redshiftaccesstos3' DELIMITER '|' EMPTYASNULL IGNOREHEADER 1 TIMEFORMAT 'auto';"
            cur.execute(executeCom)
            cur.execute("commit;")
            cur.close()
            conn.close()
            return

def getROTS(filename):
    ro_num = "repair_order_num"
    ro_ts = "src_created_ts"
    filename = "/tmp/" + filename.strip().split('/')[-1]
    f = [i.strip('\n').split("|") for i in open(filename, 'r')]
    ro_num_ind = f[0].index(ro_num)
    ro_ts_ind = f[0].index(ro_ts)

    f = open(filename, 'r')
    lineone = f.readlines()[1].strip() 
    f.close()

    fields = lineone.split('|')
    return (fields[ro_num_ind], fields[ro_ts_ind])

def lambda_handler(event, context):
    evtFrmSqs = ast.literal_eval( event['Records'][0]['body'] )['Records'][0]['s3']
    srcBucket = evtFrmSqs['bucket']['name']
    srcFile = evtFrmSqs['object']['key']
    print("fileName:     ", srcFile)
    print("event: ", event)
    print("event: ", evtFrmSqs)
    
    fileHelper.cpToTmpFolder(srcBucket, srcFile)
    ro_num, ro_ts = getROTS(srcFile)
    print("ro_num = " +  str( ro_num) )
    print("ro_ts  = " +  str( ro_ts ) )
    
    while True:
        try:
            PushCSVtoRedshiftTab(srcBucket, srcFile, ro_num, ro_ts)        
            break
        except Exception as e:
            print("PushCSVtoRedshiftTab Failed: " + str(e)+" Sleeping for 10 sec before retrying")
            time.sleep(10)
    
    return {
        'statusCode': 200,
        'body': json.dumps(srcFile)
    }