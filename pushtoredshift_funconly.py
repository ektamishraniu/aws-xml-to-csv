def PushCSVtoRedshiftTab(srcBucket, srcFile, ro_num, ro_ts):
    mytable = srcFile.split('/')[0]
    print("Table to be inserted----",mytable)
    
    conn=psycopg2.connect(dbname='dev', host='redshift-cluster-1.c1nljb0ecvas.us-east-2.redshift.amazonaws.com', port='5439', user='tbguser', password='Tbguser12')
    cur=conn.cursor()
    cur.execute("begin;")
 
    executeCom = "copy "+ mytable + " from 's3://" + srcBucket + "/" + srcFile + "' iam_role 'arn:aws:iam::526892378978:role/redshiftaccesstos3' DELIMITER '|' EMPTYASNULL IGNOREHEADER 1 TIMEFORMAT 'auto';"
    
    cur.execute(executeCom)
    
    cur.execute("commit;")
    cur.close()
    conn.close()
    return