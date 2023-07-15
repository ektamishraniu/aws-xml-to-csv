import json
import pprint;
pp = pprint.PrettyPrinter(depth=4)
import os
import fileHelper
import csv
import collections
import psycopg2
import time
from datetime import datetime
uniqekey = int(time.mktime(datetime.now().timetuple()))


def writeOutCsv(xtabInfo, cf):
    keys, values = [], []
    for key, value in xtabInfo.items():
        keys.append(key)
        values.append(value)       

    with open("/tmp/"+cf, "w") as outfile:
        csvwriter = csv.writer(outfile)
        csvwriter.writerow(keys)
        csvwriter.writerow(values)


def getTabsInfo(srcFile):
    f = open('/tmp/'+srcFile)
    alltabsInfo = json.load(f)
    f.close()
    os.remove('/tmp/'+srcFile)
    return alltabsInfo

def createROtabs(alltabsInfo, mytab):
    #['RqUID', 'DocumentInfo', 'EventInfo', 'RepairOrderHeader', 'ProfileInfo', 'DamageLineInfo', 'RepairTotalsInfo', 'RepairOrderEvents']
    rqUidTab   = list( alltabsInfo.keys() )[0]
    docInfTab  = list( alltabsInfo.keys() )[1]    
    evtInfTab  = list( alltabsInfo.keys() )[2]
    roHeadTab  = list( alltabsInfo.keys() )[3]
    profInfTab = list( alltabsInfo.keys() )[4]
    dmgLineTab = list( alltabsInfo.keys() )[5]
    repTotTab  = list( alltabsInfo.keys() )[6]
    roEvtTab   = list( alltabsInfo.keys() )[7]

    rqUidInfo  = alltabsInfo.get(rqUidTab)
    docInfInfo = alltabsInfo.get(docInfTab)
    evtInfInfo = alltabsInfo.get(evtInfTab)
    roHeadInfo  = alltabsInfo.get(roHeadTab)
    profInfInfo = alltabsInfo.get(profInfTab)
    dmgLineInfo = alltabsInfo.get(dmgLineTab)
    repTotInfo  = alltabsInfo.get(repTotTab)
    roEvtInfo   = alltabsInfo.get(roEvtTab) 
    pp.pprint(roEvtInfo)
    #pp.pprint(docInfInfo)
    #roTab = {}
    roTab = collections.OrderedDict()
    if(mytab=="roEvtTab"):
        roTab['ro_event_id']       = [uniqekey, uniqekey, uniqekey, uniqekey, uniqekey]
        roTab['event_typ']         = [roEvtInfo["RepairOrderEvent.0.EventType"],           roEvtInfo["RepairOrderEvent.1.EventType"],           roEvtInfo["RepairOrderEvent.2.EventType"],           roEvtInfo["RepairOrderEvent.3.EventType"],           roEvtInfo["RepairOrderEvent.4.EventType"] ]
        roTab['event_ts']          = [roEvtInfo["RepairOrderEvent.0.EventDateTime"],       roEvtInfo["RepairOrderEvent.1.EventDateTime"],       roEvtInfo["RepairOrderEvent.2.EventDateTime"],       roEvtInfo["RepairOrderEvent.3.EventDateTime"],       roEvtInfo["RepairOrderEvent.4.EventDateTime"] ]
        roTab['event_note']        = [roEvtInfo["RepairOrderEvent.0.EventNotes"],          roEvtInfo["RepairOrderEvent.1.EventNotes"],          roEvtInfo["RepairOrderEvent.2.EventNotes"],          roEvtInfo["RepairOrderEvent.3.EventNotes"],          roEvtInfo["RepairOrderEvent.4.EventNotes"] ]
        roTab['event_authored_by'] = [roEvtInfo["RepairOrderEvent.0.AuthoredBy.LastName"], roEvtInfo["RepairOrderEvent.1.AuthoredBy.LastName"], roEvtInfo["RepairOrderEvent.2.AuthoredBy.LastName"], None,                                                roEvtInfo["RepairOrderEvent.4.AuthoredBy.LastName"] ]
        roTab['src_created_ts']    = [docInfInfo["CreateDateTime"], docInfInfo["CreateDateTime"], docInfInfo["CreateDateTime"], docInfInfo["CreateDateTime"], docInfInfo["CreateDateTime"] ]
        roTab['dw_created_ts']     = [None, None, None, None, None]
        roTab['dw_created_by']     = [None, None, None, None, None]
        roTab['dw_modified_ts']    = [None, None, None, None, None]
        roTab['dw_modified_by']    = [None, None, None, None, None]
        roTab['repair_order_id']   = [1, 2, 3, 4, 5]
     
    if(mytab=="roOdrTab"):
        roTab["repair_order_id"]     = None
        roTab["estimate_doc_id"]     = roHeadInfo["RepairOrderIDs.EstimateDocumentID"]
        roTab["repair_order_num"]    = roHeadInfo["RepairOrderIDs.RepairOrderNum"]
        roTab["vendor_cd"]           = roHeadInfo["RepairOrderIDs.VendorCode"]
        roTab["drivable_ind"]        = roHeadInfo["VehicleInfo.Condition.DrivableInd"]
        roTab["vehicle_licesne_plate_num"]  = roHeadInfo["VehicleInfo.License"]
        roTab["vehicle_model_yr"]           = roHeadInfo["VehicleInfo.VehicleDesc.ModelYear"]
        roTab["vehicle_body_style"]         = roHeadInfo["VehicleInfo.Body.BodyStyle"]
        roTab["vehicle_make_desc"]          = roHeadInfo["VehicleInfo.VehicleDesc.MakeDesc"]
        roTab["vehicle_model_nm"]           = roHeadInfo["VehicleInfo.VehicleDesc.ModelName"]
        roTab["claim_num"]           = None
        roTab["policy_num"]          = None
        roTab["estimate_sts"]        = None
        roTab["repair_order_typ"]    = None
        roTab["referral_src_nm"]     = None
        roTab["loss_desc"]           = None
        roTab["tot_loss_ind"]        = None
        roTab["loss_primary_poi_cd"] = None
        roTab["loss_primary_poi_desc"]   = None
        roTab["loss_secondary_poi_cd"]   = None
        roTab["loss_secondary_poi_desc"] = None
        roTab["loss_occr_state"]         = None
        roTab["profile_nm"]              = profInfInfo["ProfileName"]
        roTab["estimate_appt_ts"]        = None
        roTab["loss_ts"]                 = None
        roTab["loss_reported_ts"]        = None
        roTab["ro_created_ts"]       	 = evtInfInfo["RepairEvent.CreatedDateTime"]
        roTab["schd_arrival_ts"]     	 = evtInfInfo["RepairEvent.ScheduledArrivalDateTime"]
        roTab["tgt_start_dt"]        	 = evtInfInfo["RepairEvent.TargetStartDate"]
        roTab["tgt_compl_ts"]        	 = evtInfInfo["RepairEvent.TargetCompletionDateTime"]
        roTab["requested_pickup_ts"] 	 = evtInfInfo["RepairEvent.RequestedPickUpDateTime"]
        roTab["arrival_ts"]          	 = evtInfInfo["RepairEvent.ArrivalDateTime"]
        roTab["arrival_start_dt"]    	 = evtInfInfo["RepairEvent.ActualStartDate"]
        roTab["actual_compl_ts"]     	 = evtInfInfo["RepairEvent.ActualCompletionDateTime"]
        roTab["actual_pickup_ts"]    	 = None
        roTab["close_ts"]            	 = None
        roTab["rental_assisted_ind"]     = evtInfInfo["RepairEvent.RentalAssistedInd"]
        roTab["arrival_odometer_reading"]   = None
        roTab["departure_odometer_reading"] = None
        roTab["vehicle_vin"]                = None
        roTab["vehicle_licesne_state"]      = None
        roTab["vehicle_trim_cd"]            = None
        roTab["vehicle_production_dt"]      = None
        roTab["vehicle_ext_color_nm"]       = None
        roTab["vehicle_ext_color_cd"]       = None
        roTab["vehicle_int_color_nm"]       = None
        roTab["vehicle_int_color_cd"]       = None
        roTab["injury_ind"]                 = None
        roTab["production_stage_cd"]        = None
        roTab["production_stage_dt"]        = None
        roTab["production_stage_sts_txt"]   = None
        roTab["src_created_ts"]             = docInfInfo["CreateDateTime"]
        roTab["dw_created_ts"]              = None
        roTab["dw_created_by"]              = None
        roTab["dw_modified_ts"]             = None
        roTab["dw_modified_by"]             = None
    return roTab
    

def lambda_handler(event, context):
    srcBucket = "csv-files-em"
    srcFile   = "Create.xml.json"
    trgBucket = "tables-files-em"
    print("event: ", event)

    try:
        srcBucket = event['Records'][0]['s3']['bucket']['name']
        srcFile = event['Records'][0]['s3']['object']['key']
        
    except:
        print("Will be using default values from Lambda")
        
        print("will be using local fileName:     ", srcFile)    
        print("will be using local sourceBucket: ", srcBucket)

    fileHelper.cpToTmpFolder(srcBucket, srcFile)
    alltabsInfo = getTabsInfo(srcFile)
    
    allkeys = list( alltabsInfo.keys() )
    print("allkeys: ", allkeys)
    #['RqUID', 'DocumentInfo', 'EventInfo', 'RepairOrderHeader', 'ProfileInfo', 'DamageLineInfo', 'RepairTotalsInfo', 'RepairOrderEvents']
    #docInfTab = alltabsInfo.get(allkeys[1])
    #pp.pprint(docInfTab)
    
    roEvtTab = createROtabs(alltabsInfo, "roEvtTab")
    #print("roEvtTab: ", roEvtTab)
    trgFile = srcFile + "_roEvtTab.csv"
    writeOutCsv(roEvtTab, trgFile)
    #pp.pprint(roEvtTab)
    fileHelper.cpFrmTmpToS3(trgBucket, trgFile)
    tocopycsv = 's3://'+trgBucket+'/'+trgFile 
    print("to copy csv:  ", tocopycsv)

    conn = psycopg2.connect(dbname='dev', host='redshift-cluster-1.c1nljb0ecvas.us-east-2.redshift.amazonaws.com', port='5439', user='tbguser', password='Tbguser12')
    cur = conn.cursor();
    cur.execute("begin;")
    cur.execute("copy ro_event from 's3://tables-files-em/Create.xml.json_roEvtTab.csv' iam_role 'arn:aws:iam::526892378978:role/redshiftaccesstos3' DELIMITER ',' IGNOREHEADER 1 TIMEFORMAT 'auto';")
    cur.execute("commit;")
    print("Copy executed fine!")


    '''
    roOdrTab = createROtabs(alltabsInfo, "roOdrTab")
    trgFile = srcFile + "_roOdrTab.csv"
    writeOutCsv(roOdrTab, trgFile)
    pp.pprint(roOdrTab)
    fileHelper.cpFrmTmpToS3(trgBucket, trgFile)
    '''
    
    
    print("Unique Key: ", uniqekey)
    
    
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps(srcFile)
    }