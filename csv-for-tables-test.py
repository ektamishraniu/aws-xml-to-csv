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
import pprint
pp = pprint.PrettyPrinter(depth=4)
uniqekey = int(time.mktime(datetime.now().timetuple()))
timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
acid = boto3.client('sts').get_caller_identity().get('Account')


def ConvListEle(mylist, strTo="strToFloat"):
    newlist = []
    for x in mylist:
        if(x==None):
            newlist.append(x)
        else:
            if(strTo=="strToInt"):
                newlist.append( int( float(x) ) )
            else:
                newlist.append( float(x) )
    return newlist


def writeOutCsv(xtabInfo, cf):
    keys, values = [], []
    for key, value in xtabInfo.items():
        keys.append(key)
        values.append(value)

    with open("/tmp/"+cf, "w") as outfile:
        csvwriter = csv.writer(outfile, delimiter='|')
        csvwriter.writerow(keys)
        # csvwriter.writerow(values)
        values = list(map(list, zip(*values)))
        for val in values:
            csvwriter.writerow(val)


def getTabsInfo(srcFile):
    f = open('/tmp/'+srcFile)
    alltabsInfo = json.load(f)
    f.close()
    os.remove('/tmp/'+srcFile)
    return alltabsInfo


def getMaxNumOnSplt(strlist, spliton=""):
    '''
    Parameters
    ----------
    strlist : List, List of strings.
    spliton : String, Leave blank if its the only digit OR first digit to search
                      Use substring after which you want to retrive number
        DESCRIPTION. The default is "".
    Returns: Maximum Number for the search number in list of string
    -------
    '''
    allnums = []
    for s in strlist:
        if spliton in s:
            try:
                if(spliton == ''):
                    allnums.append(re.search('[0-9]+',  s).group())
                else:
                    allnums.append(
                    re.search('[0-9]+',  str(s.split(spliton)[-1])).group())
            except:
                allnums.append(str(0))
        else:
            allnums.append(str(0))
        # print(s, allnums[-1])
    return( max( list(map(int, allnums))  ) )
    # return( 0 )


def getValsFromDict(mytab, valstr='', defval=None, loopMax=-1):
    '''
    Parameters
    ----------
    mytab : Dictionary, for tab info retrive
    valstr : String either with/without Running Number, optional
        DESCRIPTION. The default is ''.
    defval : If you would like to keep a constant value, optional
        DESCRIPTION. The default is None.
    loopMax : will be calculated using mytab else provide, optional
        DESCRIPTION. The default is -NNV.

    Returns
    -------
    List of values of length loopMax
    '''
    vals = []
    if loopMax < 0:
        loopMax = getMaxNumOnSplt(mytab.keys())
    if valstr == '':
        return [defval] * (loopMax+1)
    for i in range(loopMax+1):
        getvalstr = valstr.replace("NNV", str(i))
        try:
            mytab[getvalstr] = mytab[getvalstr].replace('|',',')
            #print(i,":   ",getvalstr,"   ",mytab[getvalstr])
            vals.append(mytab[getvalstr])
        except:
            if defval is not None:
                vals.append(defval)
            else:
                vals.append(None)
    return vals

# ['RqUID', 'DocumentInfo', 'EventInfo', 'RepairOrderHeader', 'ProfileInfo', 'DamageLineInfo', 'RepairTotalsInfo', 'RepairOrderEvents']
def createROtabs(alltabsInfo, mytab):
    rqUidTab = list(alltabsInfo.keys())[0]
    docInfTab = list(alltabsInfo.keys())[1]
    evtInfTab = list(alltabsInfo.keys())[2]
    roHeadTab = list(alltabsInfo.keys())[3]
    profInfTab = list(alltabsInfo.keys())[4]
    dmgLineTab = list(alltabsInfo.keys())[5]
    ro_total_info = list(alltabsInfo.keys())[6]
    roEvtTab = list(alltabsInfo.keys())[7]
    
    rqUidInfo = alltabsInfo.get(rqUidTab)
    docInfInfo = alltabsInfo.get(docInfTab)
    evtInfInfo = alltabsInfo.get(evtInfTab)
    roHeadInfo = alltabsInfo.get(roHeadTab)
    profInfInfo = alltabsInfo.get(profInfTab)
    dmgLineInfo = alltabsInfo.get(dmgLineTab)
    repTotInfo = alltabsInfo.get(ro_total_info)
    roEvtInfo = alltabsInfo.get(roEvtTab)
    # pp.pprint(roEvtInfo)
    # pp.pprint(docInfInfo)
    # pp.pprint(roHeadInfo)
    # pp.pprint(dmgLineInfo)
    # roTab = {}
    roTab = collections.OrderedDict()
    if(mytab == "roEvtTab"):
        roTab['ro_event_id'] = getValsFromDict(roEvtInfo, '', uniqekey)
        roTab['event_typ'] = getValsFromDict(roEvtInfo, "RepairOrderEvent.NNV.EventType")
        roTab['event_ts'] = getValsFromDict(roEvtInfo, "RepairOrderEvent.NNV.EventDateTime")
        roTab['event_note'] = getValsFromDict(roEvtInfo, "RepairOrderEvent.NNV.EventNotes")
        roTab['event_authored_by'] = getValsFromDict(roEvtInfo, "RepairOrderEvent.NNV.AuthoredBy.LastName")
        roTab['src_created_ts'] = getValsFromDict(docInfInfo, "CreateDateTime", None, getMaxNumOnSplt(roEvtInfo.keys()))
        roTab['dw_created_ts'] = getValsFromDict(roEvtInfo)
        roTab['dw_created_by'] = getValsFromDict(roEvtInfo)
        roTab['dw_modified_ts'] = getValsFromDict(roEvtInfo)
        roTab['dw_modified_by'] = getValsFromDict(roEvtInfo)
        roTab['repair_order_id'] = list(range(0,  getMaxNumOnSplt(roEvtInfo.keys())+1))

    if(mytab == "roOdrTab"):
        maxN = 0
        roTab["repair_order_id"] = getValsFromDict(roHeadInfo, '', uniqekey, maxN)
        roTab["estimate_doc_id"] = getValsFromDict(roHeadInfo, "RepairOrderIDs.EstimateDocumentID", None, maxN)
        roTab["repair_order_num"] = getValsFromDict(roHeadInfo, "RepairOrderIDs.RepairOrderNum", None, maxN)
        roTab["profile_nm"] = getValsFromDict(profInfInfo, "ProfileName", None, maxN)
        roTab["claim_num"] = getValsFromDict(roHeadInfo, "", None, maxN)
        roTab["policy_num"] = getValsFromDict(roHeadInfo, "", None, maxN)
        roTab["estimate_sts"] = getValsFromDict(roHeadInfo, "", None, maxN)
        roTab["repair_order_typ"] = getValsFromDict(roHeadInfo, "", None, maxN)
        roTab["referral_src_nm"] = getValsFromDict(roHeadInfo, "", None, maxN)
        roTab["vendor_cd"] = getValsFromDict(roHeadInfo, "RepairOrderIDs.VendorCode", None, maxN)
        roTab["loss_desc"] = getValsFromDict(roHeadInfo, "", None, maxN)
        roTab["tot_loss_ind"] = getValsFromDict(roHeadInfo, "", None, maxN)
        roTab["drivable_ind"] = getValsFromDict(roHeadInfo, "VehicleInfo.Condition.DrivableInd", None, maxN)
        roTab["loss_primary_poi_cd"] = getValsFromDict(roHeadInfo, "", None, maxN)
        roTab["loss_primary_poi_desc"] = getValsFromDict(roHeadInfo, "", None, maxN)
        roTab["loss_secondary_poi_cd"] = getValsFromDict(roHeadInfo, "", None, maxN)
        roTab["loss_secondary_poi_desc"] = getValsFromDict(roHeadInfo, "", None, maxN)
        roTab["loss_occr_state"] = getValsFromDict(roHeadInfo, "", None, maxN)
        roTab["loss_ts"] = getValsFromDict(roHeadInfo, "", None, maxN)
        roTab["loss_reported_ts"] = getValsFromDict(roHeadInfo, "", None, maxN)
        roTab["ro_created_ts"] = getValsFromDict(evtInfInfo, "RepairEvent.CreatedDateTime", None, maxN)
        roTab["estimate_appt_ts"] = getValsFromDict(roHeadInfo, "", None, maxN)
        roTab["schd_arrival_ts"] = getValsFromDict(evtInfInfo, "RepairEvent.ScheduledArrivalDateTime", None, maxN)
        roTab["tgt_start_dt"] = getValsFromDict(evtInfInfo, "RepairEvent.TargetStartDate", None, maxN)
        roTab["tgt_compl_ts"] = getValsFromDict(evtInfInfo, "RepairEvent.TargetCompletionDateTime", None, maxN)
        roTab["requested_pickup_ts"] = getValsFromDict(evtInfInfo, "RepairEvent.RequestedPickUpDateTime", None, maxN)
        roTab["arrival_ts"] = getValsFromDict(evtInfInfo, "RepairEvent.ArrivalDateTime", None, maxN)
        roTab["arrival_start_dt"] = getValsFromDict(evtInfInfo, "RepairEvent.ActualStartDate", None, maxN)
        roTab["actual_compl_ts"] = getValsFromDict(evtInfInfo, "RepairEvent.ActualCompletionDateTime", None, maxN)
        roTab["actual_pickup_ts"] = getValsFromDict(roHeadInfo, "", None, maxN)
        roTab["close_ts"] = getValsFromDict(roHeadInfo, "", None, maxN)
        roTab["rental_assisted_ind"] = getValsFromDict(evtInfInfo, "RepairEvent.RentalAssistedInd", None, maxN)
        roTab["arrival_odometer_reading"] = getValsFromDict(roHeadInfo, "", None, maxN)
        roTab["departure_odometer_reading"] = getValsFromDict(roHeadInfo, "", None, maxN)
        roTab["vehicle_vin"] = getValsFromDict(roHeadInfo, "", None, maxN)
        roTab["vehicle_licesne_plate_num"] = getValsFromDict(roHeadInfo, "VehicleInfo.License", None, maxN)
        roTab["vehicle_licesne_state"] = getValsFromDict(roHeadInfo, "", None, maxN)
        roTab["vehicle_model_yr"] = getValsFromDict(roHeadInfo, "VehicleInfo.VehicleDesc.ModelYear", None, maxN)
        roTab["vehicle_body_style"] = getValsFromDict(roHeadInfo, "VehicleInfo.Body.BodyStyle", None, maxN)
        roTab["vehicle_trim_cd"] = getValsFromDict(roHeadInfo, "", None, maxN)
        roTab["vehicle_production_dt"] = getValsFromDict(roHeadInfo, "", None, maxN)
        roTab["vehicle_make_desc"] = getValsFromDict(roHeadInfo, "VehicleInfo.VehicleDesc.MakeDesc", None, maxN)
        roTab["vehicle_model_nm"] = getValsFromDict(roHeadInfo, "VehicleInfo.VehicleDesc.ModelName", None, maxN)
        roTab["vehicle_ext_color_nm"] = getValsFromDict(roHeadInfo, "", None, maxN)
        roTab["vehicle_ext_color_cd"] = getValsFromDict(roHeadInfo, "", None, maxN)
        roTab["vehicle_int_color_nm"] = getValsFromDict(roHeadInfo, "", None, maxN)
        roTab["vehicle_int_color_cd"] = getValsFromDict(roHeadInfo, "", None, maxN)
        roTab["injury_ind"] = getValsFromDict(roHeadInfo, "", None, maxN)
        roTab["production_stage_cd"] = getValsFromDict(roHeadInfo, "", None, maxN)
        roTab["production_stage_dt"] = getValsFromDict(roHeadInfo, "", None, maxN)
        roTab["production_stage_sts_txt"] = getValsFromDict(roHeadInfo, "", None, maxN)
        roTab["src_created_ts"] = getValsFromDict(docInfInfo, "CreateDateTime", None, maxN)
        roTab["dw_created_ts"] = getValsFromDict(roHeadInfo, "", timestamp, maxN)
        roTab["dw_created_by"] = getValsFromDict(roHeadInfo, "", acid, maxN)
        roTab["dw_modified_ts"] = getValsFromDict(roHeadInfo, "", timestamp, maxN)
        roTab["dw_modified_by"] = getValsFromDict(roHeadInfo, "", acid, maxN)

    if(mytab == "dmgLineTab"):
        roTab["ro_damage_line_id"] = getValsFromDict(dmgLineInfo, '', uniqekey)
        roTab["line_num"] = getValsFromDict(dmgLineInfo, "NNV.LineNum")
        roTab["unique_seq_num"] = getValsFromDict(dmgLineInfo, "NNV.UniqueSequenceNum")
        roTab["supplement_num"] = getValsFromDict(dmgLineInfo, "NNV.SupplementNum")
        roTab["estimate_ver_cd"] = getValsFromDict(dmgLineInfo, "NNV.EstimateVerCode")
        roTab["manual_line_ind"] = getValsFromDict(dmgLineInfo, "NNV.ManualLineInd")
        roTab["automated_entry"] = getValsFromDict(dmgLineInfo, "NNV.AutomatedEntry")
        roTab["line_sts_cd"] = getValsFromDict(dmgLineInfo, "NNV.LineStatusCode")
        roTab["message_cd"] = getValsFromDict(dmgLineInfo, "NNV.MessageCode")
        roTab["vendor_ref_num"] = getValsFromDict(dmgLineInfo, "NNV.VendorRefNum")
        roTab["line_desc"] = getValsFromDict(dmgLineInfo, "NNV.LineDesc")
        roTab["desc_judgement_ind"] = getValsFromDict(dmgLineInfo, "NNV.DescJudgmentInd")
        roTab["part_typ"] = getValsFromDict(dmgLineInfo, "NNV.PartInfo.PartType")
        roTab["part_num"] = getValsFromDict(dmgLineInfo, "NNV.PartInfo.PartNum")
        roTab["oem_part_num"] = getValsFromDict(dmgLineInfo, "NNV.PartInfo.OEMPartNum")
        roTab["part_price"] = getValsFromDict(dmgLineInfo, "NNV.PartInfo.PartPrice")
        roTab["unit_part_price"] = getValsFromDict(dmgLineInfo, "NNV.PartInfo.UnitPartPrice")
        roTab["part_price_adj_typ"] = getValsFromDict(dmgLineInfo, "NNV.PartInfo.PriceAdjustment.AdjustmentType")
        roTab["part_price_adj_pct"] = getValsFromDict(dmgLineInfo, "NNV.PartInfo.PriceAdjustment.AdjustmentPct")
        roTab["part_price_adj_amt"] = getValsFromDict(dmgLineInfo, "NNV.PartInfo.PriceAdjustment.AdjustmentAmt")
        roTab["part_taxable_ind"] = getValsFromDict(dmgLineInfo, "NNV.PartInfo.TaxableInd")
        roTab["part_judgement_ind"] = getValsFromDict(dmgLineInfo, "NNV.PartInfo.PriceJudgmentInd")
        roTab["alternate_part_ind"] = getValsFromDict(dmgLineInfo, "NNV.PartInfo.AlternatePartInd")
        roTab["glass_part_ind"] = getValsFromDict(dmgLineInfo, "NNV.PartInfo.GlassPartInd")
        roTab["part_price_incl_ind"] = getValsFromDict(dmgLineInfo, "NNV.PartInfo.PriceInclInd")
        roTab["part_quantity"] = getValsFromDict(dmgLineInfo, "NNV.PartInfo.Quantity")
        roTab["labor_typ"] = getValsFromDict(dmgLineInfo, "NNV.LaborInfo.LaborType")
        roTab["labor_operation"] = getValsFromDict(dmgLineInfo, "NNV.LaborInfo.LaborOperation")
        roTab["actual_labor_hrs"] = getValsFromDict(dmgLineInfo, "NNV.LaborInfo.LaborHours")
        roTab["db_labor_hrs"] = getValsFromDict(dmgLineInfo, "NNV.LaborInfo.DatabaseLaborHours")
        roTab["labor_incl_ind"] = getValsFromDict(dmgLineInfo, "NNV.LaborInfo.LaborInclInd")
        roTab["labor_amt"] = getValsFromDict(dmgLineInfo, "NNV.LaborInfo.LaborAmt")
        roTab["labor_hrs_judgement_ind"] = getValsFromDict(dmgLineInfo, "NNV.LaborInfo.LaborHoursJudgmentInd")
        roTab["refinish_labor_typ"] = getValsFromDict(dmgLineInfo, "NNV.RefinishLaborInfo.LaborType")
        roTab["refinish_labor_operation"] = getValsFromDict(dmgLineInfo, "NNV.RefinishLaborInfo.LaborOperation")
        roTab["refinish_labor_hrs"] = getValsFromDict(dmgLineInfo, "NNV.RefinishLaborInfo.LaborHours")
        roTab["refinish_db_labor_hrs"] = getValsFromDict(dmgLineInfo, "NNV.RefinishLaborInfo.DatabaseLaborHours")
        roTab["refinish_labor_incl_ind"] = getValsFromDict(dmgLineInfo, "NNV.RefinishLaborInfo.LaborInclInd")
        roTab["refinish_labor_amt"] = getValsFromDict(dmgLineInfo, "NNV.RefinishLaborInfo.LaborAmt")
        roTab["refinish_labor_hrs_jdgmnt_ind"] = getValsFromDict(dmgLineInfo, "NNV.RefinishLaborInfo.LaborHoursJudgmentInd")
        roTab["oth_charges_typ"] = getValsFromDict(dmgLineInfo, "NNV.OtherChargesInfo.OtherChargesType")
        roTab["oth_charges_price"] = getValsFromDict(dmgLineInfo, "NNV.OtherChargesInfo.Price")
        roTab["oth_charges_uom"] = getValsFromDict(dmgLineInfo, "NNV.OtherChargesInfo.UnitOfMeasure")
        roTab["oth_charges_qty"] = getValsFromDict(dmgLineInfo, "NNV.OtherChargesInfo.Quantity")
        roTab["oth_charges_price_incl_ind"] = getValsFromDict(dmgLineInfo, "NNV.OtherChargesInfo.PriceInclInd")
        roTab["line_memo"] = getValsFromDict(dmgLineInfo, "NNV.LineMemo")
        roTab["src_created_ts"] = getValsFromDict(docInfInfo, "CreateDateTime", None, getMaxNumOnSplt(dmgLineInfo.keys()))
        roTab["dw_created_ts"] = getValsFromDict(dmgLineInfo, timestamp)
        roTab["dw_created_by"] = getValsFromDict(dmgLineInfo, acid)
        roTab["dw_modified_ts"] = getValsFromDict(dmgLineInfo, timestamp)
        roTab["dw_modified_by"] = getValsFromDict(dmgLineInfo, acid)
        roTab["repair_order_id"] = list(range(0,  getMaxNumOnSplt(dmgLineInfo.keys())+1))

    if(mytab == "ro_total_info"):
        maxL = getMaxNumOnSplt(repTotInfo.keys(), "LaborTotalsInfo")
        maxP = getMaxNumOnSplt(repTotInfo.keys(), "PartsTotalsInfo")
        maxO = getMaxNumOnSplt(repTotInfo.keys(), "OtherChargesTotalsInfo")
        maxS = getMaxNumOnSplt(repTotInfo.keys(), "SummaryTotalsInfo")    
        roTab['ro_total_info'] = getValsFromDict(repTotInfo, 'LaborTotalsInfo',uniqekey, maxL) + getValsFromDict(repTotInfo, 'PartsTotalsInfo',uniqekey, maxP) + getValsFromDict(repTotInfo, 'OtherChargesTotalsInfo',uniqekey, maxO) + getValsFromDict(repTotInfo, 'SummaryTotalsInfo',uniqekey, maxS)
        roTab['tot_typ_ctgry'] = getValsFromDict(repTotInfo, 'LaborTotalsInfo', "Labor", maxL)  + getValsFromDict(repTotInfo, 'PartsTotalsInfo', "Parts", maxP)  + getValsFromDict(repTotInfo, 'OtherChargesTotalsInfo', "Other", maxO)  + getValsFromDict(repTotInfo, 'SummaryTotalsInfo', "Summary", maxS)
        roTab['tot_typ'] = getValsFromDict(repTotInfo, "LaborTotalsInfo.NNV.TotalType", None, maxL)   + getValsFromDict(repTotInfo, "PartsTotalsInfo.NNV.TotalType", None, maxP) + getValsFromDict(repTotInfo, "OtherChargesTotalsInfo.NNV.TotalType", None, maxO) + getValsFromDict(repTotInfo, "SummaryTotalsInfo.NNV.TotalType", None, maxS)
        roTab['tot_sub_typ'] = getValsFromDict(repTotInfo, "LaborTotalsInfo.NNV.TotalSubType", None, maxL) + getValsFromDict(repTotInfo, "PartsTotalsInfo.NNV.TotalSubType", None, maxP) + getValsFromDict(repTotInfo, "OtherChargesTotalsInfo.NNV.TotalSubType", None, maxO) + getValsFromDict(repTotInfo, "SummaryTotalsInfo.NNV.TotalSubType", None, maxS)
        roTab['tot_typ_desc'] = getValsFromDict(repTotInfo, "LaborTotalsInfo.NNV.TotalTypeDesc", None, maxL) + getValsFromDict(repTotInfo, "PartsTotalsInfo.NNV.TotalTypeDesc", None, maxP) + getValsFromDict(repTotInfo, "OtherChargesTotalsInfo.NNV.TotalTypeDesc", None, maxO) + getValsFromDict(repTotInfo, "SummaryTotalsInfo.NNV.TotalTypeDesc", None, maxS)
        roTab['tot_hrs']  = ConvListEle( getValsFromDict(repTotInfo, "LaborTotalsInfo.NNV.TotalHours", None, maxL) + getValsFromDict(repTotInfo, "PartsTotalsInfo.NNV.TotalHours", None, maxP) + getValsFromDict(repTotInfo, "OtherChargesTotalsInfo.NNV.TotalHours", None, maxO) + getValsFromDict(repTotInfo, "SummaryTotalsInfo.NNV.TotalHours", None, maxS) )
        roTab['tot_amt']  = ConvListEle( getValsFromDict(repTotInfo, "LaborTotalsInfo.NNV.TotalAmt", None, maxL) + getValsFromDict(repTotInfo, "PartsTotalsInfo.NNV.TotalAmt", None, maxP) + getValsFromDict(repTotInfo, "OtherChargesTotalsInfo.NNV.TotalAmt", None, maxO) + getValsFromDict(repTotInfo, "SummaryTotalsInfo.NNV.TotalAmt", None, maxS) )
        roTab['tot_cost'] = ConvListEle( getValsFromDict(repTotInfo, "LaborTotalsInfo.NNV.TotalCost", None, maxL) + getValsFromDict(repTotInfo, "PartsTotalsInfo.NNV.TotalCost", None, maxP) + getValsFromDict(repTotInfo, "OtherChargesTotalsInfo.NNV.TotalCost", None, maxO) + getValsFromDict(repTotInfo, "SummaryTotalsInfo.NNV.TotalCost", None, maxS) )
        roTab['tot_adj_typ'] = getValsFromDict(repTotInfo, "LaborTotalsInfo.NNV.TotalAdjustmentInfo.AdjustmentType", None, maxL) + getValsFromDict(repTotInfo, "PartsTotalsInfo.NNV.TotalAdjustmentInfo.AdjustmentType", None, maxP) + getValsFromDict(repTotInfo, "OtherChargesTotalsInfo.NNV.TotalAdjustmentInfo.AdjustmentType", None, maxO) + getValsFromDict(repTotInfo, "SummaryTotalsInfo.NNV.TotalAdjustmentInfo.AdjustmentType", None, maxS)
        roTab['tot_adj_amt'] = ConvListEle( getValsFromDict(repTotInfo, "LaborTotalsInfo.NNV.TotalAdjustmentInfo.TotalAdjustmentAmt", None, maxL) + getValsFromDict(repTotInfo, "PartsTotalsInfo.NNV.TotalAdjustmentInfo.TotalAdjustmentAmt", None, maxP) + getValsFromDict(repTotInfo, "OtherChargesTotalsInfo.NNV.TotalAdjustmentInfo.TotalAdjustmentAmt", None, maxO) + getValsFromDict(repTotInfo, "SummaryTotalsInfo.NNV.TotalAdjustmentInfo.TotalAdjustmentAmt", None, maxS) )
        roTab['src_created_ts'] = getValsFromDict(docInfInfo, "CreateDateTime", None,     (maxL + maxP + maxO + maxS)*getMaxNumOnSplt(repTotInfo.keys()))
        roTab['dw_created_ts']  = getValsFromDict(repTotInfo, "",               timestamp,(maxL + maxP + maxO + maxS)*getMaxNumOnSplt(repTotInfo.keys()))
        roTab['dw_created_by']  = getValsFromDict(repTotInfo, "",               acid,     (maxL + maxP + maxO + maxS)*getMaxNumOnSplt(repTotInfo.keys()))
        roTab['dw_modified_ts'] = getValsFromDict(repTotInfo, "",               timestamp,(maxL + maxP + maxO + maxS)*getMaxNumOnSplt(repTotInfo.keys()))
        roTab['dw_modified_by'] = getValsFromDict(repTotInfo, "",               acid,     (maxL + maxP + maxO + maxS)*getMaxNumOnSplt(repTotInfo.keys()))
        roTab['repair_order_id']= getValsFromDict(repTotInfo, "",               uniqekey, (maxL + maxP + maxO + maxS)*getMaxNumOnSplt(repTotInfo.keys()))
    return roTab


def PushCSVtoRedshiftTab(trgBucket, trgFile, redshiftTab):
    conn=psycopg2.connect(dbname='dev', host='redshift-cluster-1.c1nljb0ecvas.us-east-2.redshift.amazonaws.com', port='5439', user='tbguser', password='Tbguser12')
    cur=conn.cursor()
    cur.execute("begin;")
    executeCom = "copy "+ redshiftTab+ " from 's3://" + trgBucket + "/" + trgFile + "' iam_role 'arn:aws:iam::526892378978:role/redshiftaccesstos3' DELIMITER '|' EMPTYASNULL IGNOREHEADER 1 TIMEFORMAT 'auto';"
    print("excuteCom: ", executeCom)
    cur.execute(executeCom)
    cur.execute("commit;")
    print("Copy executed fine!")    
    

def CreateCSVfileForTab(alltabsInfo, redshiftTab, srcBucket, srcFile, trgBucket):
    roSaveTab=createROtabs(alltabsInfo, redshiftTab)
    #pp.pprint(roSaveTab)
    trgFile = srcFile.replace(".json", "_.json").replace(".json", redshiftTab) + ".csv"
    writeOutCsv(roSaveTab, trgFile)
    fileHelper.cpFrmTmpToS3(trgBucket, trgFile)
    tocopycsv='s3://'+trgBucket+'/'+trgFile
    print("to copy csv:  ", tocopycsv)
    PushCSVtoRedshiftTab(trgBucket, trgFile, redshiftTab)


def lambda_handler(event, context):
    srcBucket="csv-files-em"
    srcFile="Create.xml.json"
    srcFile="RO_3612514224.json"
    trgBucket="tables-files-em"
    print("event: ", event)

    try:
        srcBucket=event['Records'][0]['s3']['bucket']['name']
        srcFile=event['Records'][0]['s3']['object']['key']

    except:
        print("Will be using default values from Lambda")

        print("will be using local fileName:     ", srcFile)
        print("will be using local sourceBucket: ", srcBucket)

    fileHelper.cpToTmpFolder(srcBucket, srcFile)
    alltabsInfo=getTabsInfo(srcFile)

    allkeys=list(alltabsInfo.keys())
    print("allkeys: ", allkeys)
    # pp.pprint(docInfTab)

    if(False):
        roEvtTab=createROtabs(alltabsInfo, "roEvtTab")
        #pp.pprint(roEvtTab)
        trgFile=srcFile + "_roEvtTab.csv"
        writeOutCsv(roEvtTab, trgFile)
        # pp.pprint(roEvtTab)
        fileHelper.cpFrmTmpToS3(trgBucket, trgFile)
        tocopycsv='s3://'+trgBucket+'/'+trgFile
        print("to copy csv:  ", tocopycsv)
        conn=psycopg2.connect(dbname='dev', host='redshift-cluster-1.c1nljb0ecvas.us-east-2.redshift.amazonaws.com',
                                port='5439', user='tbguser', password='Tbguser12')
        cur=conn.cursor()
        cur.execute("begin;")
        cur.execute("copy ro_event from 's3://tables-files-em/Create.xml.json_roEvtTab.csv' iam_role 'arn:aws:iam::526892378978:role/redshiftaccesstos3' DELIMITER ',' IGNOREHEADER 1 TIMEFORMAT 'auto';")
        cur.execute("commit;")
        print("Copy executed fine!")

    if(False):
        roOdrTab=createROtabs(alltabsInfo, "roOdrTab")
        #pp.pprint(roOdrTab)
        trgFile=srcFile + "_roOdrTab.csv"
        writeOutCsv(roOdrTab, trgFile)
        fileHelper.cpFrmTmpToS3(trgBucket, trgFile)
        tocopycsv='s3://'+trgBucket+'/'+trgFile
        print("to copy csv:  ", tocopycsv)
        conn=psycopg2.connect(dbname='dev', host='redshift-cluster-1.c1nljb0ecvas.us-east-2.redshift.amazonaws.com',
                                port='5439', user='tbguser', password='Tbguser12')
        cur=conn.cursor()
        cur.execute("begin;")
        cur.execute("copy repair_order from 's3://tables-files-em/Create.json_roOdrTab.csv' iam_role 'arn:aws:iam::526892378978:role/redshiftaccesstos3' DELIMITER ',' IGNOREHEADER 1 TIMEFORMAT 'auto';")
        cur.execute("commit;")
        print("Copy executed fine!")

    if(False):
        dmgLineTab=createROtabs(alltabsInfo, "dmgLineTab")
        #pp.pprint(dmgLineTab)
        trgFile=srcFile + "_dmgLineTab.csv"
        writeOutCsv(dmgLineTab, trgFile)
        fileHelper.cpFrmTmpToS3(trgBucket, trgFile)
        tocopycsv='s3://'+trgBucket+'/'+trgFile
        print("to copy csv:  ", tocopycsv)
        conn=psycopg2.connect(dbname='dev', host='redshift-cluster-1.c1nljb0ecvas.us-east-2.redshift.amazonaws.com',
                                port='5439', user='tbguser', password='Tbguser12')
        cur=conn.cursor()
        cur.execute("begin;")
        cur.execute("copy ro_damage_line from 's3://tables-files-em/Create.xml.json_dmgLineTab.csv' iam_role 'arn:aws:iam::526892378978:role/redshiftaccesstos3' DELIMITER ',' NULL AS None IGNOREHEADER 1 TIMEFORMAT 'auto';")
        cur.execute("commit;")
        print("Copy executed fine!")

    if(False):
        ro_total_info=createROtabs(alltabsInfo, "ro_total_info")
        #pp.pprint(ro_total_info)
        trgFile=srcFile.replace(".json", "_.json").replace(".json", "ro_total_info.csv")
        writeOutCsv(ro_total_info, trgFile)
        fileHelper.cpFrmTmpToS3(trgBucket, trgFile)
        tocopycsv='s3://'+trgBucket+'/'+trgFile
        print("to copy csv:  ", tocopycsv)
        conn=psycopg2.connect(dbname='dev', host='redshift-cluster-1.c1nljb0ecvas.us-east-2.redshift.amazonaws.com',
                                port='5439', user='tbguser', password='Tbguser12')
        cur=conn.cursor()
        cur.execute("begin;")
        cur.execute("copy ro_total_info from 's3://tables-files-em/RO_3612514224_ro_total_info.csv' iam_role 'arn:aws:iam::526892378978:role/redshiftaccesstos3' DELIMITER '|' EMPTYASNULL IGNOREHEADER 1 TIMEFORMAT 'auto';")
        cur.execute("commit;")
        print("Copy executed fine!")

    if(True):
        CreateCSVfileForTab(alltabsInfo, "ro_total_info", srcBucket, srcFile, trgBucket)

    print("Unique Key: ", uniqekey)
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps(srcFile)
    }