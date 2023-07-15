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
import glob
import pprint
pp = pprint.PrettyPrinter(depth=4)
uniqekey = int(time.mktime(datetime.now().timetuple()))
timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
acid = boto3.client('sts').get_caller_identity().get('Account')

s3c = boto3.client('s3')
s3r = boto3.resource('s3')

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

    with open(cf, "w") as outfile:
        csvwriter = csv.writer(outfile, delimiter='|')
        csvwriter.writerow(keys)
        # csvwriter.writerow(values)
        values = list(map(list, zip(*values)))
        for val in values:
            csvwriter.writerow(val)

def getTabsInfo(tmpFile):
    f = open(tmpFile, 'r')
    alltabsInfo = json.load(f)
    f.close()
    return alltabsInfo


def getmaxNumOnSplt(strlist, spliton=""):
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
    try:
        return( max( list(map(int, allnums))  ) + 1 )
    except:
        return(0)


def getValsFromDict(mytab, valstr='', defval=None, loopMax=[]):
    '''
    Parameters
    ----------
    mytab : Dictionary, for tab info retrive
    valstr : String either with/without Running Number, optional
        DESCRIPTION. The default is ''.
    defval : If you would like to keep a constant value, optional
        DESCRIPTION. The default is None.
    loopMax : will be calculated using mytab else provide, optional
        DESCRIPTION. The default is -NNV0.

    Returns
    -------
    List of values of length loopMax
    '''
    #print( "loopMax--0: " ,  loopMax)
    vals = []
    if len(loopMax)<1:
        loopMax.append( range(getmaxNumOnSplt(mytab.keys()) ) )
    
    loopMaxT = reduce(lambda x, y: x*y, [ len(list(x)) for x in loopMax] )
    #print("loopMaxT: ", loopMaxT)
    
    if valstr=='':
        return [defval] * (loopMaxT)

    #print( "loopMax--1: " ,  loopMax)
    for xs in itertools.product(*loopMax):
        getvalstr = valstr
        NNV = {}
        for i in range(len(xs)):
            NNV["NNV"+str(i)] = xs[i]
        for k, v in NNV.items():
            getvalstr = getvalstr.replace(  str(k) , str(v)  )
        #print("getvalstr: ", getvalstr)
        
        try:
            #print(xs,"   getvalstr: ", getvalstr,"    mytab[getvalstr]:  ", mytab[getvalstr])
            if( isinstance(mytab[getvalstr], str) ):#replace | by , if its a string
                mytab[getvalstr] = mytab[getvalstr].replace('|',',').replace('\r', '').replace('\n', '')
            vals.append(mytab[getvalstr])
        except Exception as e:
            #print("Error while getting value for: ",getvalstr, "        Error Mesg: ",  e)
            if defval is not None:
                vals.append(defval)
            else:
                vals.append(None)
    return vals

#'RqUID', 'DocumentInfo', 'EventInfo', 'RepairOrderHeader', 'ProfileInfo', 'DamageLineInfo', 
#'RepairTotalsInfo', 'ProductionStatus', 'RepairOrderNotes', 'RepairOrderEvents'
def createROtabs(alltabsInfo, typexml, mytab, srcFile="undefinedFile.json"):    
    RqUID             = alltabsInfo.get('RqUID')
    DocumentInfo      = alltabsInfo.get('DocumentInfo')
    EventInfo         = alltabsInfo.get('EventInfo')
    RepairOrderHeader = alltabsInfo.get('RepairOrderHeader')
    ProfileInfo       = alltabsInfo.get('ProfileInfo')
    DamageLineInfo    = alltabsInfo.get('DamageLineInfo')
    RepairTotalsInfo  = alltabsInfo.get('RepairTotalsInfo')
    ProductionStatus  = alltabsInfo.get('ProductionStatus')
    RepairOrderNotes  = alltabsInfo.get('RepairOrderNotes')
    RepairOrderEvents = alltabsInfo.get('RepairOrderEvents')
    EventTopic        = alltabsInfo.get('EventTopic')    
    
    if RepairOrderEvents is None:
        RepairOrderEvents = {}
    if RepairOrderNotes is None:
        RepairOrderNotes = {}
    if ProductionStatus is None:
        ProductionStatus = {}
    if RepairTotalsInfo is None:
        RepairTotalsInfo = {}
    if DamageLineInfo is None:
        DamageLineInfo = {}
    if ProfileInfo is None:
        ProfileInfo = {}
    if RepairOrderHeader is None:
        RepairOrderHeader = {}
    if EventInfo is None:
        EventInfo = {}
    if DocumentInfo is None:
        DocumentInfo = {}
    if EventTopic is None:
        EventTopic = {}

    # Will be returning None if Tab Not present
    # pp.pprint(RepairOrderEvents)
    # pp.pprint(DocumentInfo)
    # pp.pprint(RepairOrderHeader)
    # pp.pprint(DamageLineInfo)
    # roTab = {}
    roTab = collections.OrderedDict()
    
    if(typexml == "repair_order"):
        if(mytab == "log_table"):
            maxNN = [range(1)]
            roTab["repair_order_num"] = getValsFromDict(RepairOrderHeader, "RepairOrderIDs.RepairOrderNum", None, maxNN)
            roTab["src_created_ts"] = getValsFromDict(DocumentInfo, "CreateDateTime", None, maxNN)    
            roTab["file_name"] = [srcFile]
            roTab["event_topic"] = getValsFromDict(EventTopic, "EventTopic", None, maxNN)                  
            
        if(mytab == "repair_order"):
            maxNN = [range(1)]
            roTab["repair_order_num"] = getValsFromDict(RepairOrderHeader, "RepairOrderIDs.RepairOrderNum", None, maxNN)
            roTab["estimate_doc_id"] = getValsFromDict(RepairOrderHeader, "RepairOrderIDs.EstimateDocumentID", None, maxNN)
            roTab["profile_nm"] = getValsFromDict(ProfileInfo, "ProfileName", None, maxNN)
            roTab["claim_num"] = getValsFromDict(RepairOrderHeader, "", None, maxNN)
            roTab["policy_num"] = getValsFromDict(RepairOrderHeader, "", None, maxNN)
            roTab["estimate_sts"] = getValsFromDict(RepairOrderHeader, "", None, maxNN)
            roTab["repair_order_typ"] = getValsFromDict(RepairOrderHeader, "", None, maxNN)
            roTab["referral_src_nm"] = getValsFromDict(RepairOrderHeader, "", None, maxNN)
            roTab["vendor_cd"] = getValsFromDict(RepairOrderHeader, "RepairOrderIDs.VendorCode", None, maxNN)
            roTab["loss_desc"] = getValsFromDict(RepairOrderHeader, "", None, maxNN)
            roTab["tot_loss_ind"] = getValsFromDict(RepairOrderHeader, "", None, maxNN)
            roTab["drivable_ind"] = getValsFromDict(RepairOrderHeader, "VehicleInfo.Condition.DrivableInd", None, maxNN)
            roTab["loss_primary_poi_cd"] = getValsFromDict(RepairOrderHeader, "", None, maxNN)
            roTab["loss_primary_poi_desc"] = getValsFromDict(RepairOrderHeader, "", None, maxNN)
            roTab["loss_secondary_poi_cd"] = getValsFromDict(RepairOrderHeader, "", None, maxNN)
            roTab["loss_secondary_poi_desc"] = getValsFromDict(RepairOrderHeader, "", None, maxNN)
            roTab["loss_occr_state"] = getValsFromDict(RepairOrderHeader, "", None, maxNN)
            roTab["loss_ts"] = getValsFromDict(RepairOrderHeader, "", None, maxNN)
            roTab["loss_reported_ts"] = getValsFromDict(RepairOrderHeader, "", None, maxNN)
            roTab["ro_created_ts"] = getValsFromDict(EventInfo, "RepairEvent.CreatedDateTime", None, maxNN)
            roTab["estimate_appt_ts"] = getValsFromDict(RepairOrderHeader, "", None, maxNN)
            roTab["schd_arrival_ts"] = getValsFromDict(EventInfo, "RepairEvent.ScheduledArrivalDateTime", None, maxNN)
            roTab["tgt_start_dt"] = getValsFromDict(EventInfo, "RepairEvent.TargetStartDate", None, maxNN)
            roTab["tgt_compl_ts"] = getValsFromDict(EventInfo, "RepairEvent.TargetCompletionDateTime", None, maxNN)
            roTab["requested_pickup_ts"] = getValsFromDict(EventInfo, "RepairEvent.RequestedPickUpDateTime", None, maxNN)
            roTab["arrival_ts"] = getValsFromDict(EventInfo, "RepairEvent.ArrivalDateTime", None, maxNN)
            roTab["arrival_start_dt"] = getValsFromDict(EventInfo, "RepairEvent.ActualStartDate", None, maxNN)
            roTab["actual_compl_ts"] = getValsFromDict(EventInfo, "RepairEvent.ActualCompletionDateTime", None, maxNN)
            roTab["actual_pickup_ts"] = getValsFromDict(RepairOrderHeader, "", None, maxNN)
            roTab["close_ts"] = getValsFromDict(RepairOrderHeader, "", None, maxNN)
            roTab["rental_assisted_ind"] = getValsFromDict(EventInfo, "RepairEvent.RentalAssistedInd", None, maxNN)
            roTab["arrival_odometer_reading"] = getValsFromDict(RepairOrderHeader, "", None, maxNN)
            roTab["departure_odometer_reading"] = getValsFromDict(RepairOrderHeader, "", None, maxNN)
            roTab["vehicle_vin"] = getValsFromDict(RepairOrderHeader, "", None, maxNN)
            roTab["vehicle_licesne_plate_num"] = getValsFromDict(RepairOrderHeader, "VehicleInfo.License", None, maxNN)
            roTab["vehicle_licesne_state"] = getValsFromDict(RepairOrderHeader, "", None, maxNN)
            roTab["vehicle_model_yr"] = getValsFromDict(RepairOrderHeader, "VehicleInfo.VehicleDesc.ModelYear", None, maxNN)
            roTab["vehicle_body_style"] = getValsFromDict(RepairOrderHeader, "VehicleInfo.Body.BodyStyle", None, maxNN)
            roTab["vehicle_trim_cd"] = getValsFromDict(RepairOrderHeader, "", None, maxNN)
            roTab["vehicle_production_dt"] = getValsFromDict(RepairOrderHeader, "", None, maxNN)
            roTab["vehicle_make_desc"] = getValsFromDict(RepairOrderHeader, "VehicleInfo.VehicleDesc.MakeDesc", None, maxNN)
            roTab["vehicle_model_nm"] = getValsFromDict(RepairOrderHeader, "VehicleInfo.VehicleDesc.ModelName", None, maxNN)
            roTab["vehicle_ext_color_nm"] = getValsFromDict(RepairOrderHeader, "", None, maxNN)
            roTab["vehicle_ext_color_cd"] = getValsFromDict(RepairOrderHeader, "", None, maxNN)
            roTab["vehicle_int_color_nm"] = getValsFromDict(RepairOrderHeader, "", None, maxNN)
            roTab["vehicle_int_color_cd"] = getValsFromDict(RepairOrderHeader, "", None, maxNN)
            roTab["injury_ind"] = getValsFromDict(RepairOrderHeader, "", None, maxNN)
            roTab["production_stage_cd"] = getValsFromDict(RepairOrderHeader, "", None, maxNN)
            roTab["production_stage_dt"] = getValsFromDict(RepairOrderHeader, "", None, maxNN)
            roTab["production_stage_sts_txt"] = getValsFromDict(RepairOrderHeader, "", None, maxNN)
            roTab["src_created_ts"] = getValsFromDict(DocumentInfo, "CreateDateTime", None, maxNN)
            roTab["dw_created_ts"] = getValsFromDict(RepairOrderHeader,  "",          timestamp, maxNN)
            roTab["dw_created_by"] = getValsFromDict(RepairOrderHeader,  "",               acid, maxNN)
            roTab["dw_modified_ts"] = getValsFromDict(RepairOrderHeader, "",          timestamp, maxNN)
            roTab["dw_modified_by"] = getValsFromDict(RepairOrderHeader, "",               acid, maxNN)

        if(mytab == "ro_damage_line"):
            maxNN = [range(getmaxNumOnSplt(DamageLineInfo.keys()))]
            roTab["repair_order_num"] = getValsFromDict(RepairOrderHeader, "RepairOrderIDs.RepairOrderNum", None, maxNN)
            roTab["line_num"] = getValsFromDict(DamageLineInfo, "NNV0.LineNum")
            roTab["unique_seq_num"] = getValsFromDict(DamageLineInfo, "NNV0.UniqueSequenceNum")
            roTab["supplement_num"] = getValsFromDict(DamageLineInfo, "NNV0.SupplementNum")
            roTab["estimate_ver_cd"] = getValsFromDict(DamageLineInfo, "NNV0.EstimateVerCode")
            roTab["manual_line_ind"] = getValsFromDict(DamageLineInfo, "NNV0.ManualLineInd")
            roTab["automated_entry"] = getValsFromDict(DamageLineInfo, "NNV0.AutomatedEntry")
            roTab["line_sts_cd"] = getValsFromDict(DamageLineInfo, "NNV0.LineStatusCode")
            roTab["message_cd"] = getValsFromDict(DamageLineInfo, "NNV0.MessageCode")
            roTab["vendor_ref_num"] = getValsFromDict(DamageLineInfo, "NNV0.VendorRefNum")
            roTab["line_desc"] = getValsFromDict(DamageLineInfo, "NNV0.LineDesc")
            roTab["desc_judgement_ind"] = getValsFromDict(DamageLineInfo, "NNV0.DescJudgmentInd")
            roTab["part_typ"] = getValsFromDict(DamageLineInfo, "NNV0.PartInfo.PartType")
            roTab["part_num"] = getValsFromDict(DamageLineInfo, "NNV0.PartInfo.PartNum")
            roTab["oem_part_num"] = getValsFromDict(DamageLineInfo, "NNV0.PartInfo.OEMPartNum")
            roTab["part_price"] = getValsFromDict(DamageLineInfo, "NNV0.PartInfo.PartPrice")
            roTab["unit_part_price"] = getValsFromDict(DamageLineInfo, "NNV0.PartInfo.UnitPartPrice")
            roTab["part_price_adj_typ"] = getValsFromDict(DamageLineInfo, "NNV0.PartInfo.PriceAdjustment.AdjustmentType")
            roTab["part_price_adj_pct"] = getValsFromDict(DamageLineInfo, "NNV0.PartInfo.PriceAdjustment.AdjustmentPct")
            roTab["part_price_adj_amt"] = getValsFromDict(DamageLineInfo, "NNV0.PartInfo.PriceAdjustment.AdjustmentAmt")
            roTab["part_taxable_ind"] = getValsFromDict(DamageLineInfo, "NNV0.PartInfo.TaxableInd")
            roTab["part_judgement_ind"] = getValsFromDict(DamageLineInfo, "NNV0.PartInfo.PriceJudgmentInd")
            roTab["alternate_part_ind"] = getValsFromDict(DamageLineInfo, "NNV0.PartInfo.AlternatePartInd")
            roTab["glass_part_ind"] = getValsFromDict(DamageLineInfo, "NNV0.PartInfo.GlassPartInd")
            roTab["part_price_incl_ind"] = getValsFromDict(DamageLineInfo, "NNV0.PartInfo.PriceInclInd")
            roTab["part_quantity"] = getValsFromDict(DamageLineInfo, "NNV0.PartInfo.Quantity")
            roTab["labor_typ"] = getValsFromDict(DamageLineInfo, "NNV0.LaborInfo.LaborType")
            roTab["labor_operation"] = getValsFromDict(DamageLineInfo, "NNV0.LaborInfo.LaborOperation")
            roTab["actual_labor_hrs"] = getValsFromDict(DamageLineInfo, "NNV0.LaborInfo.LaborHours")
            roTab["db_labor_hrs"] = getValsFromDict(DamageLineInfo, "NNV0.LaborInfo.DatabaseLaborHours")
            roTab["labor_incl_ind"] = getValsFromDict(DamageLineInfo, "NNV0.LaborInfo.LaborInclInd")
            roTab["labor_amt"] = getValsFromDict(DamageLineInfo, "NNV0.LaborInfo.LaborAmt")
            roTab["labor_hrs_judgement_ind"] = getValsFromDict(DamageLineInfo, "NNV0.LaborInfo.LaborHoursJudgmentInd")
            roTab["refinish_labor_typ"] = getValsFromDict(DamageLineInfo, "NNV0.RefinishLaborInfo.LaborType")
            roTab["refinish_labor_operation"] = getValsFromDict(DamageLineInfo, "NNV0.RefinishLaborInfo.LaborOperation")
            roTab["refinish_labor_hrs"] = getValsFromDict(DamageLineInfo, "NNV0.RefinishLaborInfo.LaborHours")
            roTab["refinish_db_labor_hrs"] = getValsFromDict(DamageLineInfo, "NNV0.RefinishLaborInfo.DatabaseLaborHours")
            roTab["refinish_labor_incl_ind"] = getValsFromDict(DamageLineInfo, "NNV0.RefinishLaborInfo.LaborInclInd")
            roTab["refinish_labor_amt"] = getValsFromDict(DamageLineInfo, "NNV0.RefinishLaborInfo.LaborAmt")
            roTab["refinish_labor_hrs_jdgmnt_ind"] = getValsFromDict(DamageLineInfo, "NNV0.RefinishLaborInfo.LaborHoursJudgmentInd")
            roTab["oth_charges_typ"] = getValsFromDict(DamageLineInfo, "NNV0.OtherChargesInfo.OtherChargesType")
            roTab["oth_charges_price"] = getValsFromDict(DamageLineInfo, "NNV0.OtherChargesInfo.Price")
            roTab["oth_charges_uom"] = getValsFromDict(DamageLineInfo, "NNV0.OtherChargesInfo.UnitOfMeasure")
            roTab["oth_charges_qty"] = getValsFromDict(DamageLineInfo, "NNV0.OtherChargesInfo.Quantity")
            roTab["oth_charges_price_incl_ind"] = getValsFromDict(DamageLineInfo, "NNV0.OtherChargesInfo.PriceInclInd")
            roTab["line_memo"] = getValsFromDict(DamageLineInfo, "NNV0.LineMemo")
            roTab["src_created_ts"] = getValsFromDict(DocumentInfo, "CreateDateTime", None, maxNN)
            roTab["dw_created_ts"] = getValsFromDict(DamageLineInfo, "",          timestamp, maxNN)
            roTab["dw_created_by"] = getValsFromDict(DamageLineInfo, "",               acid, maxNN)
            roTab["dw_modified_ts"] = getValsFromDict(DamageLineInfo,"",          timestamp, maxNN)
            roTab["dw_modified_by"] = getValsFromDict(DamageLineInfo,"",               acid, maxNN)
        if(mytab == "ro_event"):
            maxNN = [range(getmaxNumOnSplt(RepairOrderEvents.keys()))]
            roTab['repair_order_num'] = getValsFromDict(RepairOrderHeader, "RepairOrderIDs.RepairOrderNum", None, maxNN)
            roTab['event_typ'] = getValsFromDict(RepairOrderEvents, "RepairOrderEvent.NNV0.EventType")
            roTab['event_ts'] = getValsFromDict(RepairOrderEvents, "RepairOrderEvent.NNV0.EventDateTime")
            roTab['event_note'] = getValsFromDict(RepairOrderEvents, "RepairOrderEvent.NNV0.EventNotes")
            roTab['event_authored_by'] = getValsFromDict(RepairOrderEvents, "RepairOrderEvent.NNV0.AuthoredBy.LastName")
            roTab['src_created_ts'] = getValsFromDict(DocumentInfo, "CreateDateTime", None, maxNN)
            roTab['dw_created_ts'] = getValsFromDict(RepairOrderEvents, timestamp,          None, maxNN)
            roTab['dw_created_by'] = getValsFromDict(RepairOrderEvents, acid,               None, maxNN)
            roTab['dw_modified_ts'] = getValsFromDict(RepairOrderEvents, timestamp,         None, maxNN)
            roTab['dw_modified_by'] = getValsFromDict(RepairOrderEvents, acid,              None, maxNN)
        if(mytab == "ro_note"):
            maxNN = [range(getmaxNumOnSplt(RepairOrderNotes.keys()))]
            roTab['repair_order_num'] = getValsFromDict(RepairOrderHeader, "RepairOrderIDs.RepairOrderNum", None, maxNN)
            roTab['line_seq_num'] = getValsFromDict(RepairOrderNotes, "RepairOrderNote.NNV0.LineSequenceNum", None, maxNN)
            roTab['note_grp']     = getValsFromDict(RepairOrderNotes, "RepairOrderNote.NNV0.NoteGroup", None, maxNN)
            roTab['note_created_ts'] = getValsFromDict(RepairOrderNotes, "RepairOrderNote.NNV0.CreateDateTime", None, maxNN)
            roTab['note']            = getValsFromDict(RepairOrderNotes, "RepairOrderNote.NNV0.Note", None, maxNN)
            roTab['authored_by_first_nm'] = getValsFromDict(RepairOrderNotes, "RepairOrderNote.NNV0.AuthoredBy.FirstName", None, maxNN)
            roTab['authored_by_last_nm']  = getValsFromDict(RepairOrderNotes, "RepairOrderNote.NNV0.AuthoredBy.LastName", None, maxNN)
            roTab['authored_by_alias']    = getValsFromDict(RepairOrderNotes, "RepairOrderNote.NNV0.AuthoredBy.AliasName", None, maxNN)
            roTab['emp_id']               = getValsFromDict(RepairOrderNotes, "", None, maxNN)        
            roTab['src_created_ts']      = getValsFromDict(DocumentInfo,  "CreateDateTime", None,maxNN)
            roTab['dw_created_ts']       = getValsFromDict(ProfileInfo, "",         timestamp, maxNN)
            roTab['dw_created_by']       = getValsFromDict(ProfileInfo, "",              acid, maxNN)
            roTab['dw_modified_ts']      = getValsFromDict(ProfileInfo, "",         timestamp, maxNN)
            roTab['dw_modified_by']      = getValsFromDict(ProfileInfo, "",              acid, maxNN)
        if(mytab == "ro_rate_info"):
            maxI0 = getmaxNumOnSplt(ProfileInfo.keys())
            maxI1 = getmaxNumOnSplt(ProfileInfo.keys(), "TaxInfo.TaxTierInfo")
            maxI2 = getmaxNumOnSplt(ProfileInfo.keys(), "AdjustmentInfo")
            maxNN = [range(maxI0), range(maxI1), range(maxI2)]
            roTab['repair_order_num'] = getValsFromDict(RepairOrderHeader, "RepairOrderIDs.RepairOrderNum", None, maxNN)
            roTab['rate_typ']            = getValsFromDict(ProfileInfo, "RateInfo.NNV0.RateType", None, maxNN)
            roTab['rate_desc']           = getValsFromDict(ProfileInfo, "RateInfo.NNV0.RateDesc", None, maxNN)
            roTab['tax_typ']             = getValsFromDict(ProfileInfo, "RateInfo.NNV0.TaxInfo.TaxType", None, maxNN)
            roTab['taxable_ind']         = getValsFromDict(ProfileInfo, "RateInfo.NNV0.TaxInfo.TaxableInd", None, maxNN)
            roTab['tier_num']            = getValsFromDict(ProfileInfo, "RateInfo.NNV0.RateTierInfo.TierNum", None, maxNN)
            roTab['rate_val']            = getValsFromDict(ProfileInfo, "RateInfo.NNV0.RateTierInfo.Rate", None, maxNN)
            roTab['rate_pct']            = getValsFromDict(ProfileInfo, "RateInfo.NNV0.RateTierInfo.Percentage", None, maxNN)
            roTab['taxtierinfo_num']       = getValsFromDict(ProfileInfo, "RateInfo.NNV0.TaxInfo.TaxTierInfo.NNV1.TierNum", None, maxNN)        
            roTab['taxtierinfo_pct']       = getValsFromDict(ProfileInfo, "RateInfo.NNV0.TaxInfo.TaxTierInfo.NNV1.Percentage", None, maxNN)
            roTab['taxtierinfo_threshold_amt'] = getValsFromDict(ProfileInfo, "RateInfo.NNV0.TaxInfo.TaxTierInfo.NNV1.ThresholdAmt", None, maxNN)        
            roTab['taxtierinfo_surcharge_amt'] = getValsFromDict(ProfileInfo, "RateInfo.NNV0.TaxInfo.TaxTierInfo.NNV1.SurchargeAmt", None, maxNN)
            roTab['adjustinfo_adjust_pct'] = getValsFromDict(ProfileInfo, "RateInfo.NNV0.AdjustmentInfo.NNV2.AdjustmentPct", None, maxNN)
            roTab['adjustinfo_adjust_typ'] = getValsFromDict(ProfileInfo, "RateInfo.NNV0.AdjustmentInfo.NNV2.AdjustmentType", None, maxNN)
            roTab['matr_calc_method_cd'] = getValsFromDict(ProfileInfo, "RateInfo.NNV0.MaterialCalcSettings.CalcMethodCode", None, maxNN)
            roTab['matr_calc_max_amt']   = getValsFromDict(ProfileInfo, "RateInfo.NNV0.MaterialCalcSettings.CalcMaxAmt", None, maxNN)        
            roTab['matr_calc_max_hrs']   = getValsFromDict(ProfileInfo, "RateInfo.NNV0.MaterialCalcSettings.CalcMaxHours", None, maxNN)
            roTab['src_created_ts']      = getValsFromDict(DocumentInfo, "CreateDateTime", None, maxNN)
            roTab['dw_created_ts']       = getValsFromDict(ProfileInfo, "",         timestamp, maxNN)
            roTab['dw_created_by']       = getValsFromDict(ProfileInfo, "",              acid, maxNN)
            roTab['dw_modified_ts']      = getValsFromDict(ProfileInfo, "",         timestamp, maxNN)
            roTab['dw_modified_by']      = getValsFromDict(ProfileInfo, "",              acid, maxNN)
        if(mytab == "ro_total_info"):
            maxL = [range(getmaxNumOnSplt(RepairTotalsInfo.keys(), "LaborTotalsInfo"))]
            maxP = [range(getmaxNumOnSplt(RepairTotalsInfo.keys(), "PartsTotalsInfo"))]
            maxO = [range(getmaxNumOnSplt(RepairTotalsInfo.keys(), "OtherChargesTotalsInfo"))]
            maxS = [range(getmaxNumOnSplt(RepairTotalsInfo.keys(), "SummaryTotalsInfo"))]
            maxNN = [ range( len(maxL[0]) + len(maxP[0]) + len(maxO[0]) + len(maxS[0]) ) ]
            roTab["repair_order_num"]= getValsFromDict(RepairOrderHeader, "RepairOrderIDs.RepairOrderNum", None, maxNN)
            roTab['tot_typ_ctgry'] = getValsFromDict(RepairTotalsInfo, 'LaborTotalsInfo', "Labor", maxL)  + getValsFromDict(RepairTotalsInfo, 'PartsTotalsInfo', "Parts", maxP)  + getValsFromDict(RepairTotalsInfo, 'OtherChargesTotalsInfo', "Other", maxO)  + getValsFromDict(RepairTotalsInfo, 'SummaryTotalsInfo', "Summary", maxS)
            roTab['tot_typ'] = getValsFromDict(RepairTotalsInfo, "LaborTotalsInfo.NNV0.TotalType", None, maxL)   + getValsFromDict(RepairTotalsInfo, "PartsTotalsInfo.NNV0.TotalType", None, maxP) + getValsFromDict(RepairTotalsInfo, "OtherChargesTotalsInfo.NNV0.TotalType", None, maxO) + getValsFromDict(RepairTotalsInfo, "SummaryTotalsInfo.NNV0.TotalType", None, maxS)
            roTab['tot_sub_typ'] = getValsFromDict(RepairTotalsInfo, "LaborTotalsInfo.NNV0.TotalSubType", None, maxL) + getValsFromDict(RepairTotalsInfo, "PartsTotalsInfo.NNV0.TotalSubType", None, maxP) + getValsFromDict(RepairTotalsInfo, "OtherChargesTotalsInfo.NNV0.TotalSubType", None, maxO) + getValsFromDict(RepairTotalsInfo, "SummaryTotalsInfo.NNV0.TotalSubType", None, maxS)
            roTab['tot_typ_desc'] = getValsFromDict(RepairTotalsInfo, "LaborTotalsInfo.NNV0.TotalTypeDesc", None, maxL) + getValsFromDict(RepairTotalsInfo, "PartsTotalsInfo.NNV0.TotalTypeDesc", None, maxP) + getValsFromDict(RepairTotalsInfo, "OtherChargesTotalsInfo.NNV0.TotalTypeDesc", None, maxO) + getValsFromDict(RepairTotalsInfo, "SummaryTotalsInfo.NNV0.TotalTypeDesc", None, maxS)
            roTab['tot_hrs']  = ConvListEle( getValsFromDict(RepairTotalsInfo, "LaborTotalsInfo.NNV0.TotalHours", None, maxL) + getValsFromDict(RepairTotalsInfo, "PartsTotalsInfo.NNV0.TotalHours", None, maxP) + getValsFromDict(RepairTotalsInfo, "OtherChargesTotalsInfo.NNV0.TotalHours", None, maxO) + getValsFromDict(RepairTotalsInfo, "SummaryTotalsInfo.NNV0.TotalHours", None, maxS) )
            roTab['tot_amt']  = ConvListEle( getValsFromDict(RepairTotalsInfo, "LaborTotalsInfo.NNV0.TotalAmt", None, maxL) + getValsFromDict(RepairTotalsInfo, "PartsTotalsInfo.NNV0.TotalAmt", None, maxP) + getValsFromDict(RepairTotalsInfo, "OtherChargesTotalsInfo.NNV0.TotalAmt", None, maxO) + getValsFromDict(RepairTotalsInfo, "SummaryTotalsInfo.NNV0.TotalAmt", None, maxS) )
            roTab['tot_cost'] = ConvListEle( getValsFromDict(RepairTotalsInfo, "LaborTotalsInfo.NNV0.TotalCost", None, maxL) + getValsFromDict(RepairTotalsInfo, "PartsTotalsInfo.NNV0.TotalCost", None, maxP) + getValsFromDict(RepairTotalsInfo, "OtherChargesTotalsInfo.NNV0.TotalCost", None, maxO) + getValsFromDict(RepairTotalsInfo, "SummaryTotalsInfo.NNV0.TotalCost", None, maxS) )
            roTab['tot_adj_typ'] = getValsFromDict(RepairTotalsInfo, "LaborTotalsInfo.NNV0.TotalAdjustmentInfo.AdjustmentType", None, maxL) + getValsFromDict(RepairTotalsInfo, "PartsTotalsInfo.NNV0.TotalAdjustmentInfo.AdjustmentType", None, maxP) + getValsFromDict(RepairTotalsInfo, "OtherChargesTotalsInfo.NNV0.TotalAdjustmentInfo.AdjustmentType", None, maxO) + getValsFromDict(RepairTotalsInfo, "SummaryTotalsInfo.NNV0.TotalAdjustmentInfo.AdjustmentType", None, maxS)
            roTab['tot_adj_amt'] = ConvListEle( getValsFromDict(RepairTotalsInfo, "LaborTotalsInfo.NNV0.TotalAdjustmentInfo.TotalAdjustmentAmt", None, maxL) + getValsFromDict(RepairTotalsInfo, "PartsTotalsInfo.NNV0.TotalAdjustmentInfo.TotalAdjustmentAmt", None, maxP) + getValsFromDict(RepairTotalsInfo, "OtherChargesTotalsInfo.NNV0.TotalAdjustmentInfo.TotalAdjustmentAmt", None, maxO) + getValsFromDict(RepairTotalsInfo, "SummaryTotalsInfo.NNV0.TotalAdjustmentInfo.TotalAdjustmentAmt", None, maxS) )
            roTab['src_created_ts'] = getValsFromDict(DocumentInfo, "CreateDateTime", None,      maxNN)
            roTab['dw_created_ts']  = getValsFromDict(RepairTotalsInfo, "",               timestamp, maxNN)
            roTab['dw_created_by']  = getValsFromDict(RepairTotalsInfo, "",               acid,      maxNN)
            roTab['dw_modified_ts'] = getValsFromDict(RepairTotalsInfo, "",               timestamp, maxNN)
            roTab['dw_modified_by'] = getValsFromDict(RepairTotalsInfo, "",               acid,      maxNN)

    return roTab

def getROnum(alltabsInfo):
    roHeadTab = list(alltabsInfo.keys())[3]
    RepairOrderHeader = alltabsInfo.get(roHeadTab)
    return getValsFromDict(RepairOrderHeader, "RepairOrderIDs.RepairOrderNum", None, [range(1)])[0]

def getROcreateTimeStamp(alltabsInfo):
    docInfTab = list(alltabsInfo.keys())[1]
    DocumentInfo = alltabsInfo.get(docInfTab)
    return getValsFromDict(DocumentInfo, "CreateDateTime", None, [range(1)])[0]

def CreateCSVfileForTab(alltabsInfo, typexml, mytable, srcFile, trgBucket):
    basename = os.path.basename(srcFile)
    dn, dext = os.path.splitext(basename)
    roSaveTab=createROtabs(alltabsInfo, typexml, mytable, basename)
    #pp.pprint(roSaveTab)
    csvFile = "/tmp/" + str( typexml ) + "_" + str( mytable ) + "_" + dn + ".csv"    
    writeOutCsv(roSaveTab, csvFile)
    trgFile = str( typexml ) + "/" + str( mytable ) + "/" + dn + ".csv"
    s3r.Object(trgBucket, trgFile).put(Body=open(csvFile, 'rb'))
    print("Coping File: ", csvFile, " to ", trgBucket, trgFile)
    tmpfiles = glob.glob(os.path.join('/tmp/*'))
    print("All Files in Tmp: ", tmpfiles)
    os.remove(csvFile)

def lambda_handler(event, context):
    srcBucket="flatten-json-em"
    srcFile="repair_order/RO_3612514224_uptime.json"
    #srcFile="procurement/9060754C-C363-4F22-A127-FC46AE0FD50F_CreditMemo_Acknowledge.json"
    trgBucket="pipedelimfiles-for-table"
    #print("event: ", event)
    try:
        srcBucket=event['Records'][0]['s3']['bucket']['name']
        srcFile=event['Records'][0]['s3']['object']['key']
    except:
        print("Will be using default values from Lambda")

    print("fileName:     ", srcFile)
    print("sourceBucket: ", srcBucket)
    dn, dext = os.path.splitext(srcFile)
    workingXML = dn.split("/")[0]
    print("workingXML:  ", workingXML)
    tmpFile = '/tmp/' + os.path.basename(srcFile)
    print("tmpFile: ", tmpFile)
    s3c.download_file(srcBucket, srcFile, tmpFile)

    alltabsInfo=getTabsInfo(tmpFile)
    allkeys=list(alltabsInfo.keys())
    print("allkeys: ", allkeys)
    
    EventTopic        = alltabsInfo.get('EventTopic')    
    print( getValsFromDict(EventTopic, "EventTopic") )
    
    table_list = {}
    table_list['repair_order'] = ["repair_order", "ro_damage_line", "ro_event", "ro_note", "ro_total_info", "ro_rate_info", "log_table"]
    #table_list = ["repair_order", "ro_damage_line", "ro_event", "ro_note", "ro_total_info", "ro_rate_info", "log_table"]
    #table_list = ["repair_order", "ro_damage_line", "ro_event", "ro_note", "ro_total_info", "ro_rate_info"]
    
    for typexml, tables in table_list.items():
        if str(typexml) == str(workingXML):
            for mytable in tables:
                print(typexml, '->', mytable)
                CreateCSVfileForTab(alltabsInfo, typexml, mytable, srcFile, trgBucket)

    return {
        'statusCode': 200,
        'body': json.dumps(srcFile)
    }