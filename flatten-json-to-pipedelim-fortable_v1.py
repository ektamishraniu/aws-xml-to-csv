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
        loopMax = getmaxNumOnSplt(mytab.keys())
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
    ro_rate_info = list(alltabsInfo.keys())[4]
    ro_damage_line = list(alltabsInfo.keys())[5]
    ro_total_info = list(alltabsInfo.keys())[6]
    ro_event = list(alltabsInfo.keys())[7]
    
    rqUidInfo = alltabsInfo.get(rqUidTab)
    docInfInfo = alltabsInfo.get(docInfTab)
    evtInfInfo = alltabsInfo.get(evtInfTab)
    roHeadInfo = alltabsInfo.get(roHeadTab)
    profInfInfo = alltabsInfo.get(ro_rate_info)
    dmgLineInfo = alltabsInfo.get(ro_damage_line)
    repTotInfo = alltabsInfo.get(ro_total_info)
    roEvtInfo = alltabsInfo.get(ro_event)
    # pp.pprint(roEvtInfo)
    # pp.pprint(docInfInfo)
    # pp.pprint(roHeadInfo)
    # pp.pprint(dmgLineInfo)
    # roTab = {}
    roTab = collections.OrderedDict()
    if(mytab == "repair_order"):
        maxN = 0
        roTab["repair_order_num"] = getValsFromDict(roHeadInfo, "RepairOrderIDs.RepairOrderNum", None, maxN)
        roTab["estimate_doc_id"] = getValsFromDict(roHeadInfo, "RepairOrderIDs.EstimateDocumentID", None, maxN)
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


    if(mytab == "ro_damage_line"):
        roTab["repair_order_num"] = getValsFromDict(roHeadInfo, "RepairOrderIDs.RepairOrderNum", None, getmaxNumOnSplt(dmgLineInfo.keys()))
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
        roTab["src_created_ts"] = getValsFromDict(docInfInfo, "CreateDateTime", None, getmaxNumOnSplt(dmgLineInfo.keys()))
        roTab["dw_created_ts"] = getValsFromDict(dmgLineInfo, timestamp)
        roTab["dw_created_by"] = getValsFromDict(dmgLineInfo, acid)
        roTab["dw_modified_ts"] = getValsFromDict(dmgLineInfo, timestamp)
        roTab["dw_modified_by"] = getValsFromDict(dmgLineInfo, acid)
    
    
    if(mytab == "ro_event"):
        roTab['repair_order_num'] = getValsFromDict(roHeadInfo, "RepairOrderIDs.RepairOrderNum", None, getmaxNumOnSplt(roEvtInfo.keys()))
        roTab['event_typ'] = getValsFromDict(roEvtInfo, "RepairOrderEvent.NNV.EventType")
        roTab['event_ts'] = getValsFromDict(roEvtInfo, "RepairOrderEvent.NNV.EventDateTime")
        roTab['event_note'] = getValsFromDict(roEvtInfo, "RepairOrderEvent.NNV.EventNotes")
        roTab['event_authored_by'] = getValsFromDict(roEvtInfo, "RepairOrderEvent.NNV.AuthoredBy.LastName")
        roTab['src_created_ts'] = getValsFromDict(docInfInfo, "CreateDateTime", None, getmaxNumOnSplt(roEvtInfo.keys()))
        roTab['dw_created_ts'] = getValsFromDict(roEvtInfo, timestamp)
        roTab['dw_created_by'] = getValsFromDict(roEvtInfo, acid)
        roTab['dw_modified_ts'] = getValsFromDict(roEvtInfo, timestamp)
        roTab['dw_modified_by'] = getValsFromDict(roEvtInfo, acid)


    if(mytab == "ro_note"):
        maxN = 1
        roTab['repair_order_num'] = getValsFromDict(roHeadInfo, "RepairOrderIDs.RepairOrderNum", None, maxN)
        roTab['src_created_ts']      = getValsFromDict(docInfInfo, "CreateDateTime", None, maxN)
        roTab['dw_created_ts']       = getValsFromDict(profInfInfo, timestamp, None, maxN)
        roTab['dw_created_by']       = getValsFromDict(profInfInfo, acid, None, maxN)
        roTab['dw_modified_ts']      = getValsFromDict(profInfInfo, timestamp, None, maxN)
        roTab['dw_modified_by']      = getValsFromDict(profInfInfo, acid, None, maxN)


    if(mytab == "ro_rate_info"):
        roTab['repair_order_num'] = getValsFromDict(roHeadInfo, "RepairOrderIDs.RepairOrderNum", None, getmaxNumOnSplt(profInfInfo.keys()))
        roTab['rate_typ']            = getValsFromDict(profInfInfo, "RateInfo.NNV.RateType")
        roTab['rate_desc']           = getValsFromDict(profInfInfo, "RateInfo.NNV.RateDesc")
        roTab['tax_typ']             = getValsFromDict(profInfInfo, "RateInfo.NNV.TaxInfo.TaxType")
        roTab['taxable_ind']         = getValsFromDict(profInfInfo, "RateInfo.NNV.TaxInfo.TaxableInd")
        roTab['tier_num']            = getValsFromDict(profInfInfo, "RateInfo.NNV.RateTierInfo.TierNum")
        roTab['rate_val']            = getValsFromDict(profInfInfo, "RateInfo.NNV.RateTierInfo.Rate")
        roTab['rate_pct']            = getValsFromDict(profInfInfo, "RateInfo.NNV.RateTierInfo.Percentage")
        #roTab['threshold_amt']       = getValsFromDict(profInfInfo, "RateInfo.NNV.TaxInfo.TaxTierInfo.M.ThresholdAmt")        
        #roTab['surcharge_amt']       = getValsFromDict(profInfInfo, "RateInfo.NNV.TaxInfo.TaxTierInfo.M.SurchargeAmt")
        #roTab['adjust_pct']          = getValsFromDict(profInfInfo, "RateInfo.NNV.AdjustmentInfo.M.AdjustmentPct")
        #roTab['adjust_typ']          = getValsFromDict(profInfInfo, "RateInfo.NNV.AdjustmentInfo.M.AdjustmentType")
        roTab['matr_calc_method_cd'] = getValsFromDict(profInfInfo, "RateInfo.NNV.MaterialCalcSettings.CalcMethodCode")
        roTab['matr_calc_max_amt']   = getValsFromDict(profInfInfo, "RateInfo.NNV.MaterialCalcSettings.CalcMaxAmt")        
        roTab['matr_calc_max_hrs']   = getValsFromDict(profInfInfo, "RateInfo.NNV.MaterialCalcSettings.CalcMaxHours")
        roTab['src_created_ts']      = getValsFromDict(docInfInfo, "CreateDateTime", None, getmaxNumOnSplt(profInfInfo.keys()))
        roTab['dw_created_ts']       = getValsFromDict(profInfInfo, timestamp)
        roTab['dw_created_by']       = getValsFromDict(profInfInfo, acid)
        roTab['dw_modified_ts']      = getValsFromDict(profInfInfo, timestamp)
        roTab['dw_modified_by']      = getValsFromDict(profInfInfo, acid)


    if(mytab == "ro_total_info"):
        maxL = getmaxNumOnSplt(repTotInfo.keys(), "LaborTotalsInfo")
        maxP = getmaxNumOnSplt(repTotInfo.keys(), "PartsTotalsInfo")
        maxO = getmaxNumOnSplt(repTotInfo.keys(), "OtherChargesTotalsInfo")
        maxS = getmaxNumOnSplt(repTotInfo.keys(), "SummaryTotalsInfo")    
        roTab["repair_order_num"]= getValsFromDict(roHeadInfo, "RepairOrderIDs.RepairOrderNum", None, (maxL + maxP + maxO + maxS)*getmaxNumOnSplt(repTotInfo.keys()) )
        roTab['tot_typ_ctgry'] = getValsFromDict(repTotInfo, 'LaborTotalsInfo', "Labor", maxL)  + getValsFromDict(repTotInfo, 'PartsTotalsInfo', "Parts", maxP)  + getValsFromDict(repTotInfo, 'OtherChargesTotalsInfo', "Other", maxO)  + getValsFromDict(repTotInfo, 'SummaryTotalsInfo', "Summary", maxS)
        roTab['tot_typ'] = getValsFromDict(repTotInfo, "LaborTotalsInfo.NNV.TotalType", None, maxL)   + getValsFromDict(repTotInfo, "PartsTotalsInfo.NNV.TotalType", None, maxP) + getValsFromDict(repTotInfo, "OtherChargesTotalsInfo.NNV.TotalType", None, maxO) + getValsFromDict(repTotInfo, "SummaryTotalsInfo.NNV.TotalType", None, maxS)
        roTab['tot_sub_typ'] = getValsFromDict(repTotInfo, "LaborTotalsInfo.NNV.TotalSubType", None, maxL) + getValsFromDict(repTotInfo, "PartsTotalsInfo.NNV.TotalSubType", None, maxP) + getValsFromDict(repTotInfo, "OtherChargesTotalsInfo.NNV.TotalSubType", None, maxO) + getValsFromDict(repTotInfo, "SummaryTotalsInfo.NNV.TotalSubType", None, maxS)
        roTab['tot_typ_desc'] = getValsFromDict(repTotInfo, "LaborTotalsInfo.NNV.TotalTypeDesc", None, maxL) + getValsFromDict(repTotInfo, "PartsTotalsInfo.NNV.TotalTypeDesc", None, maxP) + getValsFromDict(repTotInfo, "OtherChargesTotalsInfo.NNV.TotalTypeDesc", None, maxO) + getValsFromDict(repTotInfo, "SummaryTotalsInfo.NNV.TotalTypeDesc", None, maxS)
        roTab['tot_hrs']  = ConvListEle( getValsFromDict(repTotInfo, "LaborTotalsInfo.NNV.TotalHours", None, maxL) + getValsFromDict(repTotInfo, "PartsTotalsInfo.NNV.TotalHours", None, maxP) + getValsFromDict(repTotInfo, "OtherChargesTotalsInfo.NNV.TotalHours", None, maxO) + getValsFromDict(repTotInfo, "SummaryTotalsInfo.NNV.TotalHours", None, maxS) )
        roTab['tot_amt']  = ConvListEle( getValsFromDict(repTotInfo, "LaborTotalsInfo.NNV.TotalAmt", None, maxL) + getValsFromDict(repTotInfo, "PartsTotalsInfo.NNV.TotalAmt", None, maxP) + getValsFromDict(repTotInfo, "OtherChargesTotalsInfo.NNV.TotalAmt", None, maxO) + getValsFromDict(repTotInfo, "SummaryTotalsInfo.NNV.TotalAmt", None, maxS) )
        roTab['tot_cost'] = ConvListEle( getValsFromDict(repTotInfo, "LaborTotalsInfo.NNV.TotalCost", None, maxL) + getValsFromDict(repTotInfo, "PartsTotalsInfo.NNV.TotalCost", None, maxP) + getValsFromDict(repTotInfo, "OtherChargesTotalsInfo.NNV.TotalCost", None, maxO) + getValsFromDict(repTotInfo, "SummaryTotalsInfo.NNV.TotalCost", None, maxS) )
        roTab['tot_adj_typ'] = getValsFromDict(repTotInfo, "LaborTotalsInfo.NNV.TotalAdjustmentInfo.AdjustmentType", None, maxL) + getValsFromDict(repTotInfo, "PartsTotalsInfo.NNV.TotalAdjustmentInfo.AdjustmentType", None, maxP) + getValsFromDict(repTotInfo, "OtherChargesTotalsInfo.NNV.TotalAdjustmentInfo.AdjustmentType", None, maxO) + getValsFromDict(repTotInfo, "SummaryTotalsInfo.NNV.TotalAdjustmentInfo.AdjustmentType", None, maxS)
        roTab['tot_adj_amt'] = ConvListEle( getValsFromDict(repTotInfo, "LaborTotalsInfo.NNV.TotalAdjustmentInfo.TotalAdjustmentAmt", None, maxL) + getValsFromDict(repTotInfo, "PartsTotalsInfo.NNV.TotalAdjustmentInfo.TotalAdjustmentAmt", None, maxP) + getValsFromDict(repTotInfo, "OtherChargesTotalsInfo.NNV.TotalAdjustmentInfo.TotalAdjustmentAmt", None, maxO) + getValsFromDict(repTotInfo, "SummaryTotalsInfo.NNV.TotalAdjustmentInfo.TotalAdjustmentAmt", None, maxS) )
        roTab['src_created_ts'] = getValsFromDict(docInfInfo, "CreateDateTime", None,     (maxL + maxP + maxO + maxS)*getmaxNumOnSplt(repTotInfo.keys()))
        roTab['dw_created_ts']  = getValsFromDict(repTotInfo, "",               timestamp,(maxL + maxP + maxO + maxS)*getmaxNumOnSplt(repTotInfo.keys()))
        roTab['dw_created_by']  = getValsFromDict(repTotInfo, "",               acid,     (maxL + maxP + maxO + maxS)*getmaxNumOnSplt(repTotInfo.keys()))
        roTab['dw_modified_ts'] = getValsFromDict(repTotInfo, "",               timestamp,(maxL + maxP + maxO + maxS)*getmaxNumOnSplt(repTotInfo.keys()))
        roTab['dw_modified_by'] = getValsFromDict(repTotInfo, "",               acid,     (maxL + maxP + maxO + maxS)*getmaxNumOnSplt(repTotInfo.keys()))

    return roTab

def getROnum(alltabsInfo):
    roHeadTab = list(alltabsInfo.keys())[3]
    roHeadInfo = alltabsInfo.get(roHeadTab)
    return getValsFromDict(roHeadInfo, "RepairOrderIDs.RepairOrderNum", None, 0)[0]

def getROcreateTimeStamp(alltabsInfo):
    docInfTab = list(alltabsInfo.keys())[1]
    docInfInfo = alltabsInfo.get(docInfTab)
    return getValsFromDict(docInfInfo, "CreateDateTime", None, 0)[0]

def PushCSVtoRedshiftTab(trgBucket, srcFile, table_list, ro_num, ro_ts):
    conn=psycopg2.connect(dbname='dev', host='redshift-cluster-1.c1nljb0ecvas.us-east-2.redshift.amazonaws.com', port='5439', user='tbguser', password='Tbguser12')
    cur=conn.cursor()
    cur.execute("begin;")
    #for mytable in table_list:
    #    locktab = "lock "+mytable+" ;"
    #    cur.execute(locktab)
    #    print("Locked Table: ", mytable)
    
    queryS = "select repair_order_num from "+ table_list[0] +" where repair_order_num =" + "'"+str(ro_num)+"'" + " ;"        
    print(queryS)
    cur.execute(queryS)
    rows = cur.fetchall()
    print("rows: RO_num ", rows, len(rows))
    if not len(rows):
        print("Insert Values into all tables, Done, EXIT! ")
        for mytable in table_list:
            trgFile = mytable + "/" + srcFile.replace(".json", "_.json").replace(".json", mytable) + ".csv"
            executeCom = "copy "+ mytable + " from 's3://" + trgBucket + "/" + trgFile + "' iam_role 'arn:aws:iam::526892378978:role/redshiftaccesstos3' DELIMITER '|' EMPTYASNULL IGNOREHEADER 1 TIMEFORMAT 'auto';"
            cur.execute(executeCom)
            cur.execute("commit;")
        cur.close()
        conn.close()
        return
    else:
        print("This repair_order_num: "+str(ro_num)+" is in database, good for next step...")
        queryS = "select src_created_ts from "+ table_list[0] +" where repair_order_num =" + "'" + str(ro_num) + "'" +" and src_created_ts >= " +"'" + str(ro_ts) + "'" +" ;"
        print(queryS)
        cur.execute(queryS)
        rows = cur.fetchall()
        print("rows: RO_ts ", rows, len(rows))
        if len(rows):
            print("Current TS: "+ro_ts+" is less or equal to Database TS: "+str(rows))
            print("Donot do anything!! EXIT")
            cur.close()
            conn.close()
            return
        else:
            print("Current TS: "+ro_ts+" is greater than atleast one of Database TS: "+str(rows))
            print("Delete All entries for repair_order_num: "+str(ro_num))
            print("Insert current entry for current TS:     "+str(ro_ts))
            print("Exit !!! ")
            for mytable in table_list:
                delQuery = "delete from "+ mytable +" where repair_order_num = " + str(ro_num) + ";" 
                cur.execute(delQuery)
                conn.commit()

                trgFile = mytable + "/" +srcFile.replace(".json", "_.json").replace(".json", mytable) + ".csv"
                executeCom = "copy "+ mytable + " from 's3://" + trgBucket + "/" + trgFile + "' iam_role 'arn:aws:iam::526892378978:role/redshiftaccesstos3' DELIMITER '|' EMPTYASNULL IGNOREHEADER 1 TIMEFORMAT 'auto';"
                cur.execute(executeCom)
                cur.execute("commit;")
            cur.close()
            conn.close()
            return

def CreateCSVfileForTab(alltabsInfo, redshiftTab, srcBucket, srcFile, trgBucket):
    roSaveTab=createROtabs(alltabsInfo, redshiftTab)
    #pp.pprint(roSaveTab)
    csvFile = srcFile.replace(".json", "_.json").replace(".json", redshiftTab) + ".csv"    
    trgFile = redshiftTab + "/" + srcFile.replace(".json", "_.json").replace(".json", redshiftTab) + ".csv"
    writeOutCsv(roSaveTab, csvFile)
    
    fileHelper.cpFrmTmpToS3(csvFile, trgBucket, trgFile)
    tocopycsv='s3://'+trgBucket+'/'+trgFile
    print("CSV file created at:  ", tocopycsv)

def lambda_handler(event, context):
    srcBucket="flatten-json-em"
    srcFile="RO_3612514224.json"
    srcFile="20180817T170345260_330c6be4-ed12-4ae8-88b3-a3ac83815a66_RO_Create.json"
    trgBucket="pipedelimfiles-for-table"
    print("event: ", event)
    try:
        srcBucket=event['Records'][0]['s3']['bucket']['name']
        srcFile=event['Records'][0]['s3']['object']['key']
    except:
        print("Will be using default values from Lambda")

    print("fileName:     ", srcFile)
    print("sourceBucket: ", srcBucket)

    fileHelper.cpToTmpFolder(srcBucket, srcFile)
    alltabsInfo=getTabsInfo(srcFile)
    allkeys=list(alltabsInfo.keys())
    #print("allkeys: ", allkeys)

    table_list = ["repair_order", "ro_damage_line", "ro_event", "ro_total_info"]
    for mytable in table_list:
        CreateCSVfileForTab(alltabsInfo, mytable, srcBucket, srcFile, trgBucket)

    ro_num = getROnum(alltabsInfo)
    ro_ts = getROcreateTimeStamp(alltabsInfo)
    print("ro_num = " +  str( ro_num) )
    print("ro_ts  = " +  str( ro_ts ) )
    
    while True:
        try:
            PushCSVtoRedshiftTab(trgBucket, srcFile, table_list, ro_num, ro_ts)        
            break
        except Exception as e:
            print("PushCSVtoRedshiftTab Failed: " + str(e)+" Sleeping for 10 sec before retrying")
            time.sleep(10)

    return {
        'statusCode': 200,
        'body': json.dumps(srcFile)
    }