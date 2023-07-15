import json
import boto3
import os
import csv
import collections
import xml.etree.cElementTree as ElementTree
import xmltodict
import fileHelper
from itertools import chain
from functools import reduce

s3c = boto3.client('s3')


# Find the list of the Tabs
# Right Now Its optimized for RO [ #of childs ]
def getListOfTabs(srcFile="Update.xml", childN=2):
    listof_tabs = []
    tree = ElementTree.parse(srcFile)
    root = tree.getroot()
    for child in reduce(lambda x, _: chain.from_iterable(x), range(childN), root):
        listof_tabs.append(str(child.tag).split('}')[1])
    # print(listof_tabs)
    return listof_tabs


# This will be used to flat Out the Dictionary
def flatten_dict(dd, separator='.', prefix=''):
    return {prefix + separator + k if prefix else k: v
            for kk, vv in dd.items()
            for k, v in flatten_dict(vv, separator, kk).items()
            } if isinstance(dd, dict) else {prefix: dd}


# Flat the list of Dict
def flatten_listDict(d, sep="."):
    obj = collections.OrderedDict()

    def recurse(t, parent_key=""):
        if isinstance(t, list):
            for i in range(len(t)):
                recurse(t[i], parent_key + sep + str(i) if parent_key else str(i))
        elif isinstance(t, dict):
            for k, v in t.items():
                recurse(v, parent_key + sep + k if parent_key else k)
        else:
            obj[parent_key] = t

    recurse(d)
    return obj


# From Each tab, extract Info
def getTabInfoDict(myxml="parsed_xml", mytab="DocumentInfo"):
    outdict = {}
    for k, v in myxml.items():
        if mytab in k:
            ksplt: str = str(k.split(mytab)[1][1:])
            # mykey = mytab
            # if(len(ksplt)):
            #    mykey = mytab + "." + ksplt
            # outdict[mykey] = v
            outdict[ksplt] = v
    return outdict


def parseXmlToJson(srcFile, trgFile):
    # Open xml and flat it out
    srcFile = "/tmp/" + srcFile
    with open(srcFile, 'rb') as f:
        xml_content = xmltodict.parse(f)
    flattened_xml = flatten_dict(xml_content)
    listof_tabs = getListOfTabs(srcFile)
    alltabsInfo = {}
    for xtab in listof_tabs:
        alltabsInfo[xtab] = flatten_listDict(getTabInfoDict(flattened_xml, xtab))
    ajf = str("/tmp/") + trgFile
    aj = open(ajf, "w")
    aj.write(json.dumps(alltabsInfo, indent=4))
    aj.close()


def lambda_handler(event, context):
    srcBucket = "xml-files-em"
    trgBucket = "csv-files-em"
    srcFile = "Create.xml"
    print("event: ", event)

    try:
        srcBucket = event['Records'][0]['s3']['bucket']['name']
        srcFile = event['Records'][0]['s3']['object']['key']

    except:
        print("will be using local fileName:     ", srcFile)
        print("will be using local sourceBucket: ", srcBucket)
        print("will be using local targetBucket: ", trgBucket)

    trgFile = srcFile + ".json"

    fileHelper.cpToTmpFolder(srcBucket, srcFile)
    parseXmlToJson(srcFile, trgFile)
    fileHelper.cpFrmTmpToS3(trgBucket, trgFile)

    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps(srcFile)
    }