import json
import xmltodict
import xml.etree.cElementTree as ElementTree
import collections
from itertools import chain
from functools import reduce


#Find dict for a tab only
def getValueForTab(forTab="EventTopic", xmlfilename="Update.xml"):
    tabdict = {}
    myroot = ElementTree.parse(xmlfilename).getroot()
    for child in reduce(lambda x, _: chain.from_iterable(x), range(0), myroot):
        if(str(child.tag).split('}')[1]==forTab):
            return  child.text

#Find the list of the Tabs
#Right Now Its optimized for RO [ #of childs ]
def getListOfTabs(xmlfilename="Update.xml", childN=2):
    listof_tabs = []
    tree = ElementTree.parse(xmlfilename)
    root = tree.getroot()
    for child in reduce(lambda x, _: chain.from_iterable(x), range(childN), root):
            listof_tabs.append(str(child.tag).split('}')[1])
        #print(listof_tabs)
    return listof_tabs


#This will be used to flat Out the Dictionary
def flatten_dict(dd, separator='.', prefix=''):
    return { prefix + separator + k if prefix else k : v
             for kk, vv in dd.items()
             for k, v in flatten_dict(vv, separator, kk).items()
             } if isinstance(dd, dict) else { prefix : dd }

#Flat the list of Dict
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

#From Each tab, extract Info
def getTabInfoDict(myxml="parsed_xml", mytab="DocumentInfo"):
    outdict = {}
    for k, v in myxml.items():
        if mytab in k:
            ksplt: str = str(k.split(mytab)[1][1:])
            mykey = mytab
            if(len(ksplt)):
                mykey = mytab + "." + ksplt
            #outdict[mykey] = v
            outdict[ksplt] = v
    return outdict


def parseXmlToJson(xmlfilename):
    #Open xml and flat it out
    with open(xmlfilename, 'rb') as f:
        xml_content = xmltodict.parse(f)
    flattened_xml = flatten_dict(xml_content)
    listof_tabs = getListOfTabs(xmlfilename)
    print("listof_tabs: ", listof_tabs)

    alltabsInfo = {}
    for xtab in listof_tabs:
        alltabsInfo[xtab] = flatten_listDict( getTabInfoDict(flattened_xml, xtab) )
        
    ajf = str("alltabsInOne") + ".json"
    aj = open(ajf, "w")
    aj.write(json.dumps(alltabsInfo, indent=4))
    aj.close()

xmlfilename = "C:\\Users\\Ekta.Mishra\\PycharmProjects\\XMLfiles\\xml-labor-assignments\\9060754C-C363-4F22-A127-FC46AE0FD50F_LaborAssignment_Create.xml"
#xmlfilename = "C:\\Users\\Ekta.Mishra\\PycharmProjects\\XMLfiles\\xml-labor-assignments\\9060754C-C363-4F22-A127-FC46AE0FD50F_LaborAssignment_Update.xml"

parseXmlToJson(xmlfilename)
