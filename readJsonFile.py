import json
import pprint
pp = pprint.PrettyPrinter(depth=4)

f = open('alltabsInOne.json')
alltabsInfo = json.load(f)
f.close()

allkeys = list( alltabsInfo.keys() )
print("allkeys: ", allkeys)
docInfo = alltabsInfo.get(allkeys[1])
print(docInfo)
#pp.pprint({k.replace(allkeys[1], ''): v for k, v in alltabsInfo.get(allkeys[1]).items()})
#pp.pprint({k.replace(allkeys[2], ''): v for k, v in alltabsInfo.get(allkeys[2]).items()})