import requests
import json
import time
import datetime
import sys
from elasticsearch import Elasticsearch
from topTalker import getTalkers
from getUrls import extrcatUrl

global topTalker_sources
global nflow

# Converts from readable form year-month-day-hour-min-sec to unix time 
def getUnixTime(dt):
        splitDate = dt.split('-')
        mydat = datetime.datetime(int(splitDate[0]),int(splitDate[1]),int(splitDate[2]),int(splitDate[3]),int(splitDate[4]),int(splitDate[5]))
        epoch = datetime.datetime.utcfromtimestamp(0)
        td = mydat - epoch
        unixtime = (td.microseconds + (td.seconds + td.days * 86400) * 10 ** 6 )/ 10**6
        unixStr = str(unixtime)
        if len(unixStr) <13:
                for i in range (13 - len(unixStr)):
                        unixStr = unixStr + "0"
        return int(unixStr)

# Get flow entries in the range[start,end] using ES API
def getFlowEntries(start,end):
	es = Elasticsearch(['http://140.182.49.116:9200/'])
	query = {"size":1,"filter":{"bool":{"must":[{"range":{"start":{"gte":start,"lte":end,"format":"epoch_millis"}}}],"must_not":[]}}}
	size = es.search(body=query)["hits"]["total"]
	print "size in extract script is ",size
	query = {"size":size,"filter":{"bool":{"must":[{"range":{"start":{"gte":start,"lte":end,"format":"epoch_millis"}}}],"must_not":[]}}}
        return es.search(body=query,scroll='1m')["hits"]


if __name__ == '__main__':
        START_TIME = sys.argv[1]
        END_TIME = sys.argv[2]
        nflow = getFlowEntries(getUnixTime(START_TIME),getUnixTime(END_TIME))
        print "nflow data is "
	#print nflow
	print "score ",nflow["total"]
	topTalker_sources = getTalkers(nflow)
	url_list= extrcatUrl([START_TIME,END_TIME])
	print url_list
	print len(url_list)

