import wget
import os
import requests
import urllib2
import json
import time
import datetime
import sys
import bz2
import csv
import commands
from elasticsearch import Elasticsearch
from topTalker import getTalkers
from getUrls import extrcatUrl
from parseUpdate import parse
from write_to_files import write_to_csv,write_to_json
from ast import literal_eval
from pympler import tracker

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
def getFlowEntries(start,end,ES_Instance):
	es = Elasticsearch([ES_Instance])
	query = {"size":50000,"filter":{"bool":{"must":[{"range":{"start":{"gte":start,"lte":end,"format":"epoch_millis"}}}],"must_not":[]}}}
	return es.search(body=query,scroll='1m')["hits"]

if __name__ == '__main__':
	try:
		#tr = tracker.SummaryTracker()
		print "Process started at - ",datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S.%f')
		START_TIME =  datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(2), '%Y-%m-%d-%H-%M-%S')
                END_TIME =  datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(1), '%Y-%m-%d-%H-%M-%S')
	        #START_TIME = sys.argv[1]
        	#END_TIME = sys.argv[2]
		print "start - ",START_TIME
		print "end - ",END_TIME
		config_file = open("config.json","r")
		print "Can't reach here"
		config_obj = literal_eval(config_file.read())
	        nflow = getFlowEntries(getUnixTime(START_TIME),getUnixTime(END_TIME),config_obj["ES_Instance"])
		print "score ",nflow["total"]
		#print nflow
		topTalker_sources = getTalkers(nflow)
		print "ip files - ",topTalker_sources

		url_list= extrcatUrl([START_TIME,END_TIME])
		print url_list
		print len(url_list)
		pwd = os.getcwd()
		print "pwd is - ",pwd
		flapsDict={}
		for eachFile in url_list:
			file_name = wget.download(eachFile)
			decompressed_file = open(file_name[:-4],"a+")
			print "file name ---  ",file_name
			preF = parse(bz2.BZ2File(pwd+"/"+file_name,"rb"),topTalker_sources)
			print "Pref - "
			print preF
			for key,value in preF.iteritems():
				if key in flapsDict:
                                	flapsDict[key] = flapsDict[key] + value
                     		else:
					print "key not in flapsDict ",key
                        		flapsDict[key] = value
			#tr.print_diff()
		
		print "flapdict ---- "
		print flapsDict
		write_to_csv(flapsDict, topTalker_sources, START_TIME)
		write_to_json(flapsDict, topTalker_sources, START_TIME)

		#Removing update files - 
		for fname in os.listdir(pwd):
		    if fname.startswith("updates"):
        		os.remove(os.path.join(pwd, fname))

		open("status.json",'w').close() # to clear contents of the file
                status_file = open("status.json","w")
                status_file.seek(0)
                status_obj = {"timestamp":datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S.%f'),"error_text":"","error":0}
                status_file.write(json.dumps(status_obj))
                status_file.close()
	except Exception as e:
		open("status.json",'w').close() # to clear contents of the file
        	status_file = open("status.json","w")
        	status_file.seek(0)
        	status_obj = {"timestamp":datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S.%f'),"error_text":str(e),"error":1}
        	status_file.write(json.dumps(status_obj))
        	status_file.close()
