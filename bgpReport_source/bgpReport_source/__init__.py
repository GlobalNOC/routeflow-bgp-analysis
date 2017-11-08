import wget
import os
import json
import time
import datetime
import sys
import bz2
from elasticsearch import Elasticsearch
from getUrls import extrcatUrl
from parseUpdate import parse
from write_to_files import write_to_csv,write_to_json
from ast import literal_eval

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
	#Get top 10 IP group by total data bits sent - 
	query = {"size":0,"filter":{"bool":{"must":[{"range":{"start":{"gte":start,"lte":end,"format":"epoch_millis"}}}],"must_not":[]}},\
	"aggs":{"group_by_src_ip":{"terms":{"field":"meta.src_ip","size":11}, "aggs":{"total_bits":{"sum":{"field":"values.num_bits"}}}}} }
	return es.search(body=query,scroll='1m')

def write_status(error=0, error_text=""):
		open("status.json",'w').close() # to clear contents of the file
                status_file = open("status.json","w")
                status_file.seek(0)
                status_obj = {"timestamp":datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S.%f'),"error_text":error_text,"error":error}
                status_file.write(json.dumps(status_obj))
                status_file.close()

def extract_top_talkers(nflow):
	topTalkers=[]
	for each in nflow:
		if each["key"] != "":
			topTalkers.append((each["key"],each["total_bits"]["value"]))
	return topTalkers

def main(START_TIME =  datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(2), '%Y-%m-%d-%H-%M-%S'), END_TIME =  datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(1), '%Y-%m-%d-%H-%M-%S')):
	try:
		print "Process started at - ",datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S.%f')
		print "start - ",START_TIME
		print "end - ",END_TIME
		config_file = open("config.json","r")
		config_obj = literal_eval(config_file.read())
	        nflow = getFlowEntries(getUnixTime(START_TIME),getUnixTime(END_TIME),config_obj["ES_Instance"])["aggregations"]["group_by_src_ip"]["buckets"]
		topTalker_sources = extract_top_talkers(nflow)
		print topTalker_sources
		url_list= extrcatUrl([START_TIME,END_TIME])
		print url_list
		pwd = os.getcwd()
		flapsDict={}
		for eachFile in url_list:
			file_name = wget.download(eachFile)
			decompressed_file = open(file_name[:-4],"a+")
			print "file name ---  ",file_name
			preF = parse(bz2.BZ2File(pwd+"/"+file_name,"rb"),topTalker_sources)
			for key,value in preF.iteritems():
				if key in flapsDict:
                                	flapsDict[key] = flapsDict[key] + value
                     		else:
					print "key not in flapsDict ",key
                        		flapsDict[key] = value
		
		write_to_csv(flapsDict, topTalker_sources, START_TIME)
		write_to_json(flapsDict, topTalker_sources, START_TIME)
		
		#Removing update files - 
		for fname in os.listdir(pwd):
		    if fname.startswith("updates"):
        		os.remove(os.path.join(pwd, fname))
		
		write_status()
	except Exception as e:
		print "Exception -",e
		write_status(1,str(e))

