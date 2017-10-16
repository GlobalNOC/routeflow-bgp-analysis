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
	print "ip files - ",topTalker_sources
	url_list= extrcatUrl([START_TIME,END_TIME])
	print url_list
	print len(url_list)
	pwd = os.getcwd()
	print "pwd is - ",pwd
	Finalstability = []
	for eachFile in url_list:
		file_name = wget.download(eachFile)
		decompressed_file = open(file_name[:-4],"a+")
		print "file name is ---  ",file_name
		'''
		decompressed_file.write(bz2.BZ2File(pwd+"/"+file_name,"rb").read())
		Finalstability.append(parse(decompressed_file,topTalker_sources))
		'''
		Finalstability.append(parse(bz2.BZ2File(pwd+"/"+file_name,"rb"),topTalker_sources))
	print "Finalstability in main is - "
	print Finalstability
	
	#Writing the data to json and csv files - 
	
	flapsDict = {}
	
	for val in Finalstability:
        	for key,value in val.iteritems():
                	if key in flapsDict.keys():
                        	flapsDict[key] = flapsDict[key] + value
                	else:
                        	flapsDict[key] = value
	print "flapdict ---- "
	print flapsDict
	csvFile = open("Analysis.csv","a")
	file_to_write = csv.writer(csvFile, delimiter=',')
	Date = START_TIME[0:10]
	print "Date --- ",Date
	for line2 in topTalker_sources:
		listToWrite = []
        	listToWrite.append(Date)
                listToWrite.append(line2[0])
		listToWrite.append(line2[1])
	        ip = listToWrite[1]
        	ip = ip[:ip.find("x")]+"0/24"
	        listToWrite.append(flapsDict[ip])
        	cmd = "whois -h whois.radb.net "+ip+" | grep descr:"
		print "ip is -- ",ip
        	try: #When no records were found for particular IP, ignore them
                	descr = commands.getoutput(cmd).split("\n")[0].split(":")[1].strip(" ")
                	asn = commands.getoutput("whois -h whois.radb.net "+ip+" | grep origin:").split("\n")[0].split(":")[1].strip(" ")
                	asn_number = asn[asn.find("AS")+2:]
                	print asn_number
                	listToWrite.append(descr+"  |ASN - "+asn_number)
        	except:
                	listToWrite.append("NOT FOUND IN RADb")
        	print listToWrite
        	file_to_write.writerow(listToWrite)
	file_to_write.writerow([])
	csvFile.close()


	open("Analysis.json",'w').close() # to clear contents of the file  
	file_to_write = open("Analysis.json","w")
	list_file = []
	for line2 in topTalker_sources:
        	listToWrite = {"Date":"","Prefix":"","DataSentInbits":"","Events":"","Organization":""}
        	listToWrite["Date"] = Date
        	listToWrite["Prefix"] = line2[0]
        	listToWrite["DataSentInbits"] = int(line2[1])
        	ip = line2[0]
        	ip = ip[:ip.find("x")]+"0/24"
        	listToWrite["Events"] = flapsDict[ip]
        	cmd = "whois -h whois.radb.net "+ip+" | grep descr:"
        	try:
                	descr = commands.getoutput(cmd).split("\n")[0].split(":")[1].strip(" ")
                	asn = commands.getoutput("whois -h whois.radb.net "+ip+" | grep origin:").split("\n")[0].split(":")[1].strip(" ")
                	asn_number = asn[asn.find("AS")+2:]
                	listToWrite["Organization"] = descr+"  |ASN - "+asn_number
        	except:
                	listToWrite["Organization"] = "NOT FOUND IN RADb"
        	print listToWrite
        	list_file.append(listToWrite)
	file_to_write.seek(0)
	file_to_write.write(json.dumps(list_file))
	file_to_write.close()

	#Removing update files - 
	for fname in os.listdir(pwd):
	    if fname.startswith("updates"):
        	os.remove(os.path.join(pwd, fname))
