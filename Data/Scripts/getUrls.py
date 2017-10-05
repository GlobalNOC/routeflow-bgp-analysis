'''
Input: Datetime range
Output: File named 'urlFile' which contains list of url's that we need to download to compute the bgp stability information in the given time range. 
Function: In Routeview archives, the bgp updates are stored every 15 minutes. Given a range of year-mon-day-h-m-s, this program finds all the url's of relevant files that are generated in the time range.
'''

import sys
import datetime
import requests
import os
import shutil


def write_urls_tofile():
	global url_list
	ft = open("urlFile","w")
	for url in url_list:
		ft.write(url+"\n")
	ft.close()

def getRange(start_hour, start_min, end_hour, end_min):
	#Time is a list that contains start and end timeframe [yy-mm-dd-hh-mm-ss-mm, yy-mm-dd-mm-ss-mm] 
	finalList = []
	timerange = []
	t = [start_hour+"-"+start_min , end_hour+"-"+end_min]
	for val in t:
		m = int(val.split("-")[1])
		h = int(val.split("-")[0])
		if m==0:
			h = h-1
			m = 45
		else:
			m = m - (m%15) 
    
		h = str(h)
		m = str(m)
		if(len(h)!=2):
			h="0"+h
		if(len(m)!=2):
			m="0"+m
		timerange.append([h,m])

  #Timerange has start and end [hhmm] previous 15th minute. Ex 12h7m -> 1200, 23:48 -> 2345''
	start_hour=timerange[0][0]
	start_min=timerange[0][1] 
	end_hour=timerange[1][0] 
	end_min=timerange[1][1]
	finalList.append(start_hour+start_min)

	while( not ((start_hour == end_hour) and (start_min == end_min))):
		if int(start_min) > 60:
			break
		if start_min == "45":
			start_min = "00"
			start_hour = str(int(start_hour)+1)
			if(len(start_hour) <2):
				start_hour = "0" + start_hour
		else:
			start_min = str(int(start_min)+15)
		finalList.append(start_hour+start_min)
		#FinalList will have the range of 15 minute intervals that we use to build url
		#for 00:13 to 2:40 --> ["0000', '0015', '0030', '0045', '0100', '0115', '0130', '0145', '0200', '0215', '0230']
	return finalList

def extrcatUrl(time):
	print "In get uRl scirpt"
	url_list = []

	syear = time[0].split("-")[0]
	smonth = time[0].split("-")[1]
	sday = time[0].split("-")[2]
	shour = time[0].split("-")[3]
	smin = time[0].split("-")[4]

	ehour = time[1].split("-")[0]
	emonth = time[1].split("-")[1]
	eday = time[1].split("-")[2]
	ehour = time[1].split("-")[3]
	emin = time[1].split("-")[4]
	for i in range(int(smonth),int(emonth)+1):
		if smonth != emonth:
			#get all days of smon, and increment smon and continue
			#TODO: Code incomplete
			print " different month"
		else:
			#url="http://archive.routeviews.org/bgpdata/"+year+"."+month+"/UPDATES/updates."+year+month+day+"."+value+".bz2"
			for day in range(int(sday),int(eday)+1):
				if day==int(eday):
					#get hours from start to end
					for time_segment in getRange(shour,smin,ehour,emin):
						url_list.append("http://archive.routeviews.org/bgpdata/"+str(syear)+"."+str(smonth)+"/UPDATES/updates."+str(syear)+str(smonth)+str(sday)+"."+time_segment+".bz2")
				else:
					for time_segment in getRange(shour,smin,"24","00"):
						url_list.append("http://archive.routeviews.org/bgpdata/"+str(syear)+"."+str(smonth)+"/UPDATES/updates."+str(syear)+str(smonth)+str(eday)+"."+time_segment+".bz2")
						shour ="00"
						smin = "01"
           
	return url_list
