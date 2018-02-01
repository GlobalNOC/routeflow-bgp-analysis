'''
Input: Datetime range
Output: File named 'urlFile' which contains list of url's that we need to download
	to compute the bgp stability information in the given time range.
Function: In Routeview archives, the bgp updates are stored every 15 minutes.
	Given a range of year-mon-day-h-m-s, this program finds all the url's of
	relevant files that are generated in the time range.
'''

def get_range(start_hour, start_min, end_hour, end_min):
	""" Calculate time intervals between start and end time"""
	final_list = []
	timerange = []
	date_time = [start_hour+"-"+start_min, end_hour+"-"+end_min]
	for val in date_time:
		minute = int(val.split("-")[1])
		hour = int(val.split("-")[0])
		if minute == 0:
			hour = hour-1
			minute = 45
		else:
			minute = minute - (minute%15)
		hour = str(hour)
		minute = str(minute)
		if len(hour) != 2:
			hour = "0"+hour
		if len(minute) != 2:
			minute = "0"+minute
		timerange.append([hour, minute])
	start_hour = timerange[0][0]
	start_min = timerange[0][1]
	end_hour = timerange[1][0]
	end_min = timerange[1][1]
	final_list.append(start_hour+start_min)

	while not ((start_hour == end_hour) and (start_min == end_min)):
		if int(start_min) > 60:
			break
		if start_min == "45":
			start_min = "00"
			start_hour = str(int(start_hour)+1)
			if len(start_hour) < 2:
				start_hour = "0" + start_hour
		else:
			start_min = str(int(start_min)+15)
		final_list.append(start_hour+start_min)
	return final_list

def extrcat_url(time):
	"""Extracts time range intervals for given time"""
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
	lastday = {1:31, 2:28, 3:31, 4:30, 5:31, 6:30, 7:31, 8:31, 9:30, 10:31, 11:30, 12:31}
	if smonth != emonth:
		print " different month"
		for month in range(int(smonth), int(emonth)+1):
			if month == int(emonth):
				for day in range(1, int(eday)+1):
					if day == int(eday):
	                                       #get hours from start to end
						for time_segment in get_range(shour, smin, ehour, emin):
							url_list.append("http://archive.routeviews.org/bgpdata/"\
							+str(syear)+"."+str(emonth)+"/UPDATES/updates."\
							+str(syear)+str(emonth)+str(eday)+"."+time_segment+".bz2")
					else:
						for time_segment in get_range(shour, smin, "24", "00"):
							url_list.append("http://archive.routeviews.org/bgpdata/"\
							+str(syear)+"."+str(smonth)+"/UPDATES/updates."\
							+str(syear)+str(smonth)+str(sday)+"."+time_segment+".bz2")
							shour = "00"
							smin = "01"
			else:
				for day in range(int(sday), lastday[int(smonth)]+1):
					if day == lastday[int(smonth)]:
						for time_segment in get_range(shour, smin, "24", "00"):
							url_list.append("http://archive.routeviews.org/bgpdata/"\
							+str(syear)+"."+str(smonth)+"/UPDATES/updates."\
							+str(syear)+str(smonth)+str(day)+"."+time_segment+".bz2")
							shour = "00"
							smin = "01"
					else:
						for time_segment in get_range(shour, smin, "24", "00"):
							url_list.append("http://archive.routeviews.org/bgpdata/"\
							+str(syear)+"."+str(smonth)+"/UPDATES/updates."\
							+str(syear)+str(smonth)+str(sday)+"."+time_segment+".bz2")
							shour = "00"
							smin = "01"
	else:
		for day in range(int(sday), int(eday)+1):
			if day == int(eday):
				for time_segment in get_range(shour, smin, ehour, emin):
					url_list.append("http://archive.routeviews.org/bgpdata/"\
					+str(syear)+"."+str(smonth)+"/UPDATES/updates."\
					+str(syear)+str(smonth)+str(eday)+"."+time_segment+".bz2")
			else:
				for time_segment in get_range(shour, smin, "24", "00"):
					url_list.append("http://archive.routeviews.org/bgpdata/"\
					+str(syear)+"."+str(smonth)+"/UPDATES/updates."\
					+str(syear)+str(smonth)+str(sday)+"."+time_segment+".bz2")
					shour = "00"
					smin = "01"
	return url_list
