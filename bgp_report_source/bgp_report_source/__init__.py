"""
This module extracts bgp data from netsage, calculates number of events from archived files
and writes the computed data to Analysis.csv and Analysis.json files
"""
import os
import json
import time
import datetime
import sys
import bz2
from ast import literal_eval
import wget
from elasticsearch import Elasticsearch
from get_urls import extrcat_url
from parse_update import parse
from write_to_files import write_to_csv, write_to_json

# Converts from readable form year-month-day-hour-min-sec to unix time
def get_unix_time(date):
	split_date = date.split('-')
	mydat = datetime.datetime(int(split_date[0]), int(split_date[1]), int(split_date[2]),\
		int(split_date[3]), int(split_date[4]), int(split_date[5]))
	epoch = datetime.datetime.utcfromtimestamp(0)
	time_difference = mydat - epoch
	unixtime = (time_difference.microseconds + (time_difference.seconds + time_difference.days * 86400) * 10 ** 6)/ 10**6
	unix_str = str(unixtime)
	if len(unix_str) < 13:
		for i in range(13 - len(unix_str)):
			unix_str = unix_str + "0"
	return int(unix_str)

# Get flow entries in the range[start,end] using ES API
def get_flow_entries(start, end, es_instance):
	es_object = Elasticsearch([es_instance])
	#Get top 10 IP group by total data bits sent -
	query = {"query":{"range": {"start": {"gte":start, "lte":end, "format":"epoch_millis"}}},\
		"aggs":{"group_by_src_ip":{"terms":{"field":"meta.src_ip"}, "aggs":{"total_bits":\
		{"sum":{"field":"values.num_bits"}}}}}}
	return es_object.search(body=query, scroll='1m')["aggregations"]["group_by_src_ip"]["buckets"]

def write_status(file_path, error=0, error_text=""):
	open(file_path+"status.json", 'w').close() # to clear contents of the file
	status_file = open("status.json", "w")
	status_file.seek(0)
	status_obj = {"timestamp":datetime.datetime.fromtimestamp(time.time())\
		     .strftime('%Y-%m-%d %H:%M:%S.%f'), "error_text":error_text, "error":error}
	status_file.write(json.dumps(status_obj))
	status_file.close()

def extract_top_talkers(nflow):
	top_talkers = []
	for each in nflow:
		if each["key"] != "":
			top_talkers.append((each["key"], each["total_bits"]["value"]))
	return top_talkers

def main(config_file_path="",\
	START_TIME=datetime.datetime.strftime(datetime.datetime.now()\
	- datetime.timedelta(2), '%Y-%m-%d-%H-%M-%S'),\
	END_TIME=datetime.datetime.strftime(datetime.datetime.now()\
	- datetime.timedelta(1), '%Y-%m-%d-%H-%M-%S')):
	try:
		print "Process started at - ", datetime.datetime.fromtimestamp(time.time())\
		.strftime('%Y-%m-%d %H:%M:%S.%f')
		print "start - ", START_TIME
		print "end - ", END_TIME
		if config_file_path == "":
			config_obj = literal_eval(open("config.json", "r").read())
		else:
			config_obj = literal_eval(open(config_file_path+"/config.json", "r").read())
		nflow = get_flow_entries(get_unix_time(START_TIME), get_unix_time(END_TIME), config_obj["ES_Instance"])
		top_talker_sources = extract_top_talkers(nflow)
		print top_talker_sources
		url_list = extrcat_url([START_TIME, END_TIME])
		print url_list
		pwd = os.getcwd()
		flaps_dict = {}
		for each_file in url_list:
			file_name = wget.download(each_file)
			print "file name ---  ", file_name
			parsed_files = parse(bz2.BZ2File(pwd+"/"+file_name, "rb"), top_talker_sources)
			for key, value in parsed_files.iteritems():
				if key in flaps_dict:
					flaps_dict[key] = flaps_dict[key] + value
				else:
					print "key not in flaps_dict ", key
					flaps_dict[key] = value

		write_to_csv(flaps_dict, top_talker_sources, config_obj["data_file_path"], START_TIME)
		write_to_json(flaps_dict, top_talker_sources, config_obj["data_file_path"], START_TIME)

		#Removing update files -
		for fname in os.listdir(pwd):
			if fname.startswith("updates"):
				os.remove(os.path.join(pwd, fname))

		write_status(config_obj["status_file_path"])
	except Exception as e:
		print "Exception -", e
		write_status(config_obj["status_file_path"], 1, str(e))
