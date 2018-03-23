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
import copy
from ast import literal_eval
import wget
from elasticsearch import Elasticsearch
from get_urls import extrcat_url
from parse_update import parse
from write_to_files import write_to_csv, write_to_json, write_to_db, write_to_json_events,write_to_db_drill_down
from netsage_flow import get_sensor_flow_entries, get_events_flow_entries


def get_unix_time(date):
	""" Converts date time from format YYYY-MM-DD-HH-MM-SS to Unix time
	Args:
		date (str): datetime format - YYYY-MM-DD-HH-MM-SS
	Returns:
		int: Correspnding Unix timestamep
	Example:
		get_unix_time("2017-12-09-20-08-34") returns - 1512850114000
	""" 
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


def check_ip_format(ip_string):
	""" Checks if the string is in correct IPV4 address format
	Args:
		ip_string (str): IPv4 address string in the form - "XXX.XXX.XXX.x"
	Returns:
		bool : True if the format is correct, False otherwise.
	Example:
		check_ip_format("132.25.456.x") returns True
		check_ip_format("123:9983.22.1.x") return False
	"""
	if len(ip_string) <= 1:
		return False
	ip = ip_string.split(".")
	if len(ip)<4:
		return False
	try:
		if int(ip[0]) and int(ip[1]) and int(ip[2]):
			return True
	except ValueError:
		return False


def write_status(file_path, error=0, error_text=""):
	""" Writes error message to status.json at loaction given by filepath 
	Args:
		file_path (str) : location on disk to create/write to status.json file 
		error (int) : Integer number representing error number. default = 0
		error_text (str) : Error text to be writtent to status.json file. default = ""
	"""
	if error > 0:
		print "Error occurred, kindly check status file at path - ",file_path
	open(file_path+"status.json", 'w').close() # to clear the contents of file
	status_file = open(file_path+"status.json", "w")
	status_file.seek(0)
	status_obj = {"timestamp":datetime.datetime.fromtimestamp(time.time())\
		     .strftime('%Y-%m-%d %H:%M:%S.%f'), "error_text":error_text, "error":error}
	status_file.write(json.dumps(status_obj))
	status_file.close()


def extract_sensor_top_talkers(nflow):
	"""Returns top talkers for each sensor id
	Args:
		nflow (list): list of dictionaries, where key represents sensor_id and values are dictionaries of
			      data sent between source and destination IPs.
	Return:
 		list of tuples, where each tuple reprsents - (sensor_id, source_ip, data_sent_in_bits)
	"""
	top_talkers = []
	for each in nflow:
		sensor_id = each["key"]
		for each_src in each["group_by_src_ip"]["buckets"]:
			if check_ip_format(each_src["key"]):
				top_talkers.append((sensor_id, each_src["key"], each_src["total_bits"]["value"]))
	return top_talkers


def extract_events_top_talkers(nflow):
	top_talkers = []
	for each in nflow:
		if check_ip_format(each["key"]):
			top_talkers.append((each["key"], each["total_bits"]["value"]))
	return top_talkers


def main(config_file_path,\
	START_TIME=datetime.datetime.strftime(datetime.datetime.now()\
	- datetime.timedelta(2), '%Y-%m-%d-%H-%M-%S'),\
	END_TIME=datetime.datetime.strftime(datetime.datetime.now()\
	- datetime.timedelta(1), '%Y-%m-%d-%H-%M-%S')):
	try:
		print "Process started at - ", datetime.datetime.fromtimestamp(time.time())\
		.strftime('%Y-%m-%d %H:%M:%S.%f')
		print "start - ", START_TIME
		print "end - ", END_TIME
		config_obj = literal_eval(open(config_file_path+"config.json", "r").read())

		sensor_nflow = get_sensor_flow_entries(get_unix_time(START_TIME), get_unix_time(END_TIME), config_obj["netsage_instance"])
		sensor_top_talkers = extract_sensor_top_talkers(sensor_nflow)
		print "Top sensor talkers - "
		print sensor_top_talkers
		print sensor_nflow	
		
		#events_bgp_grafana data collection - 
		events_nflow = get_events_flow_entries(get_unix_time(START_TIME), get_unix_time(END_TIME), config_obj["netsage_instance"])
                events_top_talkers = extract_events_top_talkers(events_nflow)
		
		#Extracting routeflow events - 
		url_list = extrcat_url([START_TIME, END_TIME])
		pwd = os.getcwd()
		sensor_flaps_dict = {}
		sensor_flaps_dict_time = {}
		events_flaps_dict = {}
		events_flaps_dict_time = {}
		for each_file in url_list:
			file_name = wget.download(each_file)
			print "file name ---  ", file_name
			sensor_parsed_files, events_parsed_files = parse(bz2.BZ2File(pwd+"/"+file_name, "rb"), sensor_top_talkers, events_top_talkers)
			#Sensor route information - 
			for key, value in sensor_parsed_files.iteritems():
				if key in sensor_flaps_dict:
					sensor_flaps_dict[key]+= value
				else:
					sensor_flaps_dict[key] = value
			#Events route information - 
			for key, value in events_parsed_files.iteritems():
                                if key in events_flaps_dict:
                                        events_flaps_dict[key]+= value
                                else:
                                        events_flaps_dict[key] = value
		print "#########"*20	
		print "sensor flaps dict - "
		print sensor_flaps_dict
		print "events flaps dict - "
		print events_flaps_dict
		print "#########"*20
		write_to_csv(sensor_flaps_dict, sensor_top_talkers, config_obj["data_file_path"], config_obj["sensor-name-map"], START_TIME)
		json_dump = write_to_json(sensor_flaps_dict, sensor_top_talkers, config_obj["data_file_path"], config_obj["sensor-name-map"], START_TIME)
		#making a deep copy of json_dump object - 
		json_dump1 = copy.deepcopy(json_dump)	
		write_to_db(START_TIME, json_dump, config_obj["elasticsearch_instance"], config_obj["sensor_es_index"], config_obj["es_document"])
		write_to_db_drill_down(START_TIME, json_dump1, config_obj["elasticsearch_instance"], config_obj["sensor_es_index"]+"_drill_down", config_obj["es_document"])
		#Time stamp - 
		print "sensor db populated "

		json_dump = write_to_json_events(events_flaps_dict, events_top_talkers, config_obj["data_file_path"], START_TIME)
		print " events json dump --- "
                print json_dump
                write_to_db(START_TIME, json_dump, config_obj["elasticsearch_instance"], config_obj["events_es_index"], config_obj["es_document"])
		#write_to_db_drill_down(START_TIME, json_dump, config_obj["elasticsearch_instance"], config_obj["events_es_index"], config_obj["es_document"])	
		#Removing update files -
		[os.remove(os.path.join(pwd, fname)) for fname in os.listdir(pwd) if fname.startswith("updates")]
		write_status(config_obj["status_file_path"])
	except Exception as e:
		print "Exception -", e
		write_status(config_obj["status_file_path"], 1, str(e))
if __name__ == "__main__":
	main(os.getcwd()+"/", "2018-02-23-00-01-01", "2018-02-23-00-30-01")
