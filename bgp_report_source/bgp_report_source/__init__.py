#!/usr/bin/python
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


def process_error(func, e, errors):
    return True

def get_unix_time(date_str):
    """ Converts date time from format YYYY-MM-DD-HH-MM-SS to Unix time
    Args: date (str): datetime format - YYYY-MM-DD-HH-MM-SS
    Returns: int: Correspnding Unix timestamep
    Example: get_unix_time("2017-12-09-20-08-34") returns - 1512850114000
    """
    try:
        elems = date_str.split('-')

        date = datetime.datetime(int(elems[0]), int(elems[1]), int(elems[2]), int(elems[3]), int(elems[4]), int(elems[5]))

        epoch = datetime.datetime.utcfromtimestamp(0)

        diff = date - epoch

        unix_str = str((diff.microseconds + (diff.seconds + diff.days * 86400) * 10 ** 6)/ 10**6)

        if len(unix_str) < 13:
            for i in range(13 - len(unix_str)):
                unix_str = unix_str + "0"

        return int(unix_str)

    except Exception, e:
        print 'get_unix_time() ERROR:', e


def check_ip(ip_string):
    """ Checks if the string is in correct IPV4 address format
    Args: ip_string (str): IPv4 address string in the form - "XXX.XXX.XXX.x"
    Returns: bool : True if the format is correct, False otherwise.
    Example:
        check_ip("132.25.456.x") returns True
        check_ip("123:9983.22.1.x") return False
    """
    try:
        ip = ip_string.split('.')

        if len(ip) > 3 and int(ip[0]) and int(ip[1]) and int(ip[2]):
            return True
		
    except Exception, e:
        print "check_ip() ERROR: ", e
	
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

    try:
    	open(file_path+"status.json", 'w').close() # to clear the contents of file
        status_file = open(file_path+"status.json", "w")
        status_file.seek(0)
        status_obj = {"timestamp":datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S.%f'), "error_text":error_text, "error":error}
        status_file.write(json.dumps(status_obj))
        status_file.close()

    except Exception, e:
        print "write_status() ERROR:", e


def extract_sensor_top_talkers(nflow):
    """Returns top talkers for each sensor id
    Args: nflow (list): list of dictionaries; key represents sensor_id, values are dictionaries of data sent between source and destination IPs.
    Return: list of tuples, where each tuple reprsents - (sensor_id, source_ip, data_sent_in_bits)
    """
    try:
        return [(e['key'], f['key'], f['total_bits']['value']) for e in nflow for f in e['group_by_src_ip']['buckets'] if check_ip(f['key'])]
    except Exception, e:
        print "extract_sensor_top_talkers() ERROR:", e
        return []


def extract_events_top_talkers(nflow):
    try:
        return [(n["key"], n["total_bits"]["value"]) for n in nflow if check_ip(n["key"])]
    except Exception, e:
        print "extract_events_top_talkers() ERROR:", e
        return []


def main(config_file_path,\
    START_TIME=datetime.datetime.strftime(datetime.datetime.now()\
    - datetime.timedelta(2), '%Y-%m-%d-%H-%M-%S'),\
    END_TIME=datetime.datetime.strftime(datetime.datetime.now()\
    - datetime.timedelta(1), '%Y-%m-%d-%H-%M-%S')):

    errors = ''

    # Get the time data needed for processing
    try:
    	print "Process started at - ", datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S.%f')
        print "start - ", START_TIME
        print "end - ", END_TIME

        unix_start = get_unix_time(START_TIME)
        unix_end = get_unix_time(END_TIME)
    except Exception, e:
        err = 'main() ERROR: Could not get Unix times\n'
        errors += err
        print err, e


    # Get the configuration
    try:
        config_obj = literal_eval(open(config_file_path+"config.json", "r").read())
    except IOError, e:
        err = "main() ERROR: Could not open config JSON file\n"
        errors += err
        print err, e

    # Get the sensor top talkers data
    try:
        sensor_nflow = get_sensor_flow_entries(unix_start, unix_end, config_obj["netsage_instance"])
        sensor_top_talkers = extract_sensor_top_talkers(sensor_nflow)
    except Exception, e:
        err = "main() ERROR: Could not get sensor top talkers data\n"
        errors += err
        print err, e		

    # Get the events top talkers data
    try:
        events_nflow = get_events_flow_entries(unix_start, unix_end, config_obj["netsage_instance"])
        events_top_talkers = extract_events_top_talkers(events_nflow)
    except Exception, e:
        err = "main() ERROR: Could not get events top talkers data\n"
        errors += err
        print err, e

    # Get the routflow events data
    url_list = extrcat_url([START_TIME, END_TIME])
    
    pwd = os.getcwd()

    sensor_flaps_dict_a = {}
    sensor_flaps_dict_w = {}
    events_flaps_dict_a = {}
    events_flaps_dict_w = {}

    start = time.time()
    
    for url in url_list:
        
        # Set the start time for handling the file
        file_start = time.time()

        # Download the bz2 file
        try:
            print "\nGetting file at this url: {}".format(url)
            filename = wget.download(url)
            bzfilename = "{}/{}".format(pwd,filename)
            print '\nFile saved as "{}"'.format(bzfilename)

        except Exception, e:
            err = "main() ERROR: Could not download file at this URL {}".format(url)
            errors += err
            print err, e
        
        # Parse the bz2 file
        print 'Parsing data from "{}"...'.format(filename),
        bzfile = bz2.BZ2File(bzfilename, 'rb')

        parsed = parse(bzfile, sensor_top_talkers, events_top_talkers)
        sensor_parsed_files_a = parsed[0]
        sensor_parsed_files_w = parsed[1]
        events_parsed_files_a = parsed[2]
        events_parsed_files_w = parsed[3]

        parse_err = parsed[4]
        if not parse_err:
            print "\t[COMPLETE]"
        else:
            print "\t[FAILED] (Reason: {})".format(parse_err)

        # Sensor route information
        print "Setting route information from parsed data...",
        for key, value in sensor_parsed_files_a.iteritems():
            if key in sensor_flaps_dict_a:
                sensor_flaps_dict_a[key] += value
            else:
                sensor_flaps_dict_a[key] = value

        for key, value in sensor_parsed_files_w.iteritems():
            if key in sensor_flaps_dict_w:
                sensor_flaps_dict_w[key] += value
            else:
                sensor_flaps_dict_w[key] = value
        print "\t\t[COMPLETE]"

        # Events route information
        print "Setting events route information from parsed data...",
        for key, value in events_parsed_files_a.iteritems():
           if key in events_flaps_dict_a:
                events_flaps_dict_a[key] += value
           else:
                events_flaps_dict_a[key] = value

        for key, value in events_parsed_files_w.iteritems():
            if key in events_flaps_dict_w:
                events_flaps_dict_w[key] += value
            else:
                events_flaps_dict_w[key] = value
        print "\t[COMPLETE]"

        print 'Finished processing "{}" in {} seconds'.format(filename, round(time.time()-file_start, 3))

    # Handle sensor_flaps data
    try:
        write_to_csv(sensor_flaps_dict_a, sensor_top_talkers, config_obj["data_file_path"], config_obj["sensor-name-map"], START_TIME)
        json_dump = write_to_json(sensor_flaps_dict_a, sensor_top_talkers, config_obj["data_file_path"], config_obj["sensor-name-map"], START_TIME, "A")

        write_to_csv(sensor_flaps_dict_w, sensor_top_talkers, config_obj["data_file_path"], config_obj["sensor-name-map"], START_TIME)
        json_dump += write_to_json(sensor_flaps_dict_w, sensor_top_talkers, config_obj["data_file_path"], config_obj["sensor-name-map"], START_TIME, "W")
    except Exception, e:
        err = 'main() ERROR: Could not handle sensor_flaps_dict data\n'
        errors += err
        print err, e

    # Handle event_flaps data
    try:
        if json_dump:
            json_dump1 = copy.deepcopy(json_dump)
            write_to_db(START_TIME, json_dump, config_obj["elasticsearch_instance"], config_obj["sensor_es_index"], config_obj["es_document"])
            write_to_db_drill_down(START_TIME, json_dump1, config_obj["elasticsearch_instance"], config_obj["sensor_es_index"]+"_drill_down", config_obj["es_document"])
            print "sensor db populated "
        else:
            print "Nothing to populate for today"

        # Populate events data
        json_dump = write_to_json_events(events_flaps_dict_a, events_top_talkers, config_obj["data_file_path"], START_TIME, "A")
        json_dump += write_to_json_events(events_flaps_dict_w, events_top_talkers, config_obj["data_file_path"], START_TIME, "W")

    except Exception, e:
        err = 'main() ERROR: Could not handle event_flaps_dict data\n'
        errors += err
        print err, e

    # Write the processed json_dump to the database
    try:
        if json_dump:
            write_to_db(START_TIME, json_dump, config_obj["elasticsearch_instance"], config_obj["events_es_index"], config_obj["es_document"])
        else:
            print "Nothing to populate for today"
    except Exception, e:
        err = 'main() ERROR: Could not write to the DB'
        errors += err
        print err, e

    # Remove update files
    try:
        for fname in os.listdir(pwd):
            if fname.startswith('updates'):
                os.remove(os.path.join(pwd, fname))
    except Exception,e:
        err = 'main() ERROR: Could not remove update files\n'
        errors += err
        print err, e

    if errors:
        write_status(config_obj["status_file_path"], 1, errors)
    else:
        write_status(config_obj["status_file_path"])



#if __name__ == "__main__":
#	 main(os.getcwd()+"/", "2018-03-31-00-01-01", "2018-03-31-01-00-01")
