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

# Global Constants
ERRORS        = ''
DEFAULT_START = datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(2), '%Y-%m-%d-%H-%M-%S')
DEFAULT_END   = datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(1), '%Y-%m-%d-%H-%M-%S')

# Handles error logging and tracking for status output
def log_error(func, e):
    err_str = "{}() ERROR: {}\n".format(func, e)
    ERRORS += err_str
    print err_str

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
        log_error('get_unix_time', e)


def read_config(config_path):
    try:
        return literal_eval(open(config_path+"config.json", "r").read())
    except IOError, e:
        log_error('main', e)

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
        log_error('check_ip', e)
	
    return False


def write_status(file_path, error=0, error_text=""):
    """ Writes error message to status.json at loaction given by filepath 
    Args:
        file_path (str) : location on disk to create/write to status.json file 
        error (int) : Integer number representing error number. default = 0
        error_text (str) : Error text to be writtent to status.json file. default = ""
    """
    
    if error:
        print "Error(s) occurred, check the status file here: {}".format(file_path)

    try:
    	open(file_path+"status.json", 'w').close() # to clear the contents of file
        status_file = open(file_path+"status.json", "w")
        status_file.seek(0)
        status_obj = {"timestamp":datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S.%f'), "error_text":error_text, "error":error}
        status_file.write(json.dumps(status_obj))
        status_file.close()

    except Exception, e:
        log_error('write_status', e)


def extract_sensor_top_talkers(nflow):
    """Returns top talkers for each sensor id
    Args: nflow (list): list of dictionaries; key represents sensor_id, values are dictionaries of data sent between source and destination IPs.
    Return: list of tuples, where each tuple reprsents - (sensor_id, source_ip, data_sent_in_bits)
    """
    try:
        return [(e['key'], f['key'], f['total_bits']['value']) for e in nflow for f in e['group_by_src_ip']['buckets'] if check_ip(f['key'])]
    except Exception, e:
        log_error('extract_sensor_top_talkers', e)
        return []


def extract_events_top_talkers(nflow):
    try:
        return [(n["key"], n["total_bits"]["value"]) for n in nflow if check_ip(n["key"])]
    except Exception, e:
        log_error('extract_events_top_talkers', e)
        return []


def get_file(url, cwd):

    # Download the bz2 file
    try:
        print "\nGetting file at this url: {}".format(url)
        name = wget.download(url)
        bzfilename = "{}/{}".format(cwd, name)
        print '\nFile saved as "{}"'.format(bzfilename)

    except Exception, e:
        log_error('get_file', e)

    # Parse the bz2 file
    try:
        bzfile = bz2.BZ2File(bzfilename, 'rb')
    except Exception, e:
        log_error('get_file', e)

    return (bzfile, name)


def main(config_file_path, START_TIME=DEFAULT_START, END_TIME=DEFAULT_END):

    # Set the start time to track the run time duration
    start = time.time()

    # Output the time the report started and the time range of data wanted
    print "BGP Report started at {}\n".format(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S.%f'))
    print "START:\t\t{}".format(START_TIME)
    print "END:\t\t{}".format(END_TIME)

    # Get the CWD to use for files
    cwd = os.getcwd()

    # Get the start and end times as Unix epochs
    unix_start = get_unix_time(START_TIME)
    unix_end   = get_unix_time(END_TIME)

    # Get the configuration
    config = read_config(config_file_path)

    # Get the sensor top talkers data
    sensor_nflow       = get_sensor_flow_entries(unix_start, unix_end, config["netsage_instance"])
    sensor_top_talkers = extract_sensor_top_talkers(sensor_nflow)

    # Get the events top talkers data
    events_nflow       = get_events_flow_entries(unix_start, unix_end, config["netsage_instance"])
    events_top_talkers = extract_events_top_talkers(events_nflow)

    # Get the array of URLs to download bz2 files from
    urls = extrcat_url([START_TIME, END_TIME])
    
    # Initialize sensor/event flap dictionaries
    sensor_a = {}
    sensor_w = {}
    events_a = {}
    events_w = {}

    # Iterate over every data file URL
    for url in urls:
        
        # Set the start time for handling the file
        file_start = time.time()

        # Get and read the bz2 file at "url" and its name
        bzfile, name = get_file(url, cwd)

        # Parse the data in the bz2 file
        print 'Parsing data from "{}"...'.format(name),
        parsed = parse(bzfile, sensor_top_talkers, events_top_talkers)
        sensor_parsed_a = parsed[0]
        sensor_parsed_w = parsed[1]
        events_parsed_a = parsed[2]
        events_parsed_w = parsed[3]

        # Check for parsing errors
        if not parsed[4]:
            print "\t[COMPLETE]"
        else:
            print "\t[FAILED] (Reason: {})".format(parsed[4])

        # Sensor route information
        print "Setting sensor routing information from parsed data...",
        for key, value in sensor_parsed_a.iteritems():
            if key in sensor_a:
                sensor_a[key] += value
            else:
                sensor_a[key] = value

        for key, value in sensor_parsed_w.iteritems():
            if key in sensor_w:
                sensor_w[key] += value
            else:
                sensor_w[key] = value
        print "\t[COMPLETE]"

        # Events route information
        print "Setting event routing information from parsed data...",
        for key, value in events_parsed_a.iteritems():
           if key in events_a:
                events_a[key] += value
           else:
                events_a[key] = value

        for key, value in events_parsed_w.iteritems():
            if key in events_w:
                events_w[key] += value
            else:
                events_w[key] = value
        print "\t[COMPLETE]"

        print 'Finished processing "{}" in {} seconds'.format(name, round(time.time()-file_start, 3))

    # Create a JSON dump array of all the data written
    json_dump = []

    print '\nFinalizing Sensor-A Data:\n{}'.format('-'*48)
    # Write sensor_a data to CSV
    result, err = write_to_csv(sensor_a, sensor_top_talkers, config["data_file_path"], config["sensor-name-map"], START_TIME)
    if err:
        log_error(result, err)

    # Write sensor_a data to JSON
    result, err = write_to_json(sensor_a, sensor_top_talkers, config["data_file_path"], config["sensor-name-map"], START_TIME, "A")
    if err:
        log_error(result, err)
    else:
        # Add the result to the JSON dump
        json_dump += result

    print '\nFinalizing Sensor-W Data:\n{}'.format('-'*48)
    # Write sensor_w data to CSV
    result, err = write_to_csv(sensor_w, sensor_top_talkers, config["data_file_path"], config["sensor-name-map"], START_TIME)
    if err:
        log_error(result, err)

    # Write sensor_w data to JSON
    result, err = write_to_json(sensor_w, sensor_top_talkers, config["data_file_path"], config["sensor-name-map"], START_TIME, "W")
    if err:
        log_error(result, err)
    else:
        # Add the result to the JSON dump
        json_dump += result

    # Write the sensor data to Elastic
    print '\nAdding sensor data to Elastic:\n{}'.format('-'*48)
    if json_dump:

        # Copy the JSON dump
        json_dump1 = copy.deepcopy(json_dump)

        # Write the JSON dump to Elastic
        result, err = write_to_db(START_TIME, json_dump, config["elasticsearch_instance"], config["sensor_es_index"], config["es_document"])
        if err:
            log_error(result, err)

        # Write the drilldown of the JSON dump to Elastic
        result, err = write_to_db_drill_down(START_TIME, json_dump1, config["elasticsearch_instance"], config["sensor_es_index"]+"_drill_down", config["es_document"])
        if err:
            log_error(result, err)

    else:
        print "Nothing to populate for the time period!"

    # Reset the JSON dump array
    json_dump = []

    # Populate events data
    result, err = write_to_json_events(events_a, events_top_talkers, config["data_file_path"], START_TIME, "A")
    if err:
        log_error(result, err)
    else:
        json_dump += result

    result, err = write_to_json_events(events_w, events_top_talkers, config["data_file_path"], START_TIME, "W")
    if err:
        log_error(result, err)
    else:
        json_dump += result

    # Write the event data to Elastic
    print '\nAdding event data to Elastic:\n{}'.format('-'*48)
    if json_dump:
        result, err = write_to_db(START_TIME, json_dump, config["elasticsearch_instance"], config["events_es_index"], config["es_document"])
        if err:
            log_error(result, err)
    else:
        print "Nothing to populate for the time period!"

    # Remove update files
    try:
        for fname in os.listdir(cwd):
            if fname.startswith('updates'):
                os.remove(os.path.join(cwd, fname))
    except IOError, e:
        log_error('main', e)

    # Write the status file with or without any errors
    if ERRORS:
        write_status(config["status_file_path"], 1, ERRORS)
    else:
        write_status(config["status_file_path"])

