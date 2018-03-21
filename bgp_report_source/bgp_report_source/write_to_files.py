import csv
import commands
import json
from elasticsearch import Elasticsearch, helpers

def write_to_csv(flaps_dict, top_talker_sources, file_path, sensor_name_map, start_time):
	csv_file = open(file_path+"Analysis.csv", "a")
	file_to_write = csv.writer(csv_file, delimiter=',')
	date = start_time[0:10]
	for line2 in top_talker_sources:
		list_to_write = []
		list_to_write.append(date)
		sensor_name = line2[0]
		if sensor_name in sensor_name_map:
			sensor_name = sensor_name_map[sensor_name]
		list_to_write.append(sensor_name)
		list_to_write.append(line2[1])
		list_to_write.append(line2[2])
		ip_address = list_to_write[2]
		ip_address = ip_address[:ip_address.find("x")]+"0/24"
		list_to_write.append(flaps_dict[ip_address])
		cmd = "whois -h whois.radb.net "+ip_address+" | grep descr:"
		try: #When no records were found for particular IP, ignore them
			descr = commands.getoutput(cmd).split("\n")[0].split(":")[1].strip(" ")
			list_to_write.append(descr)
		except IndexError as e:
			print ("write to cs exception ", e)
			list_to_write.append("NOT FOUND IN RADb")
		file_to_write.writerow(list_to_write)
	file_to_write.writerow([])
	csv_file.close()


def write_to_json(flaps_dict, top_talker_sources, file_path, sensor_name_map, start_time):
	open(file_path+"Analysis.json", 'w').close() # to clear contents of the file
	file_to_write = open(file_path+"Analysis.json", "w")
	date = start_time[0:10]
	list_file = []
	for line2 in top_talker_sources:
		list_to_write = {"Date":"", "Sensor":"", "Prefix":"", "DataSentInbits":"", "Events":"", "Organization":""}
		list_to_write["Date"] = date
		sensor_name = line2[0]
                if sensor_name in sensor_name_map:
                        sensor_name = sensor_name_map[sensor_name]
		list_to_write["Sensor"] = sensor_name
		list_to_write["Prefix"] = line2[1]
		list_to_write["DataSentInbits"] = int(line2[2])
		ip_address = line2[1]
		ip_address = ip_address[:ip_address.find("x")]+"0/24"
		list_to_write["Events"] = flaps_dict[ip_address]
		list_to_write["Events_Time"] = flaps_dict[ip_address]
		cmd = "whois -h whois.radb.net "+ip_address+" | grep descr:"
		try:
			descr = commands.getoutput(cmd).split("\n")[0].split(":")[1].strip(" ")
			list_to_write["Organization"] = descr
		except IndexError as e:
			print "write to json exception ", e
			list_to_write["Organization"] = "NOT FOUND IN RADb"
		
		list_file.append(list_to_write)
	file_to_write.seek(0)
	file_to_write.write(json.dumps(list_file))
	print "_____________________*********************"
	print "sensor json - "
	print list_file
	print "*****************************************"
	return list_file

def write_to_json_events(flaps_dict, top_talker_sources, file_path, start_time):
        open(file_path+"events_Analysis.json", 'w').close() # to clear contents of the file
        file_to_write = open(file_path+"events_Analysis.json", "w")
        date = start_time[0:10]
        list_file = []
        for line2 in top_talker_sources:
		list_to_write = {"Date":"", "Prefix":"", "DataSentInbits":"", "Events":"", "Organization":""}
                list_to_write["Date"] = date
                list_to_write["Prefix"] = line2[0]
                list_to_write["DataSentInbits"] = int(line2[1])
                ip_address = line2[0]
                ip_address = ip_address[:ip_address.find("x")]+"0/24"
		list_to_write["Events"] = flaps_dict[ip_address]
		list_to_write["Events_Time"] = flaps_dict[ip_address]
                cmd = "whois -h whois.radb.net "+ip_address+" | grep descr:"
                try:
                        descr = commands.getoutput(cmd).split("\n")[0].split(":")[1].strip(" ")
                        list_to_write["Organization"] = descr
                except IndexError as e:
                        print "write to json exception ", e
                        list_to_write["Organization"] = "NOT FOUND IN RADb"
                list_file.append(list_to_write)
        file_to_write.seek(0)
        file_to_write.write(json.dumps(list_file))
	print "events json - "
	print list_file
        return list_file


def write_to_db(START_TIME, json_dump, es_instance, bgp_index, document):
	es_object = Elasticsearch([es_instance])
	query = { "query": { "term": {"Date":START_TIME[0:10]}}}
	hits = es_object.search(index = bgp_index, body = query,scroll='1m')["hits"]["total"]
	if hits == 0:
		prep_data = [{"_index":bgp_index,"_type":document,"_source":each} for each in json_dump]
		print prep_data
		helpers.bulk(es_object,prep_data) 
	else:
		print "Data already exists in ES for date ",START_TIME


