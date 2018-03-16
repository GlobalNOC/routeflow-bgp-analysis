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
		list_to_write.append(len(flaps_dict[ip_address]))
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
		list_to_write["Events"] = len(flaps_dict[ip_address])
		list_to_write["Events_Time"] = flaps_dict[ip_address]
		cmd = "whois -h whois.radb.net "+ip_address+" | grep descr:"
		try:
			descr = commands.getoutput(cmd).split("\n")[0].split(":")[1].strip(" ")
			list_to_write["Organization"] = descr
		except IndexError as e:
			print "write to json exception ", e
			list_to_write["Organization"] = "NOT FOUND IN RADb"
		
		list_file.append(list_to_write)
		'''
		#Drill down details - 
		drill_down = {"Date":"","Prefix":"", "Events":"", "Organization":""}
		drill_down["Date"] = date
		drill_down["Prefix"] = lin2[1]
		drill_down["Organization"] = list_to_write["Organization"]
		drill_down["Events"] = flaps_dict[ip_address]
		'''
	file_to_write.seek(0)
	file_to_write.write(json.dumps(list_file))
	print "sensor json - "
	print list_file
	return list_file

def write_to_json_events(flaps_dict, top_talker_sources, file_path, start_time):
        open(file_path+"events_Analysis.json", 'w').close() # to clear contents of the file
        file_to_write = open(file_path+"Analysis.json", "w")
        date = start_time[0:10]
        list_file = []
        for line2 in top_talker_sources:
		list_to_write = {"Date":"", "Prefix":"", "DataSentInbits":"", "Events":"", "Organization":""}
                list_to_write["Date"] = date
                list_to_write["Prefix"] = line2[0]
                list_to_write["DataSentInbits"] = int(line2[1])
                ip_address = line2[0]
                ip_address = ip_address[:ip_address.find("x")]+"0/24"
                list_to_write["Events"] = len(flaps_dict[ip_address])
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

def write_to_db_drill_down(START_TIME, json_dump, es_instance, bgp_index, document):
	es_object = Elasticsearch([es_instance])
	query = { "query": { "term": {"Date":START_TIME[0:10]}}}
	hits = es_object.search(index = bgp_index, body = query,scroll='1m')["hits"]["total"]
	if hits == 0:
		prep_data = []
		for each in json_dump:
			if len(each["Events_Time"]) > 0:
				for each_event in each["Events_Time"]:
					data = {'Prefix':each['Prefix'], 'Organization':each['Organization'],\
						'Date':each['Date'],\
						'Sensor':each['Sensor'],\
						'Peer_id':each_event[0], 'timestamp':each_event[1]}
					prep_data.append({"_index":bgp_index,"_type":document,"_source":data})
			else:
				data = {'Prefix':each['Prefix'], 'Organization':each['Organization'],\
                                                'Date':each['Date'],\
                                                'Sensor':each['Sensor'],\
                                                'Peer_id':None,'timestamp':None}
				prep_data.append({"_index":bgp_index,"_type":document,"_source":data})
				
		#prep_data = [{"_index":bgp_index,"_type":document,"_source":each} for each in json_dump]
		print prep_data
		helpers.bulk(es_object,prep_data) 
	else:
		print "Data already exists in ES for date ",START_TIME
#if __name__ == "__main__":
#	json_dump = [ { 'Events_Time': [ ], 'Prefix': u'200.136.80.x', 'Organization': 'ANSP', 'Date': '2018-02-23', 'DataSentInbits': 382990289010688, 'Sensor': 'GRNOC netsage', 'Events': 0 }, { 'Events_Time': [ ], 'Prefix': u'152.84.101.x', 'Organization': 'Rede Rio', 'Date': '2018-02-23', 'DataSentInbits': 11254217048064, 'Sensor': 'GRNOC netsage', 'Events': 0 }, { 'Events_Time': [ ], 'Prefix': u'192.111.108.x', 'Organization': 'NOT FOUND IN RADb', 'Date': '2018-02-23', 'DataSentInbits': 15772877651968, 'Sensor': 'GRNOC netsage', 'Events': 0 }, { 'Events_Time': [ ], 'Prefix': u'142.150.19.x', 'Organization': 'Peer1 route object', 'Date': '2018-02-23', 'DataSentInbits': 11371862294528, 'Sensor': 'GRNOC netsage', 'Events': 0 }, { 'Events_Time': [ ], 'Prefix': u'148.187.64.x', 'Organization': 'CSCS-NET', 'Date': '2018-02-23', 'DataSentInbits': 9787443838976, 'Sensor': 'GRNOC netsage', 'Events': 0 }, { 'Events_Time': [ [ '212.66.96.126', 1519382973 ], [ '212.66.96.126', 1519383369 ], [ '212.66.96.126', 1519382973 ], [ '212.66.96.126', 1519383369 ] ], 'Prefix': u'192.12.15.x', 'Organization': 'Brookhaven National Laboratory', 'Date': '2018-02-23', 'DataSentInbits': 8452541579264, 'Sensor': 'GRNOC netsage', 'Events': 4 }, { 'Events_Time': [ ], 'Prefix': u'128.142.210.x', 'Organization': 'CERN- European Organization for Nuclear Research', 'Date': '2018-02-23', 'DataSentInbits': 6203557543936, 'Sensor': 'GRNOC netsage', 'Events': 0 }, { 'Events_Time': [ ], 'Prefix': u'13.107.4.x', 'Organization': 'Microsoft via EMIX', 'Date': '2018-02-23', 'DataSentInbits': 9230998896640, 'Sensor': 'GRNOC netsage', 'Events': 0 }, { 'Events_Time': [ ], 'Prefix': u'131.94.186.x', 'Organization': 'University of Miami (UMIA-Z)', 'Date': '2018-02-23', 'DataSentInbits': 14908216442880, 'Sensor': 'GRNOC netsage', 'Events': 0 }, { 'Events_Time': [ ], 'Prefix': u'190.103.184.x', 'Organization': 'Florida LambdaRail Member Prefix', 'Date': '2018-02-23', 'DataSentInbits': 60208524230656, 'Sensor': 'GRNOC netsage', 'Events': 0 }, { 'Events_Time': [ [ '212.66.96.126', 1519382973 ], [ '212.66.96.126', 1519383369 ], [ '212.66.96.126', 1519382973 ], [ '212.66.96.126', 1519383369 ] ], 'Prefix': u'192.12.15.x', 'Organization': 'Brookhaven National Laboratory', 'Date': '2018-02-23', 'DataSentInbits': 39876651767712, 'Sensor': 'NEAAR New York', 'Events': 4 }, { 'Events_Time': [ [ '212.66.96.126', 1519382972 ] ], 'Prefix': u'154.114.13.x', 'Organization': 'TENET Block', 'Date': '2018-02-23', 'DataSentInbits': 4931701548144, 'Sensor': 'NEAAR New York', 'Events': 1 }, { 'Events_Time': [ ], 'Prefix': u'128.142.209.x', 'Organization': 'CERN- European Organization for Nuclear Research', 'Date': '2018-02-23', 'DataSentInbits': 8582859400616, 'Sensor': 'NEAAR New York', 'Events': 0 }, { 'Events_Time': [ ], 'Prefix': u'131.225.205.x', 'Organization': 'Fermi National Accelerator Lab', 'Date': '2018-02-23', 'DataSentInbits': 7089780504368, 'Sensor': 'NEAAR New York', 'Events': 0 }, { 'Events_Time': [ ], 'Prefix': u'132.230.203.x', 'Organization': 'UNI-FREIBURG', 'Date': '2018-02-23', 'DataSentInbits': 3438795861488, 'Sensor': 'NEAAR New York', 'Events': 0 }, { 'Events_Time': [ ], 'Prefix': u'131.225.207.x', 'Organization': 'Fermi National Accelerator Lab', 'Date': '2018-02-23', 'DataSentInbits': 4429510089800, 'Sensor': 'NEAAR New York', 'Events': 0 }, { 'Events_Time': [ ], 'Prefix': u'131.225.206.x', 'Organization': 'Fermi National Accelerator Lab', 'Date': '2018-02-23', 'DataSentInbits': 5125179114560, 'Sensor': 'NEAAR New York', 'Events': 0 }, { 'Events_Time': [ ], 'Prefix': u'192.5.207.x', 'Organization': 'AS111 proxy-registered route by Cogent', 'Date': '2018-02-23', 'DataSentInbits': 3872764837232, 'Sensor': 'NEAAR New York', 'Events': 0 }, { 'Events_Time': [ ], 'Prefix': u'193.58.172.x', 'Organization': 'IIHE', 'Date': '2018-02-23', 'DataSentInbits': 8282135725336, 'Sensor': 'NEAAR New York', 'Events': 0 }, { 'Events_Time': [ ], 'Prefix': u'128.142.32.x', 'Organization': 'CERN- European Organization for Nuclear Research', 'Date': '2018-02-23', 'DataSentInbits': 4094512599448, 'Sensor': 'NEAAR New York', 'Events': 0 }, { 'Events_Time': [ ], 'Prefix': u'137.189.4.x', 'Organization': 'Nexusguard (Customer Route)', 'Date': '2018-02-23', 'DataSentInbits': 2641743069184, 'Sensor': 'RTR transpac', 'Events': 0 }, { 'Events_Time': [ ], 'Prefix': u'171.64.68.x', 'Organization': 'AS32 proxy-registered route by Cogent', 'Date': '2018-02-23', 'DataSentInbits': 1659605639168, 'Sensor': 'RTR transpac', 'Events': 0 }, { 'Events_Time': [ ], 'Prefix': u'128.91.91.x', 'Organization': 'Comcast Cable Communications, Inc.', 'Date': '2018-02-23', 'DataSentInbits': 1872059936768, 'Sensor': 'RTR transpac', 'Events': 0 }, { 'Events_Time': [ ], 'Prefix': u'140.112.101.x', 'Organization': 'NTU , sub-set (proxy-registered route object)', 'Date': '2018-02-23', 'DataSentInbits': 748991922176, 'Sensor': 'RTR transpac', 'Events': 0 }, { 'Events_Time': [ ], 'Prefix': u'203.181.249.x', 'Organization': 'Proxy-registered route object', 'Date': '2018-02-23', 'DataSentInbits': 11203331448832, 'Sensor': 'RTR transpac', 'Events': 0 }, { 'Events_Time': [ ], 'Prefix': u'111.68.99.x', 'Organization': 'TW1 via EMIX', 'Date': '2018-02-23', 'DataSentInbits': 440758861824, 'Sensor': 'RTR transpac', 'Events': 0 }, { 'Events_Time': [ ], 'Prefix': u'150.26.108.x', 'Organization': 'Ministry of Agriculture Forestry and Fisheries Research Council', 'Date': '2018-02-23', 'DataSentInbits': 602112835584, 'Sensor': 'RTR transpac', 'Events': 0 }, { 'Events_Time': [ ], 'Prefix': u'137.229.87.x', 'Organization': 'University of Alaska', 'Date': '2018-02-23', 'DataSentInbits': 410379448320, 'Sensor': 'RTR transpac', 'Events': 0 }, { 'Events_Time': [ ], 'Prefix': u'139.19.86.x', 'Organization': 'MPII-139-19', 'Date': '2018-02-23', 'DataSentInbits': 415716032512, 'Sensor': 'RTR transpac', 'Events': 0 }, { 'Events_Time': [ ], 'Prefix': u'133.11.57.x', 'Organization': 'The University of Tokyo', 'Date': '2018-02-23', 'DataSentInbits': 313396854784, 'Sensor': 'RTR transpac', 'Events': 0 }, { 'Events_Time': [ ], 'Prefix': u'136.156.180.x', 'Organization': 'European Centre for Medium-Range Weather Forecasts', 'Date': '2018-02-23', 'DataSentInbits': 230469158352, 'Sensor': 'NETSAGE transpac', 'Events': 0 }, { 'Events_Time': [ ], 'Prefix': u'128.91.91.x', 'Organization': 'Comcast Cable Communications, Inc.', 'Date': '2018-02-23', 'DataSentInbits': 817512628768, 'Sensor': 'NETSAGE transpac', 'Events': 0 }, { 'Events_Time': [ ], 'Prefix': u'192.203.115.x', 'Organization': 'NOT FOUND IN RADb', 'Date': '2018-02-23', 'DataSentInbits': 1157238937048, 'Sensor': 'NETSAGE transpac', 'Events': 0 }, { 'Events_Time': [ ], 'Prefix': u'203.181.249.x', 'Organization': 'Proxy-registered route object', 'Date': '2018-02-23', 'DataSentInbits': 1274890111624, 'Sensor': 'NETSAGE transpac', 'Events': 0 }, { 'Events_Time': [ [ '212.66.96.126', 1519383369 ] ], 'Prefix': u'203.30.39.x', 'Organization': 'Starhub Internet Pte Ltd.', 'Date': '2018-02-23', 'DataSentInbits': 429210834808, 'Sensor': 'NETSAGE transpac', 'Events': 1 }, { 'Events_Time': [ ], 'Prefix': u'198.129.254.x', 'Organization': 'ESnet-CIDR-B', 'Date': '2018-02-23', 'DataSentInbits': 466122790152, 'Sensor': 'NETSAGE transpac', 'Events': 0 }, { 'Events_Time': [ ], 'Prefix': u'203.178.142.x', 'Organization': 'WIDE Project Backbone', 'Date': '2018-02-23', 'DataSentInbits': 590803412808, 'Sensor': 'NETSAGE transpac', 'Events': 0 }, { 'Events_Time': [ ], 'Prefix': u'198.124.238.x', 'Organization': 'ESnet-CIDR-A', 'Date': '2018-02-23', 'DataSentInbits': 182049946792, 'Sensor': 'NETSAGE transpac', 'Events': 0 }, { 'Events_Time': [ ], 'Prefix': u'129.132.86.x', 'Organization': 'ETH-NET', 'Date': '2018-02-23', 'DataSentInbits': 23342331480, 'Sensor': 'NETSAGE transpac', 'Events': 0 }, { 'Events_Time': [ ], 'Prefix': u'202.179.252.x', 'Organization': 'PACNET (proxy-registered route object)', 'Date': '2018-02-23', 'DataSentInbits': 49257415824, 'Sensor': 'NETSAGE transpac', 'Events': 0 } ]
#	write_to_db_drill_down('2018-02-23-00-01-01', json_dump, 'localhost', 'sensor_bgp_events_drill_down', 'bgpData')


