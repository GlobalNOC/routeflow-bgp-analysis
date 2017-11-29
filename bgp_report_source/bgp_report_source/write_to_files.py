import csv
import commands
import json

def write_to_csv(flaps_dict, top_talker_sources, file_path, start_time):
	csv_file = open(file_path+"Analysis.csv", "a")
	file_to_write = csv.writer(csv_file, delimiter=',')
	date = start_time[0:10]
	for line2 in top_talker_sources:
		list_to_write = []
		list_to_write.append(date)
		list_to_write.append(line2[0])
		list_to_write.append(line2[1])
		ip_address = list_to_write[1]
		ip_address = ip_address[:ip_address.find("x")]+"0/24"
		list_to_write.append(flaps_dict[ip_address])
		cmd = "whois -h whois.radb.net "+ip_address+" | grep descr:"
		print "ip is -- ", ip_address
		try: #When no records were found for particular IP, ignore them
			descr = commands.getoutput(cmd).split("\n")[0].split(":")[1].strip(" ")
			list_to_write.append(descr)
		except IndexError as e:
			print ("write to cs exception ", e)
			list_to_write.append("NOT FOUND IN RADb")
		print list_to_write
		file_to_write.writerow(list_to_write)
	file_to_write.writerow([])
	csv_file.close()

def write_to_json(flaps_dict, top_talker_sources, file_path, start_time):
	open(file_path+"Analysis.json", 'w').close() # to clear contents of the file
	file_to_write = open("Analysis.json", "w")
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
		cmd = "whois -h whois.radb.net "+ip_address+" | grep descr:"
		try:
			descr = commands.getoutput(cmd).split("\n")[0].split(":")[1].strip(" ")
			list_to_write["Organization"] = descr
		except IndexError as e:
			print "write to json exception ", e
			list_to_write["Organization"] = "NOT FOUND IN RADb"
		print list_to_write
		list_file.append(list_to_write)
	file_to_write.seek(0)
	file_to_write.write(json.dumps(list_file))
	file_to_write.close()
