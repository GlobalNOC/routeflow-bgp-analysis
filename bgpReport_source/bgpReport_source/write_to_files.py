import ast
import csv
import commands
import sys
import json

def write_to_csv(flapsDict, topTalker_sources, START_TIME):
	csvFile = open("Analysis.csv", "a")
	file_to_write = csv.writer(csvFile, delimiter=',')
	Date = START_TIME[0:10]
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
			listToWrite.append(descr)
		except Exception as e:
			print ("write to cs exception ",e)
			listToWrite.append("NOT FOUND IN RADb")
		print listToWrite
		file_to_write.writerow(listToWrite)
	file_to_write.writerow([])
	csvFile.close()

def write_to_json(flapsDict, topTalker_sources, START_TIME):
	open("Analysis.json",'w').close() # to clear contents of the file  
        file_to_write = open("Analysis.json","w")
	Date = START_TIME[0:10]
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
                        listToWrite["Organization"] = descr
                except Exception as e:
			print "write to json exception ",e
                        listToWrite["Organization"] = "NOT FOUND IN RADb"
                print listToWrite
                list_file.append(listToWrite)
        file_to_write.seek(0)
        file_to_write.write(json.dumps(list_file))
        file_to_write.close()
