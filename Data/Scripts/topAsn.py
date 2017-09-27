import commands
import sys
import json

ipFile = open("ipFile","w")

def topTalk(filename):
	with open(filename) as data_file:
		data = json.load(data_file)

	ids = set()
	output = {}
 	i=0
	for entry in data["hits"]["hits"]:
		if entry["_index"] == ".kibana":
			continue
		inner = {}
		uniqueId = entry["_id"]
		src_ipAddress = entry["_source"]["meta"]["src_ip"] 
		dst_ipAddress = entry["_source"]["meta"]["dst_ip"]
		bits_sent = (entry["_source"]["values"]["num_bits"])
		if uniqueId in ids:
			print "Duplicate flow ID found "
    		else:
      			ids.add(uniqueId)
		i+=1
		if(output.has_key(src_ipAddress)):
			if(output.get(src_ipAddress).has_key(dst_ipAddress)):
				inner[dst_ipAddress] = bits_sent + output.get(src_ipAddress).get(dst_ipAddress)
				output[src_ipAddress][dst_ipAddress] = inner[dst_ipAddress]
			else:
				inner[dst_ipAddress] = bits_sent
				output[src_ipAddress][dst_ipAddress] = inner[dst_ipAddress]
		else:
			inner[dst_ipAddress] = bits_sent
			output[src_ipAddress] = inner
	print output

# To find the src dst pair with max bits exchanged
	maxDataExchanged = -1
	srcDict = {} 
	#Calculate for each source amount of data it has sent
	for src, values in output.iteritems():
		srcDict[src] = sum(v for k,v in values.iteritems())
	print "src dict ----------"
	print srcDict
	#Get top 10 IPs with max data sent - 
	top_ten_IP = sorted(srcDict.iteritems(), key=lambda (key, value): (-value, key))[:10]
	print "Top 10 -- "
	print top_ten_IP
	for eachTuple in top_ten_IP:
		ipFile.write(str(eachTuple[0]) + " , " +str(eachTuple[1])+"\n") 
	ipFile.close()
	

if __name__ == "__main__":
	topTalk("nflow")

