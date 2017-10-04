'''
Input: Start date and End date (Date range)
Output: Text file "nflow" which contains Flow entries in the given range
Function: This script takes the date range from user and queries for netflow data from ELK in the given date range. The flow entries are then written to a file called "nflow" which is used by topAsn.py to find the top talkers
'''

import urllib
import json
import requests
import time
import datetime
import sys
f = open("nflow","w")

# Get flow entries in the range[start,end] using HTTP GET from ELK
def getFlowEntries(start,end):
  query = json.dumps({"size":10000,"filter":{"bool":{"must":[{"range":{"start":{"gte":start,"lte":end,"format":"epoch_millis"}}}],"must_not":[]}}})
  response = requests.get("http://140.182.49.116:9200/_search?pretty=1",data=query,stream=True)
  content = response.raw.read( decode_content=True)
  f.write(content)
  f.close()


# Converts from readable form year-month-day-hour-min-sec to unix time 
def getUnixTime(dt):
  splitDate = dt.split('-')
  mydat = datetime.datetime(int(splitDate[0]),int(splitDate[1]),int(splitDate[2]),int(splitDate[3]),int(splitDate[4]),int(splitDate[5]))
  epoch = datetime.datetime.utcfromtimestamp(0)
  td = mydat - epoch
  unixtime = (td.microseconds + (td.seconds + td.days * 86400) * 10 ** 6 )/ 10**6
  unixStr = str(unixtime)
  if len(unixStr) <13:
    for i in range (13 - len(unixStr)):
      unixStr = unixStr + "0"
  return int(unixStr)

if __name__ == '__main__':
  if len(sys.argv) < 3 :
    print " usage --> 'python getRequest startDate(year-month-day-hour-min-sec) endDate"
    exit(1)
  
  # get unix data type from the given time
  start = getUnixTime(sys.argv[1])
  end = getUnixTime(sys.argv[2])
  getFlowEntries(start,end)
