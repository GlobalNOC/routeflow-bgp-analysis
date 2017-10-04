import urllib
import json
import requests
import time
import datetime
import sys
from elasticsearch import Elasticsearch

query = json.dumps({"size":10000,"filter":{"bool":{"must":[{"range":{"start":{"gte":1502667000000,"lte":1502753400000,"format":"epoch_millis"}}}],"must_not":[]}}})
response = requests.get("http://140.182.49.116:9200/_search?pretty=1",data=query,stream=True)
content = response.raw.read( decode_content=True)
print content
