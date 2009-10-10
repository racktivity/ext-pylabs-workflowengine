import httplib
import simplejson as json

call = {'call':'cloud_api', 'rootobject':'ro_test', 'action':'test', 'params':{'num':5}, 'jobguid':'', 'executionparams':{}}

conn = httplib.HTTPConnection("localhost:9876")
print "Sending request"
conn.request("POST", "/", json.dumps(call), {"Content-type": "json"})

print "Getting response"
response = conn.getresponse()
result = response.read()
print "Response arrived"
print response.status, response.reason
print result

if response.status == 200:
    data = json.loads(result)
    print data
    
conn.close()

