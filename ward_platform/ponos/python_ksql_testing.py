#from ksql import KSQLAPI
import redis
import json
from datetime import datetime, timedelta, timezone


#client = KSQLAPI('http://localhost:8088')
#query = client.query('select * from WARD_PONOS_JOB_INVOCATIONS EMIT CHANGES;')
#for item in query: print(item)           


r = redis.Redis(host='localhost', port=6379, db=0)
#r.delete('foo')

data = {
    'foo': 'bar',
    'ans': 42
}



print(r.keys("*"))
#timestamp = (datetime.utcnow() + timedelta(hours=24)).replace(tzinfo=timezone.utc).timestamp()
#epoch_time = int((dt.strptime(date, "date %A %B %d %H:%M:%S %Y") - epoch).total_seconds())
#print(timestamp)
#print(epoch)
#r.execute_command('JSON.SET', 'object', '.', json.dumps(data))
#r.execute_command('EXPIREAT', 'object', int(timestamp))
#r.execute_command('JSON.DEL', 'object')
result = (r.execute_command('JSON.GET', 'test_org_test_asset'))
print(result)
print(r.execute_command('TTL', 'test_org_test_asset'))