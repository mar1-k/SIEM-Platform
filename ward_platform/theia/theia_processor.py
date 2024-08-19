import faust
import json
import requests 
import uuid

from datetime import datetime

#Define Faust App
app = faust.App(
    'Theia',
    broker='kafka://localhost:9092',
    value_serializer='json',
)

#Instantiate and initialize rules:
global rules
rules = json.loads(requests.get('http://localhost:10000').text)
print(rules)


#Basic matching method - STILL IN DEVELOPMENT
def check_match(message):
    print('Testing Match...')
    global rules
    print('Current rules: ' + str(rules))
    for rule in rules:
        for field in rule:
            if message.get(field) and rule[field].lower() in message.get(field).lower():
                alert = {}
                alert['org_id'] = message['org_id']
                alert['alert_id'] = str(uuid.uuid4())
                alert['status'] = 'OPEN'
                alert['alert_timestamp'] = str(datetime.utcnow())
                alert['evidence'] = message
                print('HAVE MATCH! ' + str(alert))
                return alert
    
    print('NO MATCH!')
    return None


#Updating agent, consumes from 'theia_rule_updates' topic.
@app.agent(app.topic('ward_theia_rule_updates'))
async def update(messages):
    async for message in messages:
        print(message)
        global rules
        rules = message['rules']
        print('Rules Updated Succesfully!')

#Processing agent, consumes from 'ward_endpoint_messages' topic and produces to 'ward_alerts'
@app.agent(app.topic('ward_endpoint_messages'), sink=[app.topic('ward_alerts')])
async def process(messages):
    async for message in messages:
        print(message)
        match_result = check_match(message)
        if match_result:
            yield match_result


        
