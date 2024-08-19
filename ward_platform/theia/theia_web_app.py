import json
from sanic import Sanic
from sanic.response import json as sanic_json
from confluent_kafka import Producer

#Initialize rule list
global rules
rules = [{'cmd_line':'mimikatz'}]

#Initialize Web App
app = Sanic(name='Theia')

#Delivery reporter (Used by ASYNC producer) #TODO: IMPLEMENT LOGGING
def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message succesfully delivered to {} [{}]'.format(msg.topic(), msg.partition()))

#Return current rules for GET requests
@app.route('/', methods=['GET'])
def get_rules(request):
   return sanic_json(rules)

#Update ruleset using JSON from POST
@app.route('/', methods=['POST'])
def update_rules(request):
    new_rules = request.json
    global rules
    rules = new_rules
    message = {}
    message['rules'] = rules
    producer_conf = {'bootstrap.servers' : "localhost:9092"}
    p = Producer(producer_conf)
    p.poll(0)
    p.produce('ward_theia_rule_updates', json.dumps(message).encode('utf-8'), callback=delivery_report)
    p.flush()
    return sanic_json({"rule_update_result":"Ok!"})

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=10000)
