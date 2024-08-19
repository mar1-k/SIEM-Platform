import json
import redis

from datetime import datetime
from sanic import Sanic
from sanic.response import text
from sanic.response import json as sanic_json
from sanic.exceptions import ServerError
from confluent_kafka import Producer

#Delivery reporter (Used by ASYNC producer)
def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print("Message delivery failed: {}".format(err))
    else:
        print("Message succesfully delivered to {} [{}]".format(msg.topic(), msg.partition()))

#Produce message to Kafka (ASYNC)
async def produce_message_to_kafka(message,topic):
    #kafka_broker = "10.0.0.200:9092"
    kafka_broker = "localhost:9092"
    producer_conf = {"bootstrap.servers" : kafka_broker, "message.max.bytes" : "1000000000"}
    p = Producer(producer_conf)
    p.poll(0)
    p.produce(topic, json.dumps(message), callback=delivery_report)
    p.flush()

#Initialize Web App
app = Sanic(name="Hermes")

#Initialize Redis Connection
redis_connection = redis.Redis(host='localhost', port=6379, db=0)

@app.route("/", methods=["GET"])
async def get_jobs(request):
    #Grab request header fields
    request_header = request.headers
    print("request headers: \n" + str(request_header))

    #Grab org and asset ids from header
    org_id = request_header.get("org_id")
    asset_id = request_header.get("asset_id")

    #Handle missing org_id/asset_id cases
    if org_id == None:
        raise ServerError("Missing or invalid org_id", status_code=400)
    elif asset_id == None:
        raise ServerError("Missing or invalid asset_id", status_code=400)

    jobs = {}
    #No jobs case: we return an empty job list
    jobs["jobs"] = []

    #jobs =  {
        #"jobs":
        #[
            #{
            #"invocation_id" : "fgs",
            #"job_type": "cmd",
            #"job_version": "0.1",
            #"job_parameters": 
               #{
                #"cmd": "echo hello"
                #}
            #},
            #{
            #"invocation_id" : "fgsfds",
            #"job_type": "cmd",
            #"job_version": "0.1",
            #"job_parameters": {
                #"cmd": "echo hello 2"
                #}
            #}
        #]
    #}

    #Form redis look up key for asset
    key = org_id + "_" +asset_id

    #Query redis for jobs
    result = redis_connection.execute_command('JSON.GET', key)
    #If we have open jobs, we load them
    if result:
        jobs["jobs"] = json.loads(result)["jobs"]
        #Delete job from redis
        redis_connection.execute_command('JSON.DEL', key)

    return sanic_json(jobs)

@app.route("/", methods=["POST"])
async def handle_posted_data(request):

    #Parse JSON payload from POST request
    request_payload = request.json
    print("POST request payload: \n" + str(request_payload))

    #Grab request payload fields
    org_id = request_payload.get("org_id")
    asset_id = request_payload.get("asset_id")
    user_agent_version = request_payload.get("user_agent_version")
    definitions_current_version = request_payload.get("definitions_current_version")
    
    logs = request_payload.get("logs")
    alerts = request_payload.get("alerts")
    job_data = request_payload.get("job_data")

    #Perform ingestion of logs and alerts (Both go to ward_endpoint_messages topic)
    if logs:
        for log in logs:
        #Construct message to be sent to Kafka
            message = log
            message["org_id"] = org_id
            message["asset_id"] = asset_id
            message["ingestion_timestamp"] = str(datetime.utcnow())
            await produce_message_to_kafka(message,"ward_endpoint_messages")
    if alerts:
        for alert in alerts:
            message = alert
            message["org_id"] = org_id
            message["asset_id"] = asset_id
            message["ingestion_timestamp"] = str(datetime.utcnow())
            await produce_message_to_kafka(message,"ward_endpoint_messages")
    if job_data:
        message = job_data
        #message["org_id"] = org_id
        #message["asset_id"] = asset_id
        message["ingestion_timestamp"] = str(datetime.utcnow())
        await produce_message_to_kafka(message,"ward_endpoint_messages")

    return sanic_json({"status":"Ok, probably"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)


