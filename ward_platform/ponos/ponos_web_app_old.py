import json
import redis
import uuid

from datetime import datetime, timedelta, timezone
from sanic import Sanic
from sanic.response import json as sanic_json
from confluent_kafka import Producer

#Initialize Sanic Web App
app = Sanic(name="Ponos")

#Initialize Redis Connection
redis_connection = redis.Redis(host='localhost', port=6379, db=0)

#Delivery reporter (Used by ASYNC producer) #TODO: IMPLEMENT LOGGING
def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print("Message delivery failed: {}".format(err))
    else:
        print("Message succesfully delivered to {} [{}]".format(msg.topic(), msg.partition()))

async def produce_message_to_kafka(message,topic):
    #kafka_broker = "10.0.0.200:9092"
    kafka_broker = "localhost:9092"
    producer_conf = {"bootstrap.servers" : kafka_broker, "message.max.bytes" : "1000000000"}
    p = Producer(producer_conf)
    p.poll(0)
    p.produce(topic, json.dumps(message).encode("utf-8"), callback=delivery_report)
    p.flush()
    #TODO: Implement logging

#Method to publish to redis through ReJSON, uses CLI commands
def publish_to_redis(key,invocation, expiration_timestamp):
    #First check to see if we have another entry:
    result = redis_connection.execute_command('JSON.GET', key)
    #If we have open jobs, we got to append to the job list
    if result:
        jobs_list = json.loads(result)["jobs"]
        for job in invocation["jobs"]:
            jobs_list.append(job)
        invocation["jobs"] = jobs_list
    
    #Publish to redis and set expiration
    redis_connection.execute_command('JSON.SET', key, '.', json.dumps(invocation))
    redis_connection.execute_command('EXPIREAT', key, expiration_timestamp)

@app.route("/", methods=["GET"])
def get_jobs(request): 
   return sanic_json(redis_connection.keys)

@app.route("/", methods=["POST"])
async def invoke_jobs(request):
    invocation_request = request.json

    message = {}
    message["jobs"] = []
    message["org_id"] = invocation_request["org_id"]
    message["asset_id"] = invocation_request["asset_id"]
    
    jobs_list = invocation_request.get("jobs")

    for job in jobs_list:
        job_to_invoke = job
        job_to_invoke["invocation_timestamp"] = str(datetime.utcnow())
        #Sets expiration time, hard-coded to 24 hours from now in Epoch
        invocation_expiration_time = datetime.utcnow() + timedelta(hours=24)
        job_to_invoke["invocation_expiration"] = str(invocation_expiration_time)
        job_to_invoke["invocation_id"] = str(uuid.uuid4())
        message["jobs"].append(job_to_invoke)
    
    #Uses org_id + _ + asset_id as the Redis key, this will likely change in the future
    publish_to_redis((invocation_request["org_id"]+"_"+invocation_request["asset_id"]),message,int(invocation_expiration_time.replace(tzinfo=timezone.utc).timestamp()))
    #Publish invocation to Kafka
    await produce_message_to_kafka(message,"ward_job_invocations")

    return sanic_json({"invocation_status":"Ok!"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=2500)
