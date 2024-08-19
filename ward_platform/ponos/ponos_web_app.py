import json
import redis
import uuid

from datetime import datetime, timedelta, timezone
from sanic.response import json as sanic_json
from confluent_kafka import Producer
from sanic import Sanic, response
from sanic_wtf import SanicForm
from wtforms import SubmitField, TextField
from wtforms.validators import DataRequired, Length

#Initialize Web App
app = Sanic(name="Ponos")
app.config['SECRET_KEY'] = '_'

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

#Method to publish to redis through ReJSON, uses CLI commands TODO: Investigate making sync
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

#Initialize Session
session = {}
@app.middleware('request')
async def add_session(request):
    request.ctx.session = session

class job_invocation_form(SanicForm):
    org_id = TextField('org_id', validators=[DataRequired()])
    asset_id = TextField('asset_id', validators=[DataRequired()])
    job_type = TextField('job_type', validators=[DataRequired()])
    job_version = TextField('job_version', validators=[DataRequired()])
    job_parameters = TextField('job_parameters', validators=[DataRequired()])
    submit = SubmitField('Invoke')


@app.route('/', methods=['GET', 'POST'])
async def index(request):
    form = job_invocation_form(request)
    if request.method == 'POST':
        org_id = form.org_id.data
        asset_id = form.asset_id.data
        job_type = form.job_type.data
        job_version = form.job_version.data
        job_parameters = form.job_parameters.data

        msg = '{} - {}'.format(datetime.now(), asset_id)
        session.setdefault('fb', []).append(msg)

        message = {}
        message["jobs"] = []
        message["org_id"] = org_id
        message["asset_id"] = asset_id

        job_to_invoke = {}
        job_to_invoke["invocation_timestamp"] = str(datetime.utcnow())
        invocation_expiration_time = datetime.utcnow() + timedelta(hours=24)
        job_to_invoke["invocation_expiration"] = str(invocation_expiration_time)
        job_to_invoke["invocation_id"] = str(uuid.uuid4())
        job_to_invoke["job_type"] = job_type
        job_to_invoke["job_version"] = job_version
        job_to_invoke["job_parameters"] = json.loads(job_parameters)
        message["jobs"].append(job_to_invoke)
        
        #Uses org_id + _ + asset_id as the Redis key, this will likely change in the future.
        publish_to_redis((org_id+"_"+asset_id),message,int(invocation_expiration_time.replace(tzinfo=timezone.utc).timestamp()))
        #Publish invocation to Kafka
        await produce_message_to_kafka(message,"ward_job_invocations")

        return response.redirect('/')
    content = f"""
    <h1>Ward Jobs</h1>
    <form action="" method="POST">
      {'<br>'.join(form.org_id.errors)}
      {'<br>'.join(form.asset_id.errors)}
      <br>
      {form.org_id(size=20, placeholder="org_id")}
      {form.asset_id(size=20, placeholder="asset_id")}
      {form.job_type(size=20, placeholder="job_type")}
      {form.job_version(size=10, placeholder="job_version")}
      {form.job_parameters(size=40, placeholder="job_parameters")}
      {form.submit}
    </form>
    """
    return response.html(content)

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=2500)