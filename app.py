import base64
import io
from threading import Thread
from time import sleep

import boto3
from PIL import Image
from flask import Flask, Response

app = Flask(__name__)

stop_run = True
# SQS queue to receive messages
img_input_queue_url = "https://sqs.ap-southeast-1.amazonaws.com/263232191138/web-queue"
# SQS queue to send messages
img_output_queue_url = "https://sqs.ap-southeast-1.amazonaws.com/263232191138/application-queue"

region_name = "ap-southeast-1"


def process_msgs():
    sqs = boto3.client('sqs',region_name=region_name)
    response = sqs.receive_message(
        QueueUrl=img_input_queue_url,
        MaxNumberOfMessages=1,
        MessageAttributeNames=[
            'All'
        ],
        VisibilityTimeout=30,
        WaitTimeSeconds=0
    )
    messages = response.get("Messages", [])
    for message in messages:
        dec_str = base64.b64decode(message["Body"])
        receipt_handle = message['ReceiptHandle']
        # Delete received message from queue
        sqs.delete_message(
            QueueUrl=img_input_queue_url,
            ReceiptHandle=receipt_handle
        )
        thumb_str = create_thumbnail(dec_str)
        publish_thumbnail(thumb_str)


def create_thumbnail(img_str):
    buf = io.BytesIO(img_str)
    img = Image.open(buf)
    img.thumbnail((120, 120))
    buffered = io.BytesIO()
    img.save(buffered, format="JPEG")
    thumb_str = base64.b64encode(buffered.getvalue()).decode()
    return thumb_str


def publish_thumbnail(thumb_str):
    sqs = boto3.client("sqs",region_name=region_name)
    response = sqs.send_message(
        QueueUrl=img_output_queue_url,
        # The length of time, in seconds, for which the delivery of all messages in the queue is delayed
        DelaySeconds=5,
        MessageBody=thumb_str
    )

    print(response['MessageId'])


def my_function():
    global stop_run
    while not stop_run:
        process_msgs()
        sleep(5)
        print("running...")
    else:
        print("stopped..")


def manual_run():
    t = Thread(target=my_function)
    t.start()
    return "Processing"


@app.route("/stop", methods=['GET'])
def stop_process():
    global stop_run
    stop_run = True
    return "Application stopped"


@app.route("/run", methods=['GET'])
def run_process():
    global stop_run
    if stop_run:
        stop_run = False
        return Response(manual_run(), mimetype="text/html")
    else:
        return "Application is already in Running"


@app.route("/", methods=['GET'])
def status():
    global stop_run
    if stop_run:
        return "Application Stopped"
    else:
        return "Application Running"


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=80, debug=True)
