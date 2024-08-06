import hashlib
import json
import logging
import time
import uuid
import xml.etree.ElementTree as et
from dataclasses import dataclass
from threading import Lock

from flask import Flask, request
from werkzeug.exceptions import NotFound

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

app = Flask(__name__)

# Disable request logs
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

message_lock = Lock()


class Message:
    def __init__(self, body):
        self.message_id = str(uuid.uuid4())
        self.receipt_handle = str(uuid.uuid4())
        self.body = body
        self.sent_timestamp = int(time.time() * 1000)
        self.visible_after = 0
        self.receive_count = 0
        self.md5_of_body = hashlib.md5(body.encode()).hexdigest()


class Queue:
    def __init__(self, name):
        self.name = name
        self.url = f"http://localhost:4566/000000000000/{name}"
        self.messages = []

    def add_message(self, message):
        with message_lock:
            self.messages.append(message)

    def get_messages(self, max_messages, visibility_timeout):
        messages = []
        current_time = int(time.time() * 1000)

        with message_lock:
            for message in self.messages:
                if message.visible_after <= current_time and len(messages) < max_messages:
                    message.receive_count += 1
                    message.visible_after = current_time + (visibility_timeout * 1000)
                    messages.append(message)

        return messages

    def delete_message(self, receipt_handle):
        with message_lock:
            self.messages = [msg for msg in self.messages if msg.receipt_handle != receipt_handle]

    def delete_message_batch(self, entries):
        with message_lock:
            for entry in entries:
                receipt_handle = entry['ReceiptHandle']
                self.messages = [msg for msg in self.messages if msg.receipt_handle != receipt_handle]


class SQSManager:
    def __init__(self):
        self.queues = {}

    def create_queue(self, queue_name):
        if queue_name not in self.queues:
            self.queues[queue_name] = Queue(queue_name)
        return self.queues[queue_name]

    def delete_queue(self, queue_url):
        queue = self.get_queue_from_url(queue_url)
        del self.queues[queue.name]

    def get_queue_from_url(self, queue_url):
        for queue in self.queues.values():
            if queue.url == queue_url:
                return queue
        raise NotFound("Queue not found")

    def get_queue_url(self, queue_name):
        if queue_name in self.queues:
            return self.queues[queue_name].url
        raise NotFound("Queue not found")


class SQSAWSJsonProtocolMapper:
    @staticmethod
    def parse_request(data):
        return json.loads(data)

    @staticmethod
    def format_response(messages: list[Message]):
        return json.dumps(messages)


@dataclass
class SQSAWSQueryProtocolMapper:
    @staticmethod
    def parse_request(action, data):
        return {key: value for key, value in data.items()}

    @staticmethod
    def format_response(action, data):
        if action == "ReceiveMessage":
            if action == "ReceiveMessage":
                messages: list[dict] = data["Messages"]
                root = et.Element("ReceiveMessageResponse", xmlns="http://queue.amazonaws.com/doc/2012-11-05/")
                result = et.SubElement(root, "ReceiveMessageResult")

                for msg in messages:
                    message_elem = et.SubElement(result, "Message")
                    et.SubElement(message_elem, "MessageId").text = msg['MessageId']
                    et.SubElement(message_elem, "ReceiptHandle").text = msg['ReceiptHandle']
                    et.SubElement(message_elem, "MD5OfBody").text = msg['MD5OfBody']
                    et.SubElement(message_elem, "Body").text = msg['Body']

                    msg_attributes = msg['Attributes']

                    attributes = [
                        ("SenderId", "AIDASSYFHUBOBT7F4XT75"),
                        ("ApproximateFirstReceiveTimestamp", str(msg_attributes['ApproximateFirstReceiveTimestamp'])),
                        ("ApproximateReceiveCount", str(msg_attributes['ApproximateReceiveCount'])),
                        ("SentTimestamp", str(msg_attributes['SentTimestamp'])),
                    ]

                    for name, value in attributes:
                        attribute_elem = et.SubElement(message_elem, "Attribute")
                        et.SubElement(attribute_elem, "Name").text = name
                        et.SubElement(attribute_elem, "Value").text = value

                metadata = et.SubElement(root, "ResponseMetadata")
                et.SubElement(metadata, "RequestId").text = "5ba605cc-1e4b-58ba-93db-59bca8677ec9"

                return et.tostring(root, encoding="unicode")
        if action == "DeleteMessageBatch":
            root = et.Element("DeleteMessageBatchResponse", xmlns="http://queue.amazonaws.com/doc/2012-11-05/")
            result = et.SubElement(root, "DeleteMessageBatchResult")

            for entry in data:
                entry_elem = et.SubElement(result, "DeleteMessageBatchResultEntry")
                et.SubElement(entry_elem, "Id").text = entry['Id']

            metadata = et.SubElement(root, "ResponseMetadata")
            et.SubElement(metadata, "RequestId").text = "5ba605cc-1e4b-58ba-93db-59bca8677ec9"

            return et.tostring(root, encoding="unicode")

        if action == "SendMessage":
            root = et.Element("SendMessageResponse", xmlns="http://queue.amazonaws.com/doc/2012-11-05/")
            result = et.SubElement(root, "SendMessageResult")
            et.SubElement(result, "MessageId").text = data[0].message_id
            et.SubElement(result, "MD5OfMessageBody").text = data[0].md5_of_body

            metadata = et.SubElement(root, "ResponseMetadata")
            et.SubElement(metadata, "RequestId").text = "5ba605cc-1e4b-58ba-93db-59bca8677ec9"

            return et.tostring(root, encoding="unicode")

        if action == "DeleteMessage":
            root = et.Element("DeleteMessageResponse", xmlns="http://queue.amazonaws.com/doc/2012-11-05/")
            metadata = et.SubElement(root, "ResponseMetadata")
            et.SubElement(metadata, "RequestId").text = "5ba605cc-1e4b-58ba-93db-59bca8677ec9"

            return et.tostring(root, encoding="unicode")

        if action == "CreateQueue":
            root = et.Element("CreateQueueResponse", xmlns="http://queue.amazonaws.com/doc/2012-11-05/")
            result = et.SubElement(root, "CreateQueueResult")
            et.SubElement(result, "QueueUrl").text = data["QueueUrl"]

            metadata = et.SubElement(root, "ResponseMetadata")
            et.SubElement(metadata, "RequestId").text = "5ba605cc-1e4b-58ba-93db-59bca8677ec9"

            return et.tostring(root, encoding="unicode")

        if action == "GetQueueUrl":
            root = et.Element("GetQueueUrlResponse", xmlns="http://queue.amazonaws.com/doc/2012-11-05/")
            result = et.SubElement(root, "GetQueueUrlResult")
            et.SubElement(result, "QueueUrl").text = data["QueueUrl"]

            metadata = et.SubElement(root, "ResponseMetadata")
            et.SubElement(metadata, "RequestId").text = "5ba605cc-1e4b-58ba-93db-59bca8677ec9"

            return et.tostring(root, encoding="unicode")

        if action == "DeleteQueue":
            root = et.Element("DeleteQueueResponse", xmlns="http://queue.amazonaws.com/doc/2012-11-05/")
            metadata = et.SubElement(root, "ResponseMetadata")
            et.SubElement(metadata, "RequestId").text = "5ba605cc-1e4b-58ba-93db-59bca8677ec9"

            return et.tostring(root, encoding="unicode")

        return json.dumps(data)


sqs_manager = SQSManager()

# Inicialização da fila "my-queue"
sqs_manager.create_queue("my-queue")


class ProtocolHandler:
    @staticmethod
    def parse_request(data, content_type):
        if 'json' in content_type:
            return json.loads(data)
        elif 'form' in content_type:
            return {key: value for key, value in request.form.items()}
        return data

    @staticmethod
    def format_response(data, content_type, action):
        if 'json' in content_type:
            return json.dumps(data)
        elif 'form' in content_type:
            data = SQSAWSQueryProtocolMapper().format_response(action, data)
        return data


class SQSOperations:
    @staticmethod
    def createqueue(data):
        queue_name = data['QueueName']
        queue = sqs_manager.create_queue(queue_name)
        return {"QueueUrl": queue.url}

    @staticmethod
    def deletequeue(data):
        queue_url = data['QueueUrl']
        sqs_manager.delete_queue(queue_url)
        return {}

    @staticmethod
    def sendmessage(data):
        queue_url = data['QueueUrl']
        message_body = data['MessageBody']
        queue = sqs_manager.get_queue_from_url(queue_url)
        message = Message(message_body)
        queue.add_message(message)
        return {
            "MessageId": message.message_id,
            "MD5OfMessageBody": message.md5_of_body
        }

    @staticmethod
    def receivemessage(data):
        queue_url = data['QueueUrl']
        queue = sqs_manager.get_queue_from_url(queue_url)
        max_messages = int(data.get('MaxNumberOfMessages', 1))
        visibility_timeout = int(data.get('VisibilityTimeout', 30))
        wait_time_seconds = int(data.get('WaitTimeSeconds', 0))

        time.sleep(wait_time_seconds)

        messages = queue.get_messages(max_messages, visibility_timeout)
        return {
            "Messages": [
                {
                    "MessageId": msg.message_id,
                    "ReceiptHandle": msg.receipt_handle,
                    "MD5OfBody": msg.md5_of_body,
                    "Body": msg.body,
                    "Attributes": {
                        "SentTimestamp": str(msg.sent_timestamp),
                        "ApproximateReceiveCount": str(msg.receive_count),
                        "ApproximateFirstReceiveTimestamp": str(int(time.time() * 1000))
                    }
                } for msg in messages
            ]
        }

    @staticmethod
    def deletemessage(data):
        queue_url = data['QueueUrl']
        receipt_handle = data['ReceiptHandle']
        queue = sqs_manager.get_queue_from_url(queue_url)
        queue.delete_message(receipt_handle)
        return {
            "Id": receipt_handle
        }

    @staticmethod
    def deletemessagebatch(data):
        queue_url = data['QueueUrl']
        queue = sqs_manager.get_queue_from_url(queue_url)
        entries = []
        i = 1
        while f'DeleteMessageBatchRequestEntry.{i}.Id' in data:
            entry = {
                'Id': data[f'DeleteMessageBatchRequestEntry.{i}.Id'],
                'ReceiptHandle': data[f'DeleteMessageBatchRequestEntry.{i}.ReceiptHandle']
            }
            entries.append(entry)
            i += 1
        queue.delete_message_batch(entries)
        return [{"Id": entry['Id']} for entry in entries]

    @staticmethod
    def getqueueurl(data):
        queue_name = data['QueueName']
        queue_url = sqs_manager.get_queue_url(queue_name)
        return {"QueueUrl": queue_url}


@app.route('/', methods=['POST'])
def sqs_operations():
    target = request.headers.get('X-Amz-Target', '')
    target_action = target.split('.')[-1] if target else None
    content_type = request.headers.get('Content-Type', '')

    if not target and not content_type:
        return ProtocolHandler.format_response({"Error": "Invalid Request"}, content_type, None), 400

    data = ProtocolHandler.parse_request(request.data, content_type)
    action = target_action or data.get('Action', '')

    try:
        operation = getattr(SQSOperations, action.lower(), None)
        if operation:
            result = operation(data)
            return ProtocolHandler.format_response(result, content_type, action)
        else:
            return ProtocolHandler.format_response({"Error": "Invalid Action"}, content_type, action), 400
    except Exception as e:
        logger.exception(e)
        return ProtocolHandler.format_response({"Error": "InternalError", "Message": str(e)}, content_type, None), 500


@app.errorhandler(404)
def not_found(error):
    content_type = request.headers.get('Content-Type', '')
    return ProtocolHandler.format_response(
        {"Error": "NotFound", "Message": "The requested resource could not be found"},
        content_type, None), 404


if __name__ == '__main__':
    # Garantir que a fila "my-queue" existe ao iniciar o servidor
    if "my-queue" not in sqs_manager.queues:
        sqs_manager.create_queue("my-queue")
    app.run(port=8080, threaded=True)
