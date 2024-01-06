from datetime import datetime
import json
import pika
from mongoengine import *
from mongoengine import connect

from faker import Faker
from pymongo.mongo_client import MongoClient

uri = "mongodb+srv://elenakolos:AneleSolok@cluster0.oxuwivp.mongodb.net/rabbit_cp?retryWrites=true&w=majority"
# Create a new client and connect to the server
client = MongoClient(uri)
# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

connect(db='rabbit_cp', host=uri, port=27017)

fake = Faker()


class Contact(Document):
    """ім'я, email та логічне поле"""

    name = StringField(max_length=100, min_length=4)
    email = StringField(max_length=40, min_length=4)
    is_delivered = BooleanField(default=False)


credentials = pika.PlainCredentials('guest', 'guest')
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost', port=5672, credentials=credentials))
channel = connection.channel()

channel.exchange_declare(exchange='email_service', exchange_type='direct')
channel.queue_declare(queue='email_queue', durable=True)
channel.queue_bind(exchange='email_service', queue='email_queue')


def main():

    for _ in range(2):
        contact = Contact(name=fake.name(), email=fake.email())       
        contact.save()

    contacts = Contact.objects()
    for contact in contacts:

        message = {
            "id": str(contact.id),
            "payload": [contact.name, contact.email],
            "date": datetime.now().isoformat(),
            "text": 'test message',
            }

        channel.basic_publish(
            exchange='email_service',
            routing_key='email_queue',
            body=json.dumps(message).encode(),
            properties=pika.BasicProperties(
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
            ))
        print(" [x] Sent %r" % message)
        
    connection.close()


if __name__ == '__main__':
    main()
   