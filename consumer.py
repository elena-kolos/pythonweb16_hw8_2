import pika
import time
import json

from producer import Contact
from mongoengine import *
from mongoengine import connect


uri = "mongodb+srv://elenakolos:AneleSolok@cluster0.oxuwivp.mongodb.net/rabbit_cp?retryWrites=true&w=majority"

connect(db='rabbit_cp', host=uri, port=27017)


def send_email(message):   
    print(f' [*] Email {message["payload"]} was successfully sent!')
  

def set_delivered(user_id):
    user = Contact.objects(id=user_id).first()
    user.is_delivered = True
    user.save()
    # print(user.name, user.is_delivered)
    print(f"[x] Delivery of {user.email} is {user.is_delivered}")

    
def callback(ch, method, properties, body):
    message = json.loads(body.decode())
    print(f" [x] Received {message}")    
    send_email(message)
    time.sleep(1)
    set_delivered(message["id"])
    
    
def consumer_service():

    credentials = pika.PlainCredentials('guest', 'guest')
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost', port=5672, credentials=credentials))
    channel = connection.channel()

    channel.queue_declare(queue='email_queue', durable=True)
    print(' [*] Waiting for messages. To exit press CTRL+C')

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='email_queue', on_message_callback=callback, auto_ack=True)
    channel.start_consuming()


if __name__ == '__main__':
   
    while True:
        consumer_service()

  