
## Collect data from the Wikipedia Stream and send it to Kafka

import time, json
from sseclient import SSEClient as EventSource
from kafka import KafkaProducer

def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer

def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))

if __name__ == "__main__":

    ## Instantiate a producer
    producer = connect_kafka_producer()

    ## Give a topic
    topic = "Wikipedia"

    ## Streaming Url
    url = 'https://stream.wikimedia.org/v2/stream/recentchange'

    ## Read the stream and produce messages to Kafka
    for event in EventSource(url):
                    
                    if event.event == 'message':
                        try:

                            change = json.loads(event.data)
                            user = change["user"]
                            title = change["title"]
                            bot = change["bot"]
                            timestamp = change["meta"]["dt"]

                        except ValueError:

                            pass
                        
                        else:
                            
                            str_send = 'User ' + user + ',' + 'Title ' + title + ',' + 'Bot ' + str(bot) + ',' + 'Timestamp ' + str(timestamp) + '\n'
                            
                            ## Produce message to Kafka

                            publish_message(producer, topic, 'parsed', str_send)
                       
