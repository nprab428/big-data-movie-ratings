from kafka import KafkaProducer
import json
import csv
import time
BOOTSTRAP_SERVER = 'mpcs53014c10-m-6-20191016152730.us-central1-a.c.mpcs53014-2019.internal:6667'

# inspired by https://github.com/kadnan/Calories-Alert-Kafka/blob/master/producer_consumer_parse_recipes.py


def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(
            bootstrap_servers=[BOOTSTRAP_SERVER],
            acks='all',
            retries=0,
            batch_size=16384,
            linger_ms=1,
            buffer_memory=33554432)
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer


def publish_message(producer_instance, topic_name, value):
    try:
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, value=value_bytes)
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))


if __name__ == "__main__":
    kafka_producer = connect_kafka_producer()

    # write data from streaming csv to producer every 5 seconds
    with open('ratings_stream.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            publish_message(kafka_producer,
                            'nprabhu_movie_ratings',
                            json.dumps(row))
            time.sleep(5)

    if kafka_producer:
        kafka_producer.close()
