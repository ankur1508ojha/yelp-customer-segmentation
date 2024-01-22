import datetime
import sys

from kafka import KafkaProducer
import io
import time


class Producer:

    def __init__(self, _server, _topic):
        self.topic = topic
        self.producer = KafkaProducer(bootstrap_servers=server)

    def publish(self, message):
        print(f"{datetime.datetime.now()} publishing to {self.topic = }, {message = }")
        self.producer.send(self.topic, bytes(message, encoding='utf-8'))

    def publish_review(self, data_file_path, index):
        print("Publishing review")
        with io.open(data_file_path, 'r') as f:
            for i, message in enumerate(f):
                print(i, message)
                if i <= index:
                    continue

                self.publish(message)
                if i % 1000 == 0:
                    time.sleep(1)

                if i >= index+10000:
                    break


if __name__ == "__main__":
    if len(sys.argv) >= 5:
        server = sys.argv[1] # "54.193.66.237:9092"
        topic = sys.argv[2] # "review"
        file_path = sys.argv[3] # "file_path_for_the_yelp_reviews"
        so_far_completed = int(sys.argv[4])
        producer = Producer(server, topic)
        producer.publish_review(file_path, so_far_completed)
    else:
        print("Insufficient arguments. Usage: python producer.py <server> <topic> <file_path> <so_far_completed>")
#%%
