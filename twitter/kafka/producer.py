from kafka import KafkaProducer
import json
import time
from classes import import_twitter
from classes import filemanage as fm

topic_name = ["Messi --lang:es"]

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

bearer_token = fm.readTxt(fm.getPath(), "bearertoken.txt")[0]

producer = KafkaProducer(bootstrap_servers=['localhost:9092'], \
    value_serializer=json_serializer)

if __name__ == "__main__":

    while 1 == 1 : ## haig de posar aixo perque envii missatge?

        my_stream = import_twitter.MyStream(bearer_token=bearer_token)
        my_stream.start_streaming_tweets(topic_name)

        ## Python stays here, if I wanted to add more sources of streaming data to kafka, I'd use this
        ## .py file and add threading, so I have all kafka producer.send centralized here.

        print("aaaa")
        producer.send("Twitter", my_stream.current_value)
        #producer.flush()
