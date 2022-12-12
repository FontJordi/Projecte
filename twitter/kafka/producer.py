from kafka import KafkaProducer
import json
import sys
import time

sys.path.append('/home/kiwichi/Documents/Projecte/twitter/classes')

from import_twitter import MyStream

topic_name = "Messi"

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

producer = KafkaProducer(bootstrap_servers=['localhost:9092'], \
    value_serializer=json_serializer)

producer.send("TutorialTopic", json.dumps({"Id": 78912}))
if __name__ == "__main__":

    while 1 ==1 : ## haig de posar aixo perque envii missatge?

        my_stream = MyStream
        producer.send("TutorialTopic", my_stream.filter(tweet_fields = ["referenced_tweets"]))
        producer.send("TutorialTopic", "hi")
        producer.flush()
        print(my_stream)
        time.sleep(0.5)

