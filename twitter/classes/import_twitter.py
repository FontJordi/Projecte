import tweepy, time
import os, sys

sys.path.insert(1, '/home/kiwichi/Documents/Projecte/twitter/classes')

import filemanage as fm
import json
from kafka import KafkaProducer

os.chdir("/home/kiwichi/Documents/Projecte/myfolder")

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

bearer_token = fm.readTxt(fm.getPath(), "bearertoken.txt")[0]
producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=json_serializer)


class MyStream(tweepy.StreamingClient):
    
    def on_connect(self):  
        
        print("Connected")
        self.current_value = "Nothing yet"

        if(self.get_rules().data is not None):
            for data in self.get_rules().data:
                self.delete_rules(data.id)

    def on_tweet(self, tweet):

        producer.send("Twitter", value=tweet.text)
        print(tweet.text)
        self.current_value = tweet.text

        time.sleep(10)

    def start_streaming_tweets(self, search_term):

        self.add_rules(add=tweepy.StreamRule(search_term))
        self.sample()

    def on_error(self, status_code):
        print(status_code)

    def on_disconnect(self, notice):
        print("disconnected")
        return


if __name__ == "__main__":

    twitter_stream = MyStream(bearer_token=bearer_token)
    twitter_stream.start_streaming_tweets(["Messi", "Argentina"])
