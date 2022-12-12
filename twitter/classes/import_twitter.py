import tweepy, time
import os
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
        if not type(self.get_rules == None):
            if(len(self.get_rules().data) > 0):

                ids = []

                for i in range(len(self.get_rules().data)):

                    ids.append(self.get_rules().data[i].id)

                self.delete_rules(ids = ids)

            #self.add_rules(tweepy.StreamRule(term))
            #self.filter(tweet_fields = ["referenced_tweets"])

    def on_tweet(self, tweet):

        if tweet.referenced_tweets == None:

            producer.send("TutorialTopic", value=tweet.text)
            #producer.send("TutorialTopic", value="hello")
            print(tweet.text)

            time.sleep(10)

    def start_streaming_tweets(self, search_term):

        #self.add_rules(add=tweepy.StreamRule(search_term))
        #print(self.get_rules())
        #print("he")
        self.sample()
        #print(self.filter(tweet_fields = ["referenced_tweets"]))

if __name__ == "__main__":

    twitter_stream = MyStream(bearer_token=bearer_token)
    twitter_stream.start_streaming_tweets("twitter")
    #twitter_stream.filter()
