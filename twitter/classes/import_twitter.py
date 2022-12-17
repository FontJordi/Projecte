import tweepy, time
import os
import filemanage as fm
import json
from kafka import KafkaProducer

os.chdir("/home/kiwichi/Documents/Projecte/myfolder")

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

bearer_token = fm.readTxt(fm.getPath(), "bearertoken.txt")[0]
#producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=json_serializer)

print(sys.path)

class MyStream(tweepy.StreamingClient):
    
    def on_connect(self): 
 
        """ Delete previous rules if they exist. Add rules"""
        
        print("Connected")     

        if(self.get_rules().data is not None):
            for data in self.get_rules().data:
                self.delete_rules(data.id)
        
        for term in terms:

            self.add_rules(tweepy.StreamRule(term))   

    def on_tweet(self, tweet):

        """ When a tweet is recieved, send the tweet text to kafka if the tweet is not a RT"""

        if tweet.referenced_tweets == None:

            #producer.send("Twitter", value=tweet.text)
            print(tweet.text)

            time.sleep(10)

    def start_streaming_tweets(self, search_term):

        """ Start twitter stream """
        self.filter(tweet_fields = ["referenced_tweets"])

    def on_error(self, status_code):

        """ When the stream detects an error, prints it"""
        print(status_code)

    def on_disconnect(self, notice):

        """Disconnect notice"""
        print("disconnected")
        return

terms = ["Messi", "Argentina"]

if __name__ == "__main__":

    twitter_stream = MyStream(bearer_token=bearer_token)
    twitter_stream.start_streaming_tweets(terms)
