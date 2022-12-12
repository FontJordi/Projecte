import tweepy, time
import os
import filemanage as fm
import json
from kafka import KafkaProducer


def json_serializer(data):
    return json.dumps(data).encode("utf-8")

bearer_token ="AAAAAAAAAAAAAAAAAAAAAG8AkQEAAAAAD96bfZgqqvgPCQezwl0bFyMMmx4%3DiHjphY15LG10yQNNk143yoWxEAg5vpgDbTISVYOPnJM0rgHJ3g"
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

            time.sleep(1)

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

#stream = MyStream(bearer_token=bearer_token)
#stream.filter(tweet_fields = ["referenced_tweets"])

#print(type(stream.filter(tweet_fields = ["referenced_tweets"])))
#stream.sample()

#stream.add_rules(tweepy.StreamRule("lol"))

#stream.filter(tweet_fields = ["referenced_tweets"])


#Response(data=[StreamRule(value='Sonny Boy', tag=None, id='1601935263453925376'), StreamRule(value='Argentina', tag=None, id='1601940833397260290'), 
#StreamRule(value='Messi', tag=None, id='1601941898003898368')], includes={}, errors=[], meta={'sent': '2022-12-11T14:10:25.224Z', 'result_count': 3})



"""
#client = tweepy.Client(bearer_token = "AAAAAAAAAAAAAAAAAAAAAG8AkQEAAAAAD96bfZgqqvgPCQezwl0bFyMMmx4%3DiHjphY15LG10yQNNk143yoWxEAg5vpgDbTISVYOPnJM0rgHJ3g")

#query = 'from:kiwichi__ -is:retweet'
#query = 'from:kiwichi__ '

#kiwichi = client.get_user(username="kiwichi__")
#tweets = client.search_recent_tweets(query=query, tweet_fields=['context_annotations', 'created_at'], max_results=100)

##print(kiwichi.data.id)
#likes = client.get_liked_tweets(kiwichi.data.id)
 
#dict1 = likes._asdict()
"""





#fm.writeResponse("/home/kiwichi/Documents/Projecte/data", "likeskiwichi", dict1)

#for like in likes.data:
#    print(like.text + "\n")

#for tweet in tweets.data:
#    print(tweet.text + "\n")
    #if len(tweet.context_annotations) > 0:
    #   print(tweet.context_annotations)



