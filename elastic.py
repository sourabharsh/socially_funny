from elasticsearch import Elasticsearch
import traceback
import sys
import json
class Elastic(object):
    
    def __init__(self, hosts=["127.0.0.1"]):
        self.es = Elasticsearch(hosts=["127.0.0.1:9200"])
    
    
    def store_user_data(self, user_json, index='index2', doc_type='tweet'):
        screen_name = user_json['screen_name']
        user_doc =  {
            "type" : "user",
            "business_profile_state": user_json['business_profile_state'],
            "created_at": user_json['created_at'],
            "description": user_json['description'],
            "fast_followers_count":  user_json['fast_followers_count'],
            "favourites_count": user_json['favourites_count'],
            "followers_count": user_json['followers_count'],
            "friends_count":    user_json['friends_count'],
            "geo_enabled": user_json['geo_enabled'],
            "location": user_json['location'],
            "media_count": user_json['media_count'],
            "name": user_json['name'],
            "normal_followers_count": user_json['normal_followers_count'],
            "protected": user_json['protected'],
            "require_some_consent": user_json['require_some_consent'],
            "screen_name": user_json['screen_name'],
            "statuses_count": user_json['statuses_count'],
            "time_zone": user_json['time_zone'],
            "verified": user_json['verified']
        }
        
        try:        
            res = self.es.index(index= index, doc_type= doc_type, id = screen_name, body= user_doc)
            print("%s successfully %s  in elasticsearch"%(screen_name, res['result']))
            
        
        except Exception as e:
            print("Error in storing user data in elasticsearch %s"%e)
            print(json.dumps(user_doc, indent=4, sort_keys=True))
            print(traceback.format_exc())
            print(sys.exc_info()[0])
            
    
    
    def store_tweet(self, tweet_json, index= "index2", type= "tweet"):
        
        tweet_id = tweet_json["id"]
        tweet_doc = {
                "type" : "tweet",
                "url" : tweet_json['entities']["media"][0]["expanded_url"],
                "created_at": tweet_json["created_at"],
                "duration_millis": tweet_json["extended_entities"]["media"][0]["video_info"]["duration_millis"],
                "variants": tweet_json["extended_entities"]["media"][0]["video_info"]['variants'],
                "favorite_count":  tweet_json["favorite_count"],
                "full_text": tweet_json['full_text'], 
                "id": tweet_json['id'],
                "id_str": tweet_json["id_str"],
                "lang": tweet_json["lang"],
                "possibly_sensitive": tweet_json["possibly_sensitive"],
                "possibly_sensitive_appealable": tweet_json["possibly_sensitive_appealable"],
                "possibly_sensitive_editable": tweet_json["possibly_sensitive_editable"],
                "reply_count": tweet_json["reply_count"] ,
                "retweet_count": tweet_json["retweet_count"]
                }

        
        try:        
            res = self.es.index(index= index, doc_type= type, id = tweet_id, body= tweet_doc)
            print("Tweet: %s successfully %s  in elasticsearch"%(tweet_id, res['result']))
        
        except Exception as e:
            print("Error in storing Tweet data in elasticsearch %s"%e)
            print(json.dumps(tweet_doc, indent=4, sort_keys=True))
            print(traceback.format_exc())
            print(sys.exc_info()[0])
        