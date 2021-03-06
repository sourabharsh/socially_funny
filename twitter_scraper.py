import json
import re
import time
import random
import requests
from bs4 import BeautifulSoup
from termcolor import colored
from elastic import Elastic 
import traceback
from urllib.parse import urlencode
class Twitter_Scraper(object):
    
    def __init__(self):
        pass 
    
    """
        a method to get all the information for a given tweet id
    """
    
    def get_tweet_ids(self, query, max_position = None):
        
        url = "https://twitter.com/i/search/timeline/?"
        params = {
            'f'         :   'videos',
            'vertical'  :   "default",
            #'q'         :   'near:"India" since:2018-06-10 until:2018-06-11',
            'q'         :    query,
            'src'       :   'typd'
        } 
        params_encoded  = urlencode(params)
        #print("Encoded query", params_encoded)
        headers = {
            'authority' :   'twitter.com',
            'method'    :   'GET',
            #'path'      :   "/i/search/timeline?f=videos&vertical=default&q=near%3A%22India%22%20since%3A2018-06-10%20until%3A2018-06-11&src=typd",  
            'path'      :   "/i/search/timeline?%s"%params_encoded,  
            'scheme'    :   'https',
            'accept'    :   "application/json, text/javascript, */*; q=0.01",
            'accept-encoding'   :   'gzip, deflate, br',
            'accept-language'   :   'en-US,en;q=0.9',
            'cache-control'     :   'no-cache',
            'dnt'               :   '1',
            'pragma'            :   'no-cache',
            #'referer'           :   "https://twitter.com/i/search/timeline?f=videos&vertical=default&q=near%3A%22India%22%20since%3A2018-06-10%20until%3A2018-06-11&src=typd",
            'referer'           :   "https://twitter.com/i/search/timeline?%s"%params_encoded,
            'user-agent'        :   "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36",
            'x-requested-with'  :   'XMLHttpRequest',
        }
       
        params['max_position'] = max_position
        res = requests.get(url = url, headers= headers, params=params)

        if res.status_code == 200:
            # Load & parse the extracted part of json 
            content = json.loads(res.text)
            min_position = content['min_position']
            print(min_position)
            
            parsed_soup = BeautifulSoup(content['items_html'], 'html.parser')
            li_tags = parsed_soup.find_all('li', attrs={"data-item-type" : "tweet"})
            print("Total # of tweets found: ", len(li_tags))
            
            # Extract all the tweet ids and save them in a list
            tweets = {}
            for li_tag in li_tags:
                tweet_id = li_tag.get('data-item-id')
                tweets[tweet_id] =  li_tag.get('data-screen-name')
            return tweets, min_position
        else:
            return 
            
    
    
    def main(self, query ):
        # Get guest ID & authorization value
        session = requests.Session()

        twitter_url  = "http://mobile.twitter.com/" 
        agent_header = {
            'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36'
        }
        
        # Get guest id & authorization token 
        req  = session.get(twitter_url, headers = agent_header)
        cookies = session.cookies.get_dict() 
        guest_id  = str(cookies['guest_id'])
        #print('guest_id:    ', guest_id)
        print("Recieved guest id")

        auth_token = self.auth_token(session)
        
        # Start scrolling through the twitter timeline pages
        max_position  = 'TWEET--'
        while True:
            tweets,  min_position = self.get_tweet_ids(query, max_position= max_position)
            
            # Old min_position becomes the new max-position
            if len(tweets):
                self.store_data(tweets, guest_id, auth_token, session,  "twitter", "tweet" )
                max_position = min_position
            else:
                return 0

    def store_data(self, tweets, guest_id, auth_token, session, index="index", data_type="type"):
            elastic = Elastic()
            for tweet_id in tweets:
                try:
                    tweet_json = self.tweet_details(tweet_id, tweets[tweet_id], guest_id, auth_token, session)  
                    
                    # Check if it contains a video or not 
                    try:
                        video_info = tweet_json["extended_entities"]["media"][0]["video_info"]
                    except KeyError as e:
                        print(tweet_id, ":  No video found ", )
                        continue
                        
                    user_json  = tweet_json.pop('user')      # separate user and tweet details
                    elastic.store_user_data(user_json, index, data_type)
                    elastic.store_tweet(tweet_json, index, data_type)
                    time.sleep(random.random()*1)
                    
                except Exception as e:
                    print("tweet %s:  Error in storing data. %s"%(tweet_id, e))
                    print(traceback.format_exc())



                    
    
    
    
    def auth_token(self, session):
        # Get Authorization key
        js_url  =   "https://abs-0.twimg.com/responsive-web/web/ltr/main.34a2f743d9af467d.js"
        js_headers = {
            'User-Agent'    :   'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36',
            'Referer'       :   'https://mobile.twitter.com/sw.js',
            'Origin'        :   'https://mobile.twitter.com'
        }     
        req_js = session.get(js_url, headers=js_headers)
        
        if req_js.status_code == 200:
            auth_str = re.search(r'BEARER_TOKEN:".*?"', req_js.text).group(0) 
            auth_token = auth_str.lstrip("BEARER_TOKEN:").strip('"')
            #print('auth_token:  ', auth_token)
            return auth_token
        else:
            print("No Authorization Token Found")
            return 

    
    

    def tweet_details(self, tweet_id, user_id, guest_id, auth_token, session):
        url = "https://api.twitter.com/1.1/statuses/show.json"
        
        querystring  =  {
            "include_profile_interstitial_type":"1",
            "include_blocking":"1",
            "include_blocked_by":"1",
            "include_followed_by":"1",
            "include_want_retweets":"1",
            "include_mute_edge":"1",
            "include_can_dm":"1",
            "skip_status":"1",
            "cards_platform":"Web-12",
            "include_cards":"1",
            "include_ext_alt_text":"true",
            "include_reply_count":"1",
            "tweet_mode":"extended",
            "trim_user":"false",
            "include_ext_media_color":"true",
            "id":tweet_id
        }

        headers = {
            'authority' :    "api.twitter.com",
            'method'    :    "GET",
            'path'      :    "/1.1/statuses/show.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_reply_count=1&tweet_mode=extended&trim_user=false&include_ext_media_color=true&id=%s"%tweet_id,
            'scheme'    :    "https",
            'accept'    :    "*/*",
            'accept-encoding'   :    "gzip, deflate, br",
            'accept-language'   :    "en-US,en;q=0.9",
            'authorization'     :    "Bearer %s"%auth_token,
            'dnt'               :    "1",
            'origin'            :    "https://mobile.twitter.com",
            'referer'           :    "https://mobile.twitter.com/%s/status/%s/video/1"%(user_id, tweet_id),
            'user-agent'        :    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36",
            'x-guest-token'     :     guest_id,
            'x-twitter-active-user' :    "yes",
            'x-twitter-client-language' : "en",
            'Cache-Control' :    "no-cache",
            }

        req = session.get(url, headers=headers, params=querystring)

        if req.status_code == 200:
            return(json.loads(req.text))        # Returning the tweet in json format     
             


        
if __name__ == "__main__":
    ts = Twitter_Scraper()
    ts.main()
