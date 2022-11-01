

import tweepy
import spacy
from fuzzywuzzy import fuzz
from datetime import datetime, timezone
import re

from sqlalchemy import create_engine
import requests
import json
import pandas as pd
import time

sp = spacy.load('en_core_web_sm')

API_KEY=None
API_KEY_SECRET=None
ACCESS_KEY=None
ACCESS_KEY_SECRET=None

auth = tweepy.OAuthHandler(API_KEY,API_KEY_SECRET)
auth.set_access_token(ACCESS_KEY, ACCESS_KEY_SECRET)
customer_handle="indxbus"


# Create API object
api = tweepy.API(auth)
import random
# Init Db

engine = create_engine('sqlite:///sl_social.db', echo=False)
sqlite_connection = engine.connect()



# Filter out RT - Done
# Filter only in response to self - Done
# Filter out SPAM, Etc, less Done
# Filter out containing a CaseID or inquiring already >>> Follow Up Case Comment (pipe) >>
# Filter out New relevant Cases >>> Follow Up with New Case creation
# Identify / List of Spam Users / Past Conv 


USERPROPS=['contributors_enabled', 'created_at', 'default_profile', 'default_profile_image', 'description', 'entities',
        'favourites_count', 'follow', 'follow_request_sent', 'follower_ids', 'followers', 'followers_count', 
        'following', 'friends', 'friends_count', 'geo_enabled', 'has_extended_profile', 'id', 'id_str', 
        'is_translation_enabled', 'is_translator', 'lang','list_memberships','list_ownerships','list_subscriptions',
        'listed_count', 'lists', 'location', 'name','notifications','parse','parse_list','profile_background_color',
        'profile_background_image_url','profile_background_image_url_https','profile_background_tile',
        'profile_banner_url','profile_image_url','profile_image_url_https','profile_link_color',
        'profile_sidebar_border_color','profile_sidebar_fill_color','profile_text_color','profile_use_background_image',
        'protected', 'screen_name', 'statuses_count', 'time_zone', 'timeline', 'translator_type', 'unfollow', 'url', 
        'utc_offset', 'verified', 'withheld_in_countries']


TWEETFIELDS=['author', 'contributors', 'coordinates', 'created_at', 'destroy', 'entities', 'favorite', 
              'favorite_count', 'favorited', 'geo', 'id', 'id_str', 'in_reply_to_screen_name','in_reply_to_status_id', 
              'in_reply_to_status_id_str', 'in_reply_to_user_id', 'in_reply_to_user_id_str', 'is_quote_status', 
              'lang', 'parse', 'parse_list', 'place','retweet', 'retweet_count', 'retweeted', 'retweets', 
              'source', 'source_url', 'text', 'truncated', 'user']

spam="elon musk bitcoin xxx vote crypto coin cz binance amazon gift cards conference asap"
praise="great awesome nice appreciate like good wonderful"
case="""fuck bullshit disgust hate bad worse fix suck difficult hard never complaint annoy product working issues asap
item purchase disgust disappointed help computer mobile case agent unable days sad malfunction pathetic laptop ceo frustrated"""


def get_original_tweets_id(lookback_posts=20):
    res=api.user_timeline(count=lookback_posts)
    self_orginal_tweets_id=[]
    for p in res:
        if (not(p.retweeted)) and (not(p.in_reply_to_status_id)):
            self_orginal_tweets_id.append(str(p.id))

    return self_orginal_tweets_id   


        
def get_tweet_replies(tweet_id:str,self_name:str = 'indxbus'):
    replies=[]
    for tweet in tweepy.Cursor(api.search_tweets,q='to:'+self_name, result_type='recent').items(1000):
        if hasattr(tweet, 'in_reply_to_status_id_str'):
            if (tweet.in_reply_to_status_id_str==tweet_id):
                replies.append(tweet)
      
    parsed=[]
    for reply in replies:
        parsed.append({
              "name":reply.user.screen_name,
              "profile_name":reply.user.name,
              "user_id":reply.user.id,
              "text":reply.text.replace('\n', ' '),
              "reply_id":reply.id_str,
              "created_at":reply.created_at,
              "followers":reply.user.followers_count,
              "following":reply.user.friends_count,
              "location":reply.user.location,
              "photo_url":reply.user.profile_image_url,
              "website":reply.user.url,
              "time_zone":reply.user.time_zone})
    
    return parsed



def return_clean_text(text):
    text=text.replace(f"@{customer_handle}","").strip()
    new_text= " ".join([token.text for token in sp(text) if not token.is_punct and not token.is_stop])
    return new_text


def classify_text(text="never going to buy bitcoin"):
    if len(text.split(" "))<3:
        return "useless"
    
    matches = {}
    categories={"spam":spam,"praise":praise,"case":case}
    for key,category in categories.items():
        score=max(fuzz.ratio(category, text),fuzz.partial_ratio(category, text),
            fuzz.token_sort_ratio(category, text),fuzz.token_set_ratio(category,text))
        matches[key]=score
        
    return list(categories.keys())[list(matches.values()).index(max(matches.values()))]

def user_escalation_impact(user_object):
    score=int(user_object["followers"]>10)+\
    int(user_object["following"]>10)+\
    int((datetime.now(timezone.utc)-user_object["created_at"]).days>1)
    return score


def extract_case_number(txt):
    case_id = re.compile(r"\bc\w+[0-9]",re.IGNORECASE).findall(txt)
    case_string=re.compile(r"\bcas\w+",re.IGNORECASE).findall(txt)
    if len(case_string)>0 and len(case_id)==0:
        return '123'
    try:
        case_id=case_id[0].lower().replace("cas","")[:5]
    except:
        case_id=0
    return case_id

def escalate_case(case_id):
    res=requests.get(f"https://supportlogic-social.glitch.me/cases/{case_id}")
    payload=json.loads(res.text)
    payload["sl_priority"]="High"
    payload["sl_sentiment"]="Negative"
    payload["sl_sentiment_text"]="Customer waiting"
    res=requests.put(f"https://supportlogic-social.glitch.me/cases/{int(case_id)}",payload)
    print("case_escalated")
    return None

def process_case_comment(case_comment_object):
    
    if len(str(case_comment_object['id']))!=5:
        error_case_format=True
        msg=f"""Hi @{case_comment_object['name']}!thank you for reaching out to us. 
        Looks like there is an issue with the 8 character case number. 
        Can you verify and resend please? - Tim"""
        
    else:
        case_id=int(case_comment_object["id"])
        res=requests.get(f"https://supportlogic-social.glitch.me/cases/{case_id}")
        agent_name=json.loads(res.text).get('sl_agent','Ben')
        
        msg= f"""Hi @{case_comment_object['name']}! Your case is currently processing, Agent {agent_name} will connect soon !
        Find details of case here - www.supportlogic.com/case_updates/{case_id}"""
        
    
    res=api.update_status(msg,in_reply_to_status_id=case_comment_object['reply_id'])
    print(f"responded to {case_comment_object['name']}")
    return res


def create_new_case(case_comment_object):
    msg=f"""Hi @{case_comment_object['sl_account_name']}! Sorry to hear about your experience. 
    I have created a support ticket CAS{case_comment_object['id']} to resolve this. 
    Please quote this reference in future conversations.
    
    You can share additional information on www.company.com/ticketform
    - {case_comment_object['sl_agent']}"""
    
    res=api.update_status(msg,in_reply_to_status_id=case_comment_object['reply_id'])
    print(f"responded to {case_comment_object['sl_account_name']}")
    return res
    


def case_to_json(reply_object):
    reply=reply_object
    priority=random.choice(["Low","Moderate"])
    agent_name = random.choice(["Tim", "Alex", "Geeta"])
    json_data={
      'sl_account_name': reply["name"],
      'id': random.randint(10000,99999),
      'sl_case_id': random.randint(10000,99999),
      'sl_status': 'Open',
      'sl_priority': priority,
      'sl_sentiment': "needAttention",
      'sl_sentiment_text': "Production Issue",
      'sl_subject': reply['text'].replace("@indxbus","").strip(),
      'sl_agent':agent_name,
      'sl_created_at':str(reply['created_at']),
      'photo_url':reply['photo_url'],
      'sl_requester_name':reply['profile_name'],
      'sl_source':'twitter',
      'followers':reply['followers'],
      'following':reply['following']
    }
    return json_data



#Run every minute for day
sleep_secs=60
for minute in range(1,1440): 
        tweets_replies_dict={}
        tweet_ids=get_original_tweets_id(lookback_posts=20)
        for tid in tweet_ids:
            reply_metadata=get_tweet_replies(tweet_id=tid)
            if len(reply_metadata)!=0:
                tweets_replies_dict[tid]=reply_metadata


        if not tweets_replies_dict:
            print("No new tweet found")

        else:
            for tweet in tweets_replies_dict:
                replies=tweets_replies_dict[tweet]
                filtered_replies=[]
                for reply in replies:
                    reply_id=reply['reply_id']
                    try:
                        case_number=int(extract_case_number(reply.get('text')))
                    except Exception as e:
                        print(e)
                    try:
                        exists=pd.read_sql_query(f"Select reply_id from tweet_replies_trace where reply_id = '{reply_id}'",sqlite_connection)
                    except:
                        exists=[]
                    if len(exists)==0:

                        if bool(case_number):
                            reply["category"]="case_comment"
                            reply["id"] = case_number         
                        else:                             
                            text=return_clean_text(reply.get('text'))
                            category=classify_text(text)
                            reply["category"]=category
                        reply["user_impact"]=user_escalation_impact(reply)

                        if (reply["category"] in ['case','case_comment']):
                            filtered_replies.append(reply)
                    else:
                        print("skipping ",reply['reply_id'])

                if len(filtered_replies)>0:
                    tweetreplies = pd.json_normalize(filtered_replies) 
                    tweetreplies['conversation_id']=tweet
                    try:
                        tweetreplies=tweetreplies.drop(columns='id')
                    except:
                        pass
                    tweetreplies.to_sql("tweet_replies_trace",sqlite_connection, if_exists="append")

                    for reply in filtered_replies:
                        try:
                            if reply['category']=='case':
                                payload=case_to_json(reply)
                                requests.post("https://supportlogic-social.glitch.me/cases",payload)
                                reply['sl_account_name']=payload['sl_account_name']
                                reply['id']=payload['id']
                                reply['sl_agent']=payload['sl_agent']
                                create_new_case(reply)

                            elif reply['category']=='case_comment':
                                case_id=case_number
                                escalate_case(reply['id'])
                                process_case_comment(reply)
                        except Exception as e:
                            print (e)
                            print("skipped")

                            #DEMO
                            #Create Case
                            #Wrong Case nUmber
                            #Correct Case number > Escalate
                            #spam
    time.sleep(sleep_secs)

#sentiment = sp("This is sample")._.blob.polarity
#sentiment = round(sentiment,2)
