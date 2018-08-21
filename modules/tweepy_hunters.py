# -*- coding: utf-8 -*-
"""
this module contains the class Tweepy_hunters, a collection of methods that
interact directly with the twitter_api; the api object in charge for bridging
the data between the process and the twitter api is provided on an upper level;
most of the methods here are instanciated as thread

Created on Sat Jan 17 19:27:42 2015

Twitter_API Docs: get followers
https://dev.twitter.com/rest/reference/get/users/lookup
@author: giotto
"""
import tweepy
from traceback import format_exc
from modules.utils import Utils
from threading import currentThread
import random
from modules.mongo_inserter import Mongo_inserter
from modules.mongo_extractor import Mongo_extractor
from datetime import datetime


class Tweepy_hunters(Mongo_inserter, Mongo_extractor, Utils):
    '''methods dealing directly from the twitter_api and; all methods hereby
    listed need an authentication tweepy api object as argument; most of the
    hunters here work within a multithreading environment; note how
    next(api_cycler) when used provides a new/successive api object from an
    api_pool '''

    def tweets_hunter(self, *args, **kwargs):
        '''it downloads twitter statutes containing specific key words;
        firstly each status is evaluated as retweet or original tweet,
        in the first case the retweet contains its original status as attribute
        which will be taken into consideration; only original tweets are
        stored, but only if not already present in the DB (if already present
        the tweet details will be updated); authors of both original tweets and
        retweets are stored as nodes but only if not already present in the DB
        (if already present their details will be simply updated); if a
        retweeted  status is found an edge is also stored, linking the author
        of the original status and the author of the retweet; most (not all) of
        these methods are built to fit into a multithreading environment the
        twitter api method used is described here:
        https://dev.twitter.com/rest/reference/get/search/tweets'''

        try:
            self.lock.acquire()

            tweepy_api = args[0]
            lang_parameters = args[1]
            # search_term here must be a string
            search_terms = args[2]

            raw_status_ids = set()
            raw_tweets_ids = set()
            raw_retweets_ids = set()

            # https://dev.twitter.com/rest/reference/get/search/tweets
            # https://dev.twitter.com/rest/public/search
            if lang_parameters:
                tweepy_cursor = tweepy.Cursor(tweepy_api.search,
                                              q=search_terms,
                                              include_entities=True,
                                              # result_type="popular",
                                              lang=lang_parameters,
                                              ).pages()
            else:
                tweepy_cursor = tweepy.Cursor(tweepy_api.search,
                                              q=search_terms,
                                              include_entities=True,
                                              ).pages()
            self.logger.info('searching tweets for: ' + str(search_terms))
            while True:
                try:
                    tweets_page = next(tweepy_cursor)
                    for tweet in tweets_page:
                        raw_status_ids.add(tweet.id)
                        if hasattr(tweet, 'retweeted_status'):
                            raw_retweets_ids.add(tweet.id)
                            if hasattr(self, '_verbose'):
                                self.logger.info('retweets found: ' + str(
                                    len(raw_retweets_ids)) +
                                    "; total statuses found: " +
                                    str(len(raw_status_ids)))
                            # in case that tweepy_api.retweets does not find
                            # this particular user
                            self.mongo_node_inserter(
                                tweet.retweeted_status.user, search_terms,
                                tweet.retweeted_status.created_at)
                            self.mongo_node_inserter(
                                tweet.user, search_terms, tweet.created_at)
                            self.mongo_retweeters_hunt_edges_inserter(
                                tweet.retweeted_status.user.id, tweet.user.id,
                                tweet.retweeted_status.id, search_terms)
                            self.mongo_tweet_inserter(
                                tweet.retweeted_status, search_terms)

                        else:
                            raw_tweets_ids.add(tweet.id)
                            if hasattr(self, '_verbose'):
                                self.logger.info('tweets found: ' + str(
                                    len(raw_tweets_ids)) +
                                    "; total statuses found: " +
                                    str(len(raw_status_ids)))

                            self.mongo_node_inserter(
                                tweet.user, search_terms, tweet.created_at)
                            self.mongo_tweet_inserter(tweet, search_terms)

                except StopIteration:
                    self.logger.info(
                        "Ain't no more tweets to download for " +
                        str(search_terms))
                    self.lock.release()
                    break
                except tweepy.TweepError as e:
                    if self.tweepy_error_boolean(e):
                        continue
                    else:
                        break
        except Exception:
            self.logger.error(format_exc())
            self.disconnector()

    def timeline_hunter(self, *args, **kwargs):
        try:

            self.lock.acquire()
            tweepy_api = args[0]
            target_user_screen_name = args[1]
            # search_term here must be a string
            search_terms = target_user_screen_name + '_timeline'
            raw_status_ids = set()
            raw_tweets_ids = set()
            raw_retweets_ids = set()
            # https://dev.twitter.com/rest/reference/get/statuses/user_timeline
            tweepy_cursor = tweepy.Cursor(tweepy_api.user_timeline,
                                          include_rts=True,
                                          screen_name=target_user_screen_name,
                                          count=200,
                                          ).pages()

            self.logger.info('searching timeline for: ' +
                             str(target_user_screen_name))
            while True:
                try:
                    tweets_page = next(tweepy_cursor)
                    for tweet in tweets_page:
                        raw_status_ids.add(tweet.id)
                        if hasattr(tweet, 'retweeted_status'):
                            raw_retweets_ids.add(tweet.id)
                            if hasattr(self, '_verbose'):
                                self.logger.info('retweets found: ' + str(
                                    len(raw_retweets_ids)) +
                                    "; total statuses found: " +
                                    str(len(raw_status_ids)))
                            # in case that tweepy_api.retweets does not find
                            # this particular user
                            self.mongo_node_inserter(
                                tweet.retweeted_status.user, search_terms,
                                tweet.retweeted_status.created_at)
                            self.mongo_node_inserter(
                                tweet.user, search_terms, tweet.created_at)
                            self.mongo_retweeters_hunt_edges_inserter(
                                tweet.retweeted_status.user.id, tweet.user.id,
                                tweet.id)
                            self.mongo_tweet_inserter(
                                tweet.retweeted_status, search_terms)
                        else:
                            raw_tweets_ids.add(tweet.id)
                            if hasattr(self, '_verbose'):
                                self.logger.info('tweets found: ' + str(
                                    len(raw_tweets_ids)) +
                                    "; total statuses found: " +
                                    str(len(raw_status_ids)))

                            self.mongo_node_inserter(
                                tweet.user, search_terms, tweet.created_at)
                            self.mongo_tweet_inserter(tweet, search_terms)

                except StopIteration:
                    self.logger.info(
                        "Ain't no more tweets to download for " +
                        str(search_terms))
                    self.lock.release()
                    break
                except tweepy.TweepError as e:
                    if self.tweepy_error_boolean(e):
                        continue
                    else:
                        break

        except Exception:
            self.logger.error(format_exc())
            self.disconnector()

    def retweeters_hunter(self, *args, **kwargs):
        '''for each tweet stored it finds up to 100 twitter user who
        re-posted(retweeted) it, saving the users (and their details) as nodes
        and creating an eventual edge linking the "retweeters" and the author
        of the original tweet the twitter api method used is described here:
        https://dev.twitter.com/rest/reference/get/statuses/retweets/%3Aid
        an alternative would be tweepy_api.retweeters(status.id)
        but retrieves just 100 most recent retweets'''
        try:
            self.lock.acquire()
            tweepy_api = args[0]
            # target_search_terms here must be a list
            while True:
                target_tweet = self.mongo_retweeters_hunt_target_finder()
                if target_tweet:

                    tweet_id = target_tweet['_id']
                    author_id = target_tweet['author_id']
                    search_terms = target_tweet['search_terms']

                    try:

                        retweets = tweepy_api.retweets(tweet_id, count=200)

                        if retweets:
                            for retweet in retweets:
                                self.mongo_node_inserter(
                                    retweet.user, search_terms,
                                    retweet.created_at)
                                self.mongo_retweeters_hunt_edges_inserter(
                                    author_id, retweet.user.id, tweet_id)
                            self.mongo_retweeters_hunted_tweet_updater(
                                tweet_id)
                        # normally it's triggered when retweet_count=1 or 2 but
                        # retweeters profiles are not retrieveable
                        else:
                            self.mongo_retweeters_hunted_tweet_updater(
                                tweet_id)

                    except tweepy.TweepError as e:
                        self.tweepy_error_boolean(e, tweet_id=tweet_id)
                else:
                    self.lock.release()
                    break
        except Exception:
            self.logger.error(format_exc())
            self.disconnector()

    def mentioned_hunter(self, *args, **kwargs):
        try:
            api_cycler = args[0]
            # target_search_terms here must be a list
            while True:
                target_tweet = self.mongo_mentioned_hunt_target_finder()
                if target_tweet:
                    tweet_id = target_tweet['_id']
                    user_mentioner_id = target_tweet['author_id']
                    users_mentioned = target_tweet['mentions']
                    tweet_search_terms = target_tweet['search_terms']
                    tweet_created_at = target_tweet['created_at']
                    for mentioned in users_mentioned:
                        try:
                            # https://dev.twitter.com/rest/reference/get/users/show
                            mentioned_user = next(
                                api_cycler).get_user(mentioned)
                            self.mongo_node_inserter(
                                mentioned_user, tweet_search_terms,
                                tweet_created_at)
                            self.mongo_mentions_hunt_edges_inserter(
                                user_mentioner_id, mentioned_user.id, tweet_id)
                        except tweepy.TweepError as e:
                            if self.tweepy_error_boolean(e):
                                continue
                            else:
                                break
                    self.mongo_mentions_hunted_tweet_updater(tweet_id)
                else:
                    break
        except Exception:
            self.logger.error(format_exc())
            self.disconnector()

    def followers_id_hunter(self, *args, **kwargs):
        '''twitter users are targeted and its followers ids are retreived;
        only edges are created, linking the ids of users in a followed-follower
        relationship; target users(input) are extracted from a control table
        the twitter api method used is described here:
        https://dev.twitter.com/rest/reference/get/followers/ids'''
        try:
            self.lock.acquire()
            thread_name = currentThread().getName()
            tweepy_api = args[0]
            search_terms = args[1]
            while True:

                target_user = self.mongo_followers_id_hunt_target_finder(
                    search_terms)

                if target_user:
                    target_user_id = target_user['_id']
                    self.mongo_control_followers_locker(
                        target_user_id, 'lock', thread_name)
                    self.process_targeted_users_count += 1
                    tweepy_cursor = tweepy.Cursor(
                        tweepy_api.followers_ids, user_id=target_user_id,
                        count=5000).pages()
                    self.logger.info(thread_name + ' starting with: ' +
                                     str(target_user_id) + ' ' +
                                     str(target_user['screen_name']))
                    foll_ids_count = 0
                    while True:
                        try:
                            followers_page = next(tweepy_cursor)
                            self.process_foll_ids_count += len(followers_page)
                            foll_ids_count += len(followers_page)
                            self.mongo_followers_id_inserter(
                                target_user_id, followers_page)
                            if self.process_foll_ids_count % 10000 == 0:
                                self.logger.debug(
                                    thread_name + '_foll_ids_count : ' +
                                    str(self.process_foll_ids_count))
                            # we don't want to get stuck on huge fishes
                            if self.limited:
                                if foll_ids_count == 100000:
                                    raise StopIteration
                        except StopIteration:
                            self.mongo_followers_id_hunted_updater(
                                target_user_id)
                            self.mongo_control_followers_locker(
                                target_user_id, 'unlock', thread_name)
                            self.logger.info(thread_name + " done with: " +
                                             str(target_user_id) + ' ' +
                                             str(target_user['screen_name']) +
                                             ' process_targeted_users_count: '
                                             + str(self.
                                                   process_targeted_users_count
                                                   ))
                            break
                        except tweepy.TweepError as e:
                            if self.\
                                tweepy_error_boolean(e,
                                                     node_id=target_user_id):
                                continue
                            else:
                                self.mongo_control_followers_locker(
                                    target_user_id, 'unlock', thread_name)
                                break

                else:
                    # release is here because we want this thread to stop
                    # existing when there will be no more targets
                    self.lock.release()
                    break

        except Exception:
            self.logger.error(format_exc())
            self.disconnector()

    def followers_list_hunter(self, *args, **kwargs):
        '''twitter users are targeted and its followers (with their details)
        are retreived; followers found are saved as nodes and edges are
        created, linking the users in a followed-follower relationship; target
        users(input) are extracted from a control table if not explicitly
        passed as kwargs argument the twitter api method used is described
        here: https://dev.twitter.com/rest/reference/get/followers/list'''

        try:
            count_target_user = 0
            self.lock.acquire()

            thread_name = currentThread().getName()
            tweepy_api = args[0]
            search_terms = args[1]
            while True:
                if kwargs:
                    target_user = kwargs
                    # we need only one spin if a target is explicitally given
                    if count_target_user == 1:
                        break
                else:
                    target_user = self.mongo_followers_list_hunt_target_finder(
                        search_terms)

                if target_user:
                    target_user_id = target_user['_id']
                    self.mongo_control_followers_locker(
                        target_user_id, 'lock', thread_name)
                    self.mongo_control_followers_list_locker(
                        target_user_id, 'lock', thread_name)
                    target_user_screen_name = target_user['screen_name']

                    count_target_user += 1
                    # https://dev.twitter.com/rest/reference/get/followers/list
                    tweepy_cursor = tweepy.Cursor(
                        tweepy_api.followers,
                        screen_name=target_user_screen_name,
                        count=5000).pages()
                    self.logger.info(
                        thread_name + ' starting with: ' +
                        str(target_user_screen_name))
                    follower_page_count = 0
                    foll_count = 0
                    while True:
                        try:
                            follower_page = next(tweepy_cursor)
                            follower_page_count += 1
                            for follower in follower_page:

                                self.mongo_node_inserter(
                                    follower, search_terms,
                                    datetime(9999, 9, 9))
                                # mongo_followers_id_inserter is supposed to
                                # insert a collection of ids
                                self.mongo_followers_id_inserter(
                                    target_user_id, [follower.id, ])
                                foll_count += 1
                                if hasattr(self, '_verbose'):
                                    self.logger.info(
                                        str(foll_count) +
                                        ' followers found for: ' +
                                        str(target_user_screen_name))
                                if self.limited:
                                    if foll_count == 50000:
                                        raise StopIteration
                        except StopIteration:
                            self.logger.info(
                                thread_name + " done with: " +
                                str(target_user_screen_name))

                            self.mongo_followers_id_hunted_updater(
                                target_user_id)
                            self.mongo_followers_list_hunted_updater(
                                target_user_id)
                            self.mongo_control_followers_locker(
                                target_user_id, 'unlock', thread_name)
                            self.mongo_control_followers_list_locker(
                                target_user_id, 'unlock', thread_name)

                            break
                        except tweepy.TweepError as e:
                            if self.tweepy_error_boolean(e,
                                                         node_id=target_user_id
                                                         ):
                                continue
                            else:
                                self.mongo_control_followers_locker(
                                    target_user_id, 'unlock', thread_name)
                                self.mongo_control_followers_list_locker(
                                    target_user_id, 'unlock', thread_name)

                                break
                else:

                    self.lock.release()
                    break
        except Exception:
            self.logger.error(format_exc())
            self.disconnector()

    def trends_hunter(self, *args, **kwargs):
        '''in the moment this method is called, all the trends in the world are
        stored;number 1 trend of the moment is stored for each geographical
        location available, and separately the top 10 trends of each
        geographical location the 2 twitter api methods used are described
        here: https://dev.twitter.com/rest/reference/get/trends/available
        https://dev.twitter.com/rest/reference/get/trends/place'''

        try:
            self.lock.acquire()

            api_cycler = args[0]
            trending_countries = (next(api_cycler)).trends_available()

            for location in trending_countries:
                woeid = location['woeid']
                if location['name'] == location['country']:
                    country = (location['country']).encode('utf-8')
                    region = None
                elif location['name'] == 'Worldwide':
                    country = 'Worldwide'
                    region = None
                else:
                    country = (location['country']).encode('utf-8')
                    region = (location['name']).encode('utf-8')
                try:
                    trends = (next(api_cycler)).trends_place(woeid)
                    for index, trend in enumerate(trends[0]['trends']):
                        self.mongo_trend_inserter(
                            country, region, (trend['name']).encode('utf-8'),
                            index + 1, woeid)
                    top_trend = (trends[0]['trends'][0][
                                 'name']).encode('utf-8')
                    self.mongo_top_trend_inserter(
                        country, region, top_trend, woeid)
                    self.logger.info(
                        str(country) + ' ' + str(region) + ' ' + str(top_trend)
                        + ' ' + str(woeid))
                except tweepy.TweepError as e:
                    if self.tweepy_error_boolean(e):
                        continue
                    else:
                        break
        except Exception:
            self.logger.error(format_exc())
            self.disconnector()

    def single_user_finder(self, *args, **kwargs):
        '''this method needs a twitter user name as an input; it returns the
        users id,name and followers_count the twitter api method used is
        described here:https://dev.twitter.com/rest/reference/get/users/show'''
        try:
            api = args[0]
            target_user = args[1]
            user = api.get_user(target_user)
            return user
        except Exception:
            self.logger.error(format_exc())
            self.disconnector()

    def zombi_twitter_account(self, *args, **kwargs):
        '''it finds the world top trend of the moment and random tweets related
        to it, it therefore posts the tweets on our account the twitter api
        method used is described here:
        https://dev.twitter.com/rest/reference/get/search/tweets'''

        try:
            random_api_name = random.sample(self.apis, 1)[0]
            random_api = self.apis[random_api_name]

            trends = random_api.trends_place(1)
            global_trends = ""
            for index, trend in enumerate(trends[0]['trends']):
                trend = trend['name'].encode('utf-8')
                if ' ' not in trend:  # single word trend only
                    if index == 0:
                        global_trends = trend
            tweepy_cursor = tweepy.Cursor(random_api.search,
                                          q=global_trends,
                                          include_entities=True,
                                          ).items()
            random_tweet = next(tweepy_cursor)
            random_tweet = str(random_tweet.text.encode('utf-8'))
            if random_tweet.startswith('RT'):
                random_tweet = random_tweet[3:]
            self.logger.debug(random_tweet)
            random_api.update_status(status=random_tweet)

        except Exception:
            self.logger.error(format_exc())
            self.disconnector()
