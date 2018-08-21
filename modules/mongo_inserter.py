# -*- coding: utf-8 -*-
"""
This Module is in charge for bridging the Twitter data into the Mongo Databas;
the class Mongo_inserter relies on a previously set environment in order to
work and it is obviously meant to be inherited.

Created on Fri Sep  4 16:42:07 2015

@author: giotto
"""
from traceback import format_exc
import datetime
from pymongo.errors import DuplicateKeyError


class Mongo_inserter(object):
    '''a collection of methods in charge of any data insertion into our mongodb
    database'''

    def mongo_node_inserter(self, *args, **kwargs):
        '''This method inserts or eventually updates a twitter_user aka a node
        into the nodes_collection; nodes are associated with one or more
        search_term which are at the base of the retrieving process from the
        Twitter API and in a second step from the Database. Each Search_term is
        associated with a singular date, which in most of cases represent the
        creation date of the oldest Tweet/status that has been found through
        this search term and that is responsible for finding this User/Node.'''
        try:
            user = args[0]
            try:
                search_terms = args[1]
                status_created_at = args[2]
                if not isinstance(search_terms, list):
                    search_terms = [search_terms, ]

                self.mongo_nodes_collection.\
                    update_one({'_id': user.id},
                               {'$set':
                                {'screen_name': user.screen_name,
                                 'created_at': user.created_at,
                                 'tweets_count': user.statuses_count,
                                 'followers_count': user.followers_count,
                                 'friends_count': user.friends_count,
                                 'listed_count': user.listed_count,
                                 'profile_language':
                                 (user.lang).encode('utf-8'),
                                 'profile_image_url':
                                 (user.profile_image_url).encode('utf-8'),
                                 'geo_enabled': user.geo_enabled,
                                 'protected': user.protected,
                                 'verified': user.verified, },
                                "$currentDate":
                                {"update_time": True},
                                '$addToSet':
                                {'hunter': self._hunt_name, }}, upsert=True)
                if user.name:
                    user.name = user.name.replace("'", "`")
                    self.mongo_nodes_collection.update_one(
                        {'_id': user.id},
                        {'$set':
                         {'name': (user.name).encode('utf-8'), }})
                if user.description:
                    user.description = (user.description).replace("'", "`")
                    self.mongo_nodes_collection.update_one(
                        {'_id': user.id},
                        {'$set':
                         {'description': (user.description).encode('utf-8'),
                          }})
                if user.location:
                    user.location = (user.location).replace("'", "`")
                    self.mongo_nodes_collection.update_one(
                        {'_id': user.id},
                        {'$set':
                         {'location': (user.location).encode('utf-8'), }})
                if user.profile_background_image_url:
                    self.mongo_nodes_collection.\
                        update_one({'_id': user.id},
                                   {'$set': {
                                       'profile_background_image_url':
                                       (user.profile_background_image_url).
                                       encode('utf-8'), }})
                if user.url:
                    user.location = (user.location).replace("'", "`")
                    self.mongo_nodes_collection.update_one(
                        {'_id': user.id},
                        {'$set':
                         {'personal_homepage':
                          (user.url).encode('utf-8'), }})

                for single_search_term in search_terms:
                    search_terms_field = 'search_terms.' + \
                        str((single_search_term.lower()).replace('$','').encode('utf-8'))
                    self.mongo_nodes_collection.\
                        update_one({'_id': user.id},
                                   {'$push':
                                    {search_terms_field:
                                     {'$each': [status_created_at, ],
                                      '$sort': 1,
                                      '$slice': 1, }}})

                if hasattr(self, '_verbose'):
                    self.logger.info("twitter_user stored: " + str(user.id))
            except DuplicateKeyError:
                pass
            except Exception:
                self.logger.error(user)
                raise
        except Exception:
            self.logger.error(format_exc())
            self.disconnector()

    def mongo_tweet_inserter(self, *args, **kwargs):
        '''This method inserts or eventually updates a Twitter_status aka Tweet
        into the tweets_collection; tweets are associated with one or more
        search_term which are at the base of the retrieving process from the
        Twitter API and in second step from the Database.'''
        try:
            status = args[0]
            try:
                search_terms = args[1]
                status.text = status.text.replace("'", "`")
                status.source = status.source.replace("'", "`")
                self.mongo_tweets_collection.\
                    update_one({'_id': status.id},
                               {'$set':
                                {'author_id': status.user.id,
                                 'author_name': status.user.screen_name,
                                 'created_at': status.created_at,
                                 'tweet_text': (status.text).encode('utf-8'),
                                 'status_lang': status.lang,
                                 'source': (status.source).encode('utf-8'),
                                 'retweet_count': status.retweet_count, },
                                "$currentDate":
                                {"update_time": True},
                                '$addToSet':
                                {'search_terms': search_terms.lower().replace('$','')}},
                               upsert=True)

                if status.entities['user_mentions']:
                    mentions = [mention['screen_name']
                                for mention in
                                status.entities['user_mentions']]
                    self.mongo_tweets_collection.\
                        update_one({'_id': status.id},
                                   {'$addToSet':
                                    {'mentions':
                                     {'$each': mentions}}})

                if status.entities['hashtags']:
                    hashtags = [hashtag['text']
                                for hashtag in status.entities['hashtags']]
                    self.mongo_tweets_collection.\
                        update_one({'_id': status.id},
                                   {'$addToSet':
                                    {'hashtags':
                                     {'$each': hashtags}}})
                if status.entities['urls']:
                    urls = [url['expanded_url'].replace(
                        "'", "`") for url in status.entities['urls']]
                    self.mongo_tweets_collection.\
                        update_one({'_id': status.id},
                                   {'$addToSet':
                                    {'urls':
                                     {'$each': urls}}})
                if status.coordinates:
                    coordinates = status.coordinates['coordinates']
                    self.mongo_tweets_collection.\
                        update_one({'_id': status.id},
                                   {'$set':
                                    {'coordinates.longitude':
                                     float(coordinates[0]),
                                     'coordinates.latitude':
                                     float(coordinates[1])}})

                if hasattr(self, '_verbose'):
                    self.logger.info("tweet stored: " + str(status.id))
            except Exception:
                self.logger.error(status)
                self.logger.error(format_exc())
                self.disconnector()

        except Exception:
            self.logger.error(format_exc())
            self.disconnector()

    def mongo_followers_id_inserter(self, *args, **kwargs):
        '''This method updates a single twitter_user/node in the
        nodes_collection by appending the twitter_user_id of the twitter_users
        who are considered as "followers" of the targeted node; the function
        operates a bulk insert through "$each"; "$addToSet" makes sure that the
        attribute "followers_id_hunt_edges" cointains a list of unique
        values.'''

        try:
            node_id = args[0]
            followers_id = args[1]  # must be a list because of "$each"
            self.mongo_nodes_collection.update_one({'_id': node_id},
                                                   {'$addToSet':
                                                    {'followers_id_hunt_edges':
                                                     {'$each': followers_id}}})

            if hasattr(self, '_verbose'):
                self.logger.info(
                    "mongo_followers_id_inserter updated: " + str(node_id))
        except Exception:
            self.logger.error(format_exc())
            self.disconnector()

    def mongo_mentions_hunt_edges_inserter(self, *args, **kwargs):
        '''This method updates a single twitter_user/node in the
        nodes_collection by appending the twitter_user_id of the twitter_users
        who has been "mentioned" in a tweet written by the targeted node
        together with the tweet_id of the status where the "mention" took
        place; "$addToSet" makes sure that the attribute "mentions_hunt_edges"
        cointains a list of unique values.'''
        try:
            source = args[0]
            target = args[1]
            tweet_id = args[2]
            self.mongo_nodes_collection.update_one({'_id': source},
                                                   {'$addToSet':
                                                       {'mentions_hunt_edges':
                                                        (target, tweet_id)}})
            if hasattr(self, '_verbose'):
                self.logger.info(
                    "mongo_mentions_hunt_edges_inserter updated: " +
                    str(source))
        except Exception:
            self.logger.error(format_exc())
            self.disconnector()

    def mongo_retweeters_hunt_edges_inserter(self, *args, **kwargs):
        '''This method updates a single twitter_user/node in the
        nodes_collection by appending the twitter_user_id of the twitter_users
        who "retweeted" a tweet written by the targeted node, together with the
        tweet_id of the status that has been retweeted; "$addToSet" makes sure
        that the attribute "retweeters_hunt_edges" cointains a list of unique
        values.'''
        try:
            source = args[0]
            target = args[1]
            tweet_id = args[2]
            self.mongo_nodes_collection.\
                update_one({'_id': source},
                           {'$addToSet':
                            {'retweeters_hunt_edges': (target, tweet_id)}})
            if hasattr(self, '_verbose'):
                self.logger.info(
                    "mongo_retweeters_hunt_edges_inserter updated: " +
                    str(source))
        except Exception:
            self.logger.error(format_exc())
            self.disconnector()

    def mongo_followers_id_hunted_updater(self, *args, **kwargs):
        '''This method updates a single twitter_user/node in the
        nodes_collection by setting the attribute
        "followers_id_hunt_edges_update_time" with the current date_time stamp,
        therefore pointing out when the targeted node has been scanned with the
        followers_id_hunt last.'''
        try:
            node_id = args[0]
            self.mongo_nodes_collection.\
                update_one({'_id': node_id},
                           {'$currentDate':
                            {"followers_id_hunt_edges_update_time": True}})
        except Exception:
            self.logger.error(format_exc())
            self.disconnector()

    def mongo_followers_list_hunted_updater(self, *args, **kwargs):
        '''This method updates a single twitter_user/node in the
        nodes_collection by setting the attribute
        "followers_id_hunt_edges_update_time" with the current date_time stamp,
        therefore pointing out when the targeted node has been scanned with the
        followers_id_hunt last.'''
        try:
            node_id = args[0]
            self.mongo_nodes_collection.\
                update_one({'_id': node_id},
                           {'$currentDate':
                            {"followers_list_hunt_edges_update_time": True}})
        except Exception:
            self.logger.error(format_exc())
            self.disconnector()

    def mongo_retweeters_hunted_tweet_updater(self, *args, **kwargs):
        '''this method updates a single twitter_status/tweet in the
        tweets_collection by setting the attribute "retweeters_hunted" with a
        message (e.g."7 retweeters found"), representing the number of
        retweeters retrieved for this status. Alternatevely the attribute will
        be set as "empty flag" if no "retweeters" has been retrieved. The
        latter case is normally triggered by a change of privacy settings by
        the owner of the tweet.'''
        try:
            tweet_id = args[0]
            self.mongo_tweets_collection.\
                update_one({'_id': tweet_id},
                           {'$currentDate':
                            {"retweeters_hunted": True}})
        except Exception:
            self.logger.error(format_exc())
            self.disconnector()

    def mongo_mentions_hunted_tweet_updater(self, *args, **kwargs):
        '''this method updates a single twitter_status/tweet in the
        tweets_collection by setting the attribute "mentioned_hunted" as "True"
        after searching foreventual users "mentioned" within this tweet.'''
        try:
            tweet_id = args[0]
            self.mongo_tweets_collection.\
                update_one({'_id': tweet_id},
                           {"$currentDate":
                            {"mentioned_hunted": True}})
        except Exception:
            self.logger.error(format_exc())
            self.disconnector()

    def mongo_control_followers_locker(self, *args, **kwargs):
        '''this method updates a single twitter_user/node in the
        nodes_collection by setting or deleting the attribute
        "control_followers_id_lock"; such attributes prevents different thread
        within followers_id_hunt from targeting the same node/twitter_user at
        the same time. Before migrating/changing to mongodb, a "control_table"
        in PostgreSQL used to perform this duty.'''

        try:
            node_id = args[0]
            control = args[1]
            query = {'_id': node_id}
            if len(args) > 2:
                thread_name = args[2]
                if control == 'lock':
                    update = {'$set':
                              {"control_followers_id_lock": thread_name}}
            if control == 'unlock':
                update = {'$unset':
                          {"control_followers_id_lock": ""}}

            self.mongo_nodes_collection.update_one(query, update)
        except Exception:
            self.logger.error(format_exc())
            self.disconnector()

    def mongo_control_followers_list_locker(self, *args, **kwargs):
        '''this method updates a single twitter_user/node in the
        nodes_collection by setting or deleting the attribute
        "control_followers_list_lock"; such attributes prevents different
        thread within followers_list_hunt from targeting the same
        node/twitter_user at the same time. Before migrating/changing to
        mongodb, a "control_table" in PostgreSQL used to perform this duty.'''
        try:
            node_id = args[0]
            control = args[1]
            query = {'_id': node_id}
            if len(args) > 2:
                thread_name = args[2]
                if control == 'lock':
                    update = {'$set':
                              {"control_followers_list_lock": thread_name}}
            if control == 'unlock':
                update = {'$unset':
                          {"control_followers_list_lock": ""}}

            self.mongo_nodes_collection.update_one(query, update)
        except Exception:
            self.logger.error(format_exc())
            self.disconnector()

    def mongo_control_retweeters_locker(self, *args, **kwargs):
        '''this method updates a single tweet in the tweets_collection
        by setting or deleting the attribute "control_retweeters_lock"; such
        attributes prevents different thread within retweeters_hunt from
        targeting the same tweet document at the same time. Before
        migrating/changing to mongodb, a "control_table" in PostgreSQL used to
        perform this duty.'''

        try:
            tweet_id = args[0]
            control = args[1]
            query = {'_id': tweet_id}
            if len(args) > 2:
                thread_name = args[2]
                if control == 'lock':
                    update = {'$set':
                              {"control_retweeters_lock": thread_name}}
            if control == 'unlock':
                update = {'$unset':
                          {"control_retweeters_lock": ""}}

            self.mongo_tweets_collection.update_one(query, update)

        except Exception:
            self.logger.error(format_exc())
            self.disconnector()

    def mongo_control_mentioned_locker(self, *args, **kwargs):
        '''this method updates a single tweet in the tweets_collection
        by setting or deleting the attribute "control_mentioned_lock"; such
        attributes prevents different thread within mentioned_hunt from
        targeting the same tweet document at the same time. Before
        migrating/changing to mongodb, a "control_table" in PostgreSQL used to
        perform this duty.'''

        try:
            tweet_id = args[0]
            control = args[1]
            query = {'_id': tweet_id}
            if len(args) > 2:
                thread_name = args[2]
                if control == 'lock':
                    update = {'$set':
                              {"control_mentioned_lock": thread_name}}
            if control == 'unlock':
                update = {'$unset':
                          {"control_mentioned_lock": ""}}

            self.mongo_tweets_collection.update_one(query, update)

        except Exception:
            self.logger.error(format_exc())
            self.disconnector()

    def mongo_top_trend_inserter(self, *args, **kwargs):
        '''this method insert a new top_trend into the "top_trends_collection".
        Just as easy as that.'''
        try:
            country = args[0]
            region = args[1]
            trend = args[2]
            woeid = args[3]
            self.mongo_top_trends_collection.\
                insert({'country': country,
                        'region': region,
                        'trend': trend,
                        'woeid': woeid,
                        "UTC_insert": datetime.datetime.now()})
        except Exception:
            self.logger.error(format_exc())
            self.disconnector()

    def mongo_trend_inserter(self, *args, **kwargs):
        '''this method insert a new trend into the "top_trends_collection".
        Just as easy as that.'''
        try:
            country = args[0]
            region = args[1]
            trend = args[2]
            ranking = args[3]
            woeid = args[4]
            self.mongo_trends_collection.\
                insert({'country': country,
                        'region': region,
                        'trend': trend,
                        'woeid': woeid,
                        'ranking': ranking,
                        "UTC_insert": datetime.datetime.now()})

        except Exception:
            self.logger.error(format_exc())
            self.disconnector()

    def mongo_error_node_updater(self, *args, **kwargs):
        '''this method updates a single twitter_user/node in the
        nodes_collection by setting an attribure "error" with the error_code
        found while trying to retrieve data from this user. This is normally
        triggered by a change of privacy settings by the user or by a twitter
        account that has been deleted.'''
        try:
            node_id = args[0]
            error_code = args[1]
            self.mongo_nodes_collection.update_one({'_id': node_id},
                                                   {'$set':
                                                       {"error": error_code}})

        except Exception:
            self.logger.error(format_exc())
            self.disconnector()

    def mongo_error_tweet_updater(self, *args, **kwargs):
        '''this method updates a single twitter_status/tweet in the
        tweets_collection by setting an attribure "error" with the error_code
        found while trying to retrieve data from this status. This is normally
        triggered by a change of privacy settings by owner of the tweet or by a
        tweet that has been deleted.'''
        try:
            tweet_id = args[0]
            error_code = args[1]
            self.mongo_tweets_collection.update_one({'_id': tweet_id},
                                                    {'$set':
                                                        {"error": error_code}})
        except Exception:
            self.logger.error(format_exc())
            self.disconnector()
