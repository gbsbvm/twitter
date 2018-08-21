# -*- coding: utf-8 -*-
"""
this Module is in charge for bridging the data from the Mongodb Database;
the class Mongo_extractor relies on a previously set environment in order to
work as this class is supposed to be inheriteded by a child class

Created on Thu Sep 10 19:02:36 2015

@author: giotto
"""
from traceback import format_exc
import random


class Mongo_extractor(object):
    '''a collection of methods extracting data from mongodb'''

    def mongo_retweeters_hunt_target_finder(self, *args, **kwargs):
        try:
            match = {'$match': {'retweet_count': {'$ne': 0},
                                'retweeters_hunted': {'$exists': 0},
                                'control_retweeters_lock': {'$exists': 0},

                                'error': {'$exists': 0}}}
            try:
                tweet = next(self.mongo_tweets_collection.
                             aggregate([match,
                                        {'$project':
                                         {'author_id': 1,
                                          'search_terms': 1}},
                                        {'$sample': {'size': 1}}, ]))

                return tweet
            except StopIteration:
                self.logger.info(
                    'mongo_retweeters_hunt_target_finder run out of targets!')

        except Exception:
            self.logger.error(format_exc())
            self.disconnector()

    def mongo_mentioned_hunt_target_finder(self, *args, **kwargs):
        try:
            match = {'$match': {'mentions': {'$exists': 1},
                                'mentioned_hunted': {'$exists': 0},
                                'control_mentioned_lock': {'$exists': 0},
                                'error': {'$exists': 0}}}
            try:
                tweet = next(self.mongo_tweets_collection.
                             aggregate([match,
                                        {'$project':
                                         {'author_id': 1,
                                          'mentions': 1,
                                          'search_terms': 1,
                                          'created_at': 1}},
                                        {'$sample': {'size': 1}}, ]))

                return tweet
            except StopIteration:
                self.logger.info(
                    'mongo_mentioned_hunt_target_finder run out of targets!')

        except Exception:
            self.logger.error(format_exc())
            self.disconnector()

    def mongo_followers_id_hunt_target_finder(self, *args, **kwargs):
        '''function in charge of selecting a random node by the nodes collection
        that has not been targeted by followers_id_hunt yet; it works in both
        cases that a sub_set of nodes is targeted by providing a search_term
        and that the whole set/collection is considered'''
        def node_target_finder(*args, **kwargs):
            '''function in charge of building the query against the database in
            such a flexible way that a search_terms might provided or not'''

            match_query = {'followers_count': {'$ne': 0},
                           'error': {'$exists': 0},
                           'followers_id_hunt_edges_update_time':
                           {'$exists': 0},
                           'control_followers_id_lock': {'$exists': 0}, }
            if args:
                single_search_term = args[0]
                search_terms_field = 'search_terms.' + \
                    str((single_search_term.lower()).encode('utf-8').replace('$',''))
                match_query[search_terms_field] = {'$exists': 1},
            node = next(self.mongo_nodes_collection.
                        aggregate([{'$match': match_query, },
                                   {'$project': {
                                       'screen_name': 1}},
                                   {'$sample': {'size': 1}}, ]))
            return node

        try:
            list_terms = args[0]
            if list_terms:
                random.shuffle(list_terms)
                for single_search_term in list_terms:
                    try:
                        target_node = node_target_finder(single_search_term)
                        if target_node:
                            return target_node
                            break
                    except StopIteration:
                        self.logger.info(
                            'mongo_followers_id_hunt_target_finder run out of\
                            targets!')
            else:
                return node_target_finder()
        except Exception:
            self.logger.error(format_exc())
            self.disconnector()

    def mongo_followers_list_hunt_target_finder(self, *args, **kwargs):
        try:
            list_terms = args[0]
            for single_search_term in list_terms:
                search_terms_field = 'search_terms.' + \
                    str((single_search_term.lower()).encode('utf-8').replace('$',''))
                node = self.mongo_nodes_collection.\
                    find_one({'followers_count': {'$ne': 0},
                              search_terms_field: {'$exists': 1},
                              'error': {'$exists': 0},
                              'followers_list_hunt_edges_update_time':
                              {'$exists': 0},
                              'control_followers_list_lock': {'$exists': 0}, },
                             {'screen_name': 1})
                if node:
                    break
            if node:
                return node
            else:
                self.logger.info(
                    'mongo_followers_list_hunt_target_finder run out of \
                    targets!')
        except Exception:
            self.logger.error(format_exc())
            self.disconnector()
