# -*- coding: utf-8 -*-
"""
this module contains utilities dealing with current working directory,
eventual folders creation, and above all with the twitter_api errors;
a couple of unused function (id_maker and id_decoder) are kept here

Created on Wed Jun 25 18:16:11 2014

@author: giotto
"""
import os
from datetime import datetime
from time import sleep as time_sleep
from traceback import format_exc
import errno
import sys
import itertools
from threading import currentThread


class Utils(object):
    '''Acouple of extra methods that I did not know exactly where to store'''
    # static variables that allows several threads to have a common counters;
    global_count429 = 0
    count401 = 0
    count404 = 0
    count403 = 0
    count429 = 0

    def tweepy_error_boolean(self, e, node_id=None, tweet_id=None):
        '''an adaptation taken from a python-twitter cookbook;
        it takes care of tweepy exceptions into a multi threading environment;
        in case of a 429 code (rate limit exceed), the thread will be put on
        sleep and release the thread lock letting other(s) thread work in the
        meanwhile; the thread is re-activated after the 15 min sleep AND when
        the other thread(s) release the lock'''
        try:
            if hasattr(e, 'response'):
                if hasattr(e.response, 'status_code'):
                    if e.response.status_code == 429:
                        Utils.global_count429 += 1

                        self.count429 += 1
                        self.logger.info(currentThread().getName(
                        ) + " stepped into the limit. No big deal.")
                        # self.count429, belongs to the single hunt,
                        # Utils.global_count429 gives tweets_hunt+retweets_hunt
                        # count
                        self.logger.info("{} rate limit count: {}".format(
                            currentThread().getName(), self.count429,))
                        self.logger.info(
                            "count401 (Protected Timeline):\
                            {}".format(Utils.count401))
                        self.logger.info(
                            "count403 (Forbidden) : \
                            {}".format(Utils.count403))
                        self.logger.info(
                            "count404 (Not Found) : \
                            {}".format(Utils.count404))
                        self.logger.info("Retrying in 15 minutes...ZzZ...")
                        if hasattr(self, 'lock'):
                            self.lock.release()
                        time_sleep(60 * 15 + 5)
                        if hasattr(self, 'lock'):
                            self.lock.acquire()
                        # just to refresh the connection after the 15 min pause
                        self.mongo_connector()
                        self.logger.info(currentThread().getName(
                        ) + ' ...ZzZ...Awake now and trying again.')
                        return True
                    elif e.response.status_code == 401:
                        # typical for protected timelines
                        Utils.count401 += 1
                        self.logger.info(
                            'Encountered 401 Error (Protected Profile)')
                        self.logger.info("count401: {}".format(Utils.count401))
                        if node_id:
                            self.mongo_error_node_updater(node_id, '401_Error')
                            self.mongo_control_followers_locker(
                                node_id, 'unlock')
                        if tweet_id:
                            self.mongo_error_tweet_updater(
                                tweet_id, '401_Error')

                        return False
                    elif e.response.status_code == 403:
                        Utils.count403 += 1
                        self.logger.info('Encountered 403 Error (Forbidden)')
                        if node_id:
                            self.mongo_error_node_updater(node_id, '403_Error')
                            self.mongo_control_followers_locker(
                                node_id, 'unlock')
                        if tweet_id:
                            self.mongo_error_tweet_updater(
                                tweet_id, '403_error')
                        return False
                    elif e.response.status_code == 404:
                        # a deleted account
                        Utils.count404 += 1
                        self.logger.info('Encountered 404 Error (Not Found)')
                        self.logger.info("count404: {}".format(Utils.count404))
                        if node_id:
                            self.mongo_error_node_updater(node_id, '404_Error')
                            self.mongo_control_followers_locker(
                                node_id, 'unlock')
                        if tweet_id:
                            self.mongo_error_tweet_updater(
                                tweet_id, '404_error')

                        return False
                    elif e.response.status_code in (500, 502, 503, 504,):
                        self.logger.error('Troubles from Twitter side. \
                        Encountered {0} Error. Retrying in {1} seconds'.format(
                            e.response.status_code, 10))
                        time_sleep(10)
                        return True
                    else:
                        self.logger.error("new exception to catch!!")
                        self.logger.error(
                            "e:         " + str(e) + str(type(e)))
                        self.logger.error(
                            "e.response.status_code:  " +
                            str(e.response.status_code))
                        return False
                ############################
                else:
                    self.logger.info("e.response do not have attr status_code")
                    self.logger.info(" dir(e.response): " +
                                     str(dir(e.response)))
                    return True
                ############################
            else:
                self.logger.warning("e do not have attr response")
                self.logger.warning("dir(e): " + str(dir(e)))
                time_sleep(10)
                return True
        except Exception:
            self.logger.error(format_exc())
            self.disconnector()

    def folder_path_handler(self, *args, **kwargs):
        '''this method changes and eventually creates the current working
        directory; it is  used together with directory_handler; its flexibility
        is limited to a couple of pre-set paths'''
        try:
            if args:
                folder_name = str(args[0])
            else:
                folder_name = ''
            cwd = str(os.getcwd())
            if '/root' in cwd or '/home/complus' in cwd:
                complus_basic_folder = "/home/complus/"
                complus_specific_folder = "/home/complus/" + folder_name
                self.directory_handler(complus_basic_folder)
                self.directory_handler(complus_specific_folder)

                os.chdir(complus_specific_folder)
            elif '/giotto' in cwd:
                self.directory_handler("/home/giotto/Documents/statistics/")
                self.directory_handler(
                    "/home/giotto/Documents/statistics/" + folder_name)

                os.chdir("/home/giotto/Documents/statistics/" + folder_name)
            # here we can add an eventual third terminal
            else:
                if hasattr(self, 'logger'):
                    self.logger.warning(
                        'Path issues. On which machine are you? File will be\
                        saved on the Current Working Directory: ' + str(cwd))
                else:
                    sys.stderr.write(str(datetime.utcnow(
                    )) + 'Path issues. On which machine are you? File will be\
                    saved on the Current Working Directory: ' + str(cwd))

        except Exception:
            if hasattr(self, 'logger'):
                self.logger.error(format_exc())
            else:
                sys.stderr.write(str(datetime.utcnow()) +
                                 " logs catch! " + format_exc())
            self.disconnector()

    def directory_handler(self, *args, **kwargs):
        '''it creates a new directory if it does not already exists;
        found online its logic follows Grace Hopper's principle: “it is easier
        to ask forgiveness than permission” '''
        try:
            folder = args[0]
            try:
                os.makedirs(folder)
            except OSError as exc:  # Python >2.5
                if exc.errno == errno.EEXIST and os.path.isdir(folder):
                    pass
                else:
                    raise
        except Exception:
            self.logger.error(format_exc())
            # in order to avoid an infinite loop with disconnector and logs
            sys.exit(0)

    def search_terms_directory_handler(self, *args, **kwargs):
        '''Given a schema_name as an argument, this function returns a list of
        search_terms associated with the given schema_name; the list is
        extracted from a dictionary which is created by loading schema_names
        and search_terms from a schemas_structure.txt file; unlike search_terms
        schema_names are not present nor stored in any way in the database;
        since Mongodb is case sensive, this function sets all terms in lower
        cases. '''
        try:

            directory_file = self.mongo_control_collection.find_one(
                {'schema_structure': {'$exists': 1}})
            search_terms_directory = directory_file['schema_structure']

            if args and args[0]:  # leave me like this

                schema_name = args[0].lower()
                if schema_name == 'all_search_terms':
                    all_search_terms = list(itertools.chain(
                        *search_terms_directory.values()))
                    return all_search_terms

                else:
                    try:
                        search_terms =\
                            [single_term.lower().split('_timeline')[0]
                                for single_term in
                                search_terms_directory[schema_name]]
                        return search_terms
                    except KeyError:
                        self.logger.error(
                            """The schema_name {} does not exist. Hereby a list
                            of the schema_names available
                            """.format(schema_name))
                        for schema in sorted(search_terms_directory.keys()):
                            self.logger.error(schema)
                        self.disconnector()

            else:
                return None
        except Exception:
            self.logger.error(format_exc())
            self.disconnector()
