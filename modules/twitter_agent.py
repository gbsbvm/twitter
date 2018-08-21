# -*- coding: utf-8 -*-
"""
This module is to be considered as the backbone of each Twitter hunt process/
subprocess, its  methods are generally called within a single or multiprocess
environment and are responsible for gathering and/or managing Twitter data.

Created on Sat Jan 17 15:34:58 2015

@author: giotto
"""

import time
from traceback import format_exc
from modules.alpha_omega import IO_handler
from modules.tweepy_hunters import Tweepy_hunters
import threading
import random
import itertools
from random import shuffle
import datetime
from socket import gethostname
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))

class Twitter_hunts(IO_handler, Tweepy_hunters):
    '''A collection of methods/hunts dealing on a single or multi-threading level
    gathering/managing/processing Twitter data.'''

    def tweets_hunt(self, *args, **kwargs):
        '''this method is the first subprocess of nodes_hunt; it initiates and
        terminates threads by providing a single search_term (which is not necessary
        a single word) and a specific api(that it actually name the thread ;
        the assigned api also names the thread itself; each thread acts independently
        and does not interfere with the others thanks to self.lock which is hold by
        a thread until it hits a twitter_api's limit.'''
        try:
            self._hunt_name = 'tweets_hunt'
            self.schema = kwargs['schema'].lower()
            database = kwargs['database'].lower()
            log_file_name = database + '_' + self.schema + '_nodes_hunt'
            self.logs(log_file_name, self._hunt_name)
        except Exception:
            self.logs()
            self.logger.error(format_exc())
            self.disconnector()
        try:
            self.connector(*args, **kwargs)
            threads = []
            search_terms = self.search_terms_directory_handler(self.schema)
            lang_parameter = args[0]
            self.lock = threading.RLock()

            if len(search_terms) < len(self.apis):
                number_apis_needed = len(search_terms)
            else:
                number_apis_needed = len(self.apis)
            dispachted_apis = random.sample(list(self.apis), number_apis_needed)
            # https://docs.python.org/2/library/itertools.html#itertools.cycle
            for term, api_name in zip(search_terms, itertools.cycle(dispachted_apis)):
                api = self.apis[api_name]
                thread_name = gethostname() + api_name
                hunter = threading.Thread(
                    name=thread_name, target=self.tweets_hunter, args=(api, lang_parameter, term))
                threads += [hunter]
                hunter.start()
                time.sleep(2)
            for thread in threads:
                thread.join()
        except Exception:
            self.logger.error(format_exc())
            self.disconnector()
        else:
            self.disconnector("don't kill the signal")

    def timeline_hunt(self, *args, **kwargs):
        '''this method creates  a "time_line_hunter" threads in order to search
        and store the top trends of the moment'''

        try:
            self._hunt_name = 'timeline_hunt'
            self.schema = kwargs['schema'].lower()
            database = kwargs['database'].lower()
            log_file_name =\
                database + '_' + self.schema + '_' + self._hunt_name
            self.logs(log_file_name, self._hunt_name)
        except Exception:
            self.logs()
            self.logger.error(format_exc())
            self.disconnector()
        try:
            self.connector(*args, **kwargs)
            if self.schema.endswith('_timeline'):
                search_terms = [self.schema.split('_timeline')[0], ]
            else:
                search_terms = self.search_terms_directory_handler(self.schema)
            for target_user_name in search_terms:
                self.lock = threading.RLock()
                random_api_name = random.sample(list(self.apis), 1)[0]
                random_api = self.apis[random_api_name]
                thread_name = gethostname() + random_api_name
                time_line_hunt =\
                    threading.Thread(name=thread_name,
                                     target=self.timeline_hunter,
                                     args=(random_api, target_user_name))
                time_line_hunt.start()
                time_line_hunt.join()

        except Exception:
            self.logger.error(format_exc())
            self.disconnector()
        else:
            self.disconnector()

    def retweeters_hunt(self, *args, **kwargs):
        '''this method is the second subprocess of nodes_hunt and it uses a base
        of tweets found by tweets_hunt as input; the method initiates and
        terminates threads, that look for up to 100 users (aka "retweeters")
        who re-posted them; the assigned api also names the thread itself; each
        thread acts independently and does not interfere with the others thanks
        to self.lock which is hold by a thread until it hits a twitter_api's
        limit.'''
        try:
            self._hunt_name = 'retweeters_hunt'
            self.schema = kwargs['schema'].lower()
            database = kwargs['database'].lower()
            log_file_name = database + '_' + self.schema + '_nodes_hunt'
            self.logs(log_file_name, self._hunt_name)
        except Exception:
            self.logs()
            self.logger.error(format_exc())
            self.disconnector()

        try:
            self.connector(*args, **kwargs)

            threads = []
            self.logger.info('searching for retweeters')
            if not hasattr(self, 'lock'):
                self.lock = threading.RLock()

            for api_name in self.apis:
              #      hunter_name="retweeters_hunter_"+str(index+1)
                api = self.apis[api_name]
                thread_name = gethostname() + api_name
                hunter =\
                    threading.Thread(name=thread_name,
                                     target=self.retweeters_hunter,
                                     args=(api,))
                threads += [hunter]
                hunter.start()
                time.sleep(2)
            for thread in threads:
                thread.join()

        except Exception:
            self.logger.error(format_exc())
            self.disconnector()
        else:
            self.disconnector()

    def mentions_hunt(self, *args, **kwargs):
        '''this method provides the bases to mentioned_hunter that looks for
        twitter_users mentioned in previously retrieved tweets,

        itertools.cycle ensures that each search has a different api,
        https://docs.python.org/2/library/itertools.html#itertools.cycle;'''
        try:
            self._hunt_name = 'mentions_hunt'
            self.schema = kwargs['schema'].lower()
            database = kwargs['database'].lower()
            log_file_name = database + '_' + self.schema + '_nodes_hunt'
            self.logs(log_file_name, self._hunt_name)
        except Exception:
            self.logs()
            self.logger.error(format_exc())
            self.disconnector()

        try:
            self.connector(*args, **kwargs)
            apis = list(self.apis.values())
            shuffle(apis)
            api_cycler = itertools.cycle(apis)
            self.mentioned_hunter(api_cycler,)

        except Exception:
            self.logger.error(format_exc())
            self.disconnector()
        else:
            self.disconnector()

    def followers_id_hunt(self, *args, **kwargs):
        '''this method is  a subprocess of edges_hunt; it initiates and
        threads by taking independently an equal portion of the apis available,
        without communicating with the other subprocesses; each api is assigned
        to a thread and it names the thread itself; each thread acts
        independently and does not interfere with the others thanks to
        self.lock which is hold by a thread until it hits a twitter_api's
        limit.'''

        try:
            process_number = args[0]
            amount_processes = args[1]

            self._hunt_name = 'edges_hunt'
            self.schema = kwargs['schema'].lower()
            database = kwargs['database'].lower()
            if self.schema:
                log_file_name =\
                    database + '_' + self.schema + '_' + self._hunt_name
            else:
                log_file_name = database + '_database_' + self._hunt_name
            log_name = str(process_number) + '_' + \
                self._hunt_name + '_pid_' + str(os.getpid())
            self.logs(log_file_name, log_name)
        except Exception:
            self.logs()
            self.logger.error(format_exc())
            self.disconnector()

        try:
            self.connector(*args, **kwargs)
            if '-u' in args:
                self.limited = False
            else:
                self.limited = True
            self.process_targeted_users_count = 0
            self.process_foll_ids_count = 0
            search_terms = self.search_terms_directory_handler(self.schema)
            threads = []
            self.lock = threading.RLock()
            api_keys = self.apis.keys()

            # self.apis is a dictionary, hence it is not ordered, but its
            # keys(apis name) can be ordered in a list
            api_keys.sort()
            # how many apis will be assigned to each subprocess
            max_apis_available_pro_process = len(api_keys) // amount_processes
            # its sintax is obscure but it works, lists comprehensions are great
            # it divides the api_keys into a list of lists and it assigns one
            # of this sublist to dispachted_apis process_number is used as
            # index to pick the right sublist
            dispachted_apis =\
                [api_keys[i:i + max_apis_available_pro_process] for i
                 in range(0, len(api_keys),
                          max_apis_available_pro_process)][process_number - 1]
            for api_name in dispachted_apis:
                api = self.apis[api_name]
                thread_name = gethostname() + api_name
                hunter =\
                    threading.Thread(name=thread_name,
                                     target=self.followers_id_hunter,
                                     args=(api, search_terms))
                threads += [hunter]
                hunter.start()
                time.sleep(2)
            for thread in threads:
                thread.join()
        except Exception:
            self.logger.error(format_exc())
            self.disconnector()
        else:
            self.disconnector()

    def followers_followers_hunt(self, *args, **kwargs):
        '''the target_user name itself names the schema where the data will be
        stored; first the target_user's details are stored, secondly the
        target's followers details, lastly the target's followers' followers;
        the last step involves the use of multithreads; both in the second and
        third step is self.followers_list_hunter used as subthread although the
        second step the sub_thread is instantiated only the sake of
        self.followers_list_hunter portability; each thread is assigned to ONE
        specific api that also names it; each thread acts independently and
        does not interfere with the others thanks to self.lock that ensure that
        only one thread at the time is actually receiving data from the
        twitter_api or writing on the database; the lock is passed among the
        threads each time that the thread holding it is forced to "sleep" for
        15 min due to the twitter_api limitation.'''
        try:
            process_number = args[0]
            amount_processes = args[1]

            self._hunt_name = 'followers_list_hunt'
            self.schema = kwargs['schema'].lower()
            database = kwargs['database'].lower()
            log_file_name =\
                database + '_' + self.schema + '_followers_followers_hunt'
            log_name = str(process_number) + '_' + \
                self._hunt_name + '_pid_' + str(os.getpid())
            self.logs(log_file_name, log_name)
        except Exception:
            self.logs()
            self.logger.error(format_exc())
            self.disconnector()
        try:
            self.connector(*args, **kwargs)
            if '-u' in args:
                self.limited = False
            else:
                self.limited = True
            target_user_name = kwargs['schema']
            search_terms = [target_user_name + '_followers_followers', ]
            random_api_name = random.sample(list(self.apis), 1)[0]
            random_api = self.apis[random_api_name]
            # random_api here can be used without problem twice,
            # single_user_finder and followers_hunter have different different
            # calls/requests limits
            target_user = self.single_user_finder(random_api, target_user_name)
            self.mongo_node_inserter(
                target_user, search_terms, datetime.datetime(9999, 9, 9))
            threads = []
            self.lock = threading.RLock()
            api_keys = self.apis.keys()
            # self.apis is a dictionary, hence it is not ordered, but its
            # keys(the apis name) can be ordered in a list
            api_keys.sort()
            # how many apis will be assigned to each subprocess
            max_apis_available_pro_process = len(api_keys) // amount_processes
            # its sintax is obscure but it works,
            # it divides the api_keys into a list of lists and it assigns one
            # of this sublist to dispachted_apis
            dispachted_apis =\
                [api_keys[i:i + max_apis_available_pro_process]for i in
                 range(0, len(api_keys),
                       max_apis_available_pro_process)][process_number]
            for api_name, api in dispachted_apis.iteritems():
                thread_name = gethostname() + api_name
                hunter =\
                    threading.Thread(name=thread_name,
                                     target=self.followers_list_hunter,
                                     args=(api, search_terms))
                threads += [hunter]
                hunter.start()
                time.sleep(2)
            for thread in threads:
                thread.join()
        except Exception:
            self.logger.error(format_exc())
            self.disconnector()
        else:
            self.disconnector()

    def followers_list_hunt(self, *args, **kwargs):
        '''G4iven target_user, its detail are first retrieved through
        single_user_finder, and then its followers are target through the
        followers_list_hunter.'''
        # connector has to stay out of the try-except block since disconnector
        # depends on it
        try:
            self._hunt_name = 'followers_list_hunt'
            self.schema = kwargs['schema'].lower()
            database = kwargs['database'].lower()
            log_file_name =\
                database + '_' + self.schema + '_' + self._hunt_name
            self.logs(log_file_name, self._hunt_name)
        except Exception:
            self.logs()
            self.logger.error(format_exc())
            self.disconnector()
        try:
            self.connector(*args, **kwargs)
            if '-u' in args:
                self.limited = False
            else:
                self.limited = True
            target_user_name = self.schema
            search_terms = [target_user_name + '_followers', ]

            random_api_name_1 = random.sample(list(self.apis), 1)[0]
            random_api_1 = self.apis[random_api_name_1]
            random_api_name_2 = random.sample(list(self.apis), 1)[0]
            random_api_2 = self.apis[random_api_name_2]

            # random_api here can be used without problem twice,
            # single_user_finder and followers_hunter have different different
            # calls/requests limits
            target_user = self.single_user_finder(
                random_api_1, target_user_name)
            self.mongo_node_inserter(
                target_user, search_terms, datetime.datetime(9999, 9, 9))
            target_user = {'_id': target_user.id,
                           'screen_name': target_user.screen_name}
            self.lock = threading.RLock()
            thread_name = gethostname() + random_api_name_2
            hunter =\
                threading.Thread(name=thread_name,
                                 target=self.followers_list_hunter,
                                 args=(
                                     random_api_2, search_terms),
                                 kwargs=target_user)
            hunter.start()
            hunter.join()
        except Exception:
            self.logger.error(format_exc())
            self.disconnector()
        else:
            self.disconnector()

    def trends_hunt(self, *args, **kwargs):
        '''this method takes care of searching  and storing twitter's top trends
        of the momentit initiated and terminates a subthread that will take
        care of the operation; self.trends_hunter is subthreaded only for
        compatibility/ sake, since it is part of Tweepy_hunters;
        itertools.cycle ensures that each thread/search term has api,
        https://docs.python.org/2/library/itertools.html#itertools.cycle;'''
        try:
            self._hunt_name = 'trends_hunt'
            self.schema = kwargs['schema'].lower()
            database = kwargs['database'].lower()
            log_file_name =\
                database + '_' + self.schema + '_' + self._hunt_name
            self.logs(log_file_name, self._hunt_name)
        except Exception:
            self.logs()
            self.logger.error(format_exc())
            self.disconnector()

        try:
            self.connector(*args, **kwargs)
            self.logger.info('Starting collecting world trends')

            apis = list(self.apis.values())
            shuffle(apis)
            api_cycler = itertools.cycle(apis)

            self.lock = threading.RLock()
            tweet_hunter = threading.Thread(
                target=self.trends_hunter, args=(api_cycler,))
            tweet_hunter.start()
            tweet_hunter.join()

        except Exception:
            self.logger.error(format_exc())
            self.disconnector()
        else:
            self.disconnector()
