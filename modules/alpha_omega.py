# -*- coding: utf-8 -*-
"""
a module for the operations at the beginning and end of a process

Created on Sat Jan 17 18:13:10 2015

@author: giotto
"""
import time
import sys
import signal
import pymongo
from datetime import datetime
from traceback import format_exc
import modules.oathdance_tweepy_matteo as oathdance_tweepy_matteo
from multiprocessing import current_process
import logging
import os
from config import access_codes


class IO_handler(object):
    '''A class collecting the methods that creates and closes the basic environment
    needed by the single (sub)process to operate.'''

    def connector(self, *args, **kwargs):
        '''replacement of __init__, whose use is not advisable because of the
        multiprocessing structure:
        http://rhodesmill.org/brandon/2010/python-multiprocessing-linux-windows/;
        this method sets the some of required environment needed by the process
        an extra '-v' (as verbose) as argument enables extra messages for the
        insertion of data in the database and extra messages(useful mainly for
        debugging).'''
        # needed by views_maker() for the view_control_table_selector()

        self.followers_max_limit = 100000
        if '-v' in args or '--verbose' in args:
            self._verbose = True
        self.mongo_connector()
        self.apis_loader(*args)

    def logs(self, *args, **kwargs):
        '''this method is in charge of creating the logs files, a log_file_name
        variable is either provided explicitally or by etracting the name of
        the current process the folder_path_handler function takes care of
        where the files are created/saved;'''

        try:
            if args:
                if len(args) == 1:
                    log_file_name = args[0]
                    log_name = args[0]
                else:
                    log_file_name = args[0]
                    log_name = args[1]
            else:
                log_file_name = current_process().name
                log_name = current_process().name
            self.folder_path_handler('python_log_files')
            info_log_file_name = str(time.strftime(
                "%Y%m%d_")) + log_file_name + "_info.log"
            err_log_file_name = str(time.strftime(
                "%Y%m%d_")) + log_file_name + "_error.log"
            if os.path.exists(info_log_file_name):
                self.info_log_file = open(info_log_file_name, "a")
            else:
                self.info_log_file = open(info_log_file_name, "w")
            if os.path.exists(err_log_file_name):
                self.error_log_file = open(err_log_file_name, "a")
            else:
                self.error_log_file = open(err_log_file_name, "w")
            self.logger = logging.getLogger(log_name)
            self.logger.setLevel(logging.DEBUG)
            if not self.logger.handlers:

                # create a file handler
                handler_file_info = logging.FileHandler(info_log_file_name)
                handler_file_info.setLevel(logging.DEBUG)
                handler_file_error = logging.FileHandler(err_log_file_name)
                handler_file_error.setLevel(logging.WARNING)
                handler_stdout = logging.StreamHandler()
                handler_stdout.setLevel(logging.DEBUG)
                # create a logging format
                formatter = logging.Formatter(
                    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
                handler_file_info.setFormatter(formatter)
                handler_file_error.setFormatter(formatter)
                handler_stdout.setFormatter(formatter)
                # add the handlers to the logger
                self.logger.addHandler(handler_file_info)
                self.logger.addHandler(handler_file_error)
                self.logger.addHandler(handler_stdout)
            self.logger.info(' ')
            self.logger.info(
                '############################################################')
            self.logger.info(
                '###             Laistrygonians and Cyclops,              ###')
            self.logger.info(
                '###      wild Poseidon you will not encounter them       ###')
            self.logger.info(
                '###     unless you bring them along inside your soul,    ###')
            self.logger.info(
                '###    unless your soul sets them up in front of you.    ###')
            self.logger.info(
                '###                                                      ###')
            self.logger.info(
                '###            Hope the voyage is a long one.            ###')
            self.logger.info(
                '############################################################')
            self.logger.info(' ')
        except Exception:
            sys.stderr.write(str(datetime.utcnow()) +
                             " logs catch! " + format_exc())
            sys.exit(0)

    def apis_loader(self, *args, **kwargs):
        '''this method loads into the class' namespace a dictionary containing
        api_objects associated to a simbolic api_name; according to the
        argument given, this function select and loads the apis objects,
        belonging to a specific twitter account.'''

        try:
            if '_giannirage' in args:
                apis = oathdance_tweepy_matteo.Apis_giannirage(self.logger)
                self.apis = apis.__dict__
                self.logger.info("giannirage's apis have been loaded")
            elif '_gbsbvm' in args:
                apis = oathdance_tweepy_matteo.Apis_gbsbvm(self.logger)
                self.apis = apis.__dict__
                self.logger.info("gbsbvm's apis have been loaded")
            elif '_fragglecologne' in args:
                apis = oathdance_tweepy_matteo.Apis_fragglecologne(self.logger)
                self.apis = apis.__dict__
                self.logger.info("fragglecologne's apis have been loaded")
            elif '_ramonperez12345' in args:
                apis =\
                    oathdance_tweepy_matteo.Apis_ramonperez12345(self.logger)
                self.apis = apis.__dict__
                self.logger.info("ramonperez12345's apis have been loaded")
            elif '_aaAll3x' in args:
                apis = oathdance_tweepy_matteo.Apis_aaAll3x(self.logger)
                self.apis = apis.__dict__
                self.logger.info("_aaAll3x's apis have been loaded")
            elif '_lamasputo' in args:
                apis = oathdance_tweepy_matteo.Apis_lamasputo(self.logger)
                self.apis = apis.__dict__
                self.logger.info("_lamasputo's apis have been loaded")

            elif '_gianmar_1' in args:
                apis = oathdance_tweepy_matteo.Apis_gianmar_1(self.logger)
                self.apis = apis.__dict__
                self.logger.info("_lamasputo's apis have been loaded")
            elif '_pietracchioso_1' in args:
                apis = oathdance_tweepy_matteo.Apis_pietracchioso_1(self.logger)
                self.apis = apis.__dict__
                self.logger.info("_lamasputo's apis have been loaded")
            elif '_poeraccio_1' in args:
                apis = oathdance_tweepy_matteo.Apis_poeraccio_1(self.logger)
                self.apis = apis.__dict__
                self.logger.info("_lamasputo's apis have been loaded")

            else:
                self.logger.warning(
                    'no twitter account provided, no apis loaded')
        except Exception:
            self.logger.error(format_exc())
            self.disconnector()

    def mongo_connector(self, *args, **kwargs):
        '''this method establish a connection with the mongodb instance'''
        try:
            client = pymongo.MongoClient(
                                'mongodb://' + access_codes['MONGODB']['username'] + ':' +
                                 access_codes['MONGODB']['psw'] + '@adriana:' +
                                 access_codes['MONGODB']['port'])

            client = pymongo.MongoClient(
                'mongodb://complus:DUotto618@adriana:27018')
            if '-t' in args or '--test' in args:
                self.mongo_twitter_db = client.twitter_test
            else:
                self.mongo_twitter_db = client.twitter

            self.mongo_nodes_collection = self.mongo_twitter_db.nodes
            self.mongo_tweets_collection = self.mongo_twitter_db.tweets
            self.mongo_top_trends_collection =\
                self.mongo_twitter_db.world_top_trends
            self.mongo_trends_collection = self.mongo_twitter_db.world_trends
            self.mongo_control_collection = self.mongo_twitter_db.control
            self.logger.info(
                'Connected and ready to write on {}'.format(client))
        except Exception:
            self.logger.error(format_exc())
            self.disconnector()

    def disconnector(self, *args, **kwargs):
        '''This method makes sure that the connection with the database and
        the logs are closed and if called without arguments ends the process
        itself'''
        if not hasattr(self, 'logger'):
            self.logs()
        try:
            if hasattr(self, 'db_conn'):
                self.db_conn.close()
            self.logger.info(' ')
            self.logger.info(
                '############################################################')
            self.logger.info(
                '###                                                      ###')
            self.logger.info(
                '###           Keep Ithaka always in your mind.           ###')
            self.logger.info(
                '###     Arriving there is what you are destined for.     ###')
            self.logger.info(
                '###                                                      ###')
            self.logger.info(
                '############################################################')
            self.logger.info(' ')
            if hasattr(self, 'info_log_file'):
                self.info_log_file.close()
                self.error_log_file.close()
            if not args:
                os.kill(os.getpid(), signal.SIGTERM)
        except Exception:
            if hasattr(self, 'logger'):
                self.logger.error(format_exc())
            else:
                sys.stderr.write(str(datetime.utcnow()) +
                                 " logs catch! " + format_exc())
            sys.exit(1)
