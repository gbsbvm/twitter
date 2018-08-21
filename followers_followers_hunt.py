# -*- coding: utf-8 -*-
"""
this module is in charge of dealing with the single subprocesses that look for
for all the followers, together with their details from a list of targets present in the control list;
also the edges between the target and the followers will be created;

it needs a schema_name where tweets and nodes have been stored during the nodes hunt,
any followed-follower connection among each users is searched and stored;

it is possible to decide how many subprocesses to initiate by giving the max number
of subprocesses desidered as argument in the command line

Created on Fri Jun 26 18:24:47 2015

@author: giotto
"""
import multiprocessing
from modules.twitter_agent import Twitter_hunts
import sys
import time
#from pycallgraph import PyCallGraph
#from pycallgraph.output import GraphvizOutput


def followers_followers_hunt(*args, **kwargs):
    try:
        args = []
        db = {
            'database': 'twitter',
            'schema': ''}
     #   apis_slot='_giannirage'
        apis_slot = '_fragglecologne'
        if '-v' in sys.argv or '--verbose' in sys.argv:
            args.append('-v')
        if '-t' in sys.argv or '--test' in sys.argv:
            args.append('-t')

        if '--unlimited' in sys.argv or '-u' in sys.argv:
            args.append('-u')

        debug_schema = 'ITB_Berlin'
        max_number_subprocesses = 1

        sub_processes = []
    ###################################################
        if len(sys.argv) == 1:
            apis_slot = '_fragglecologne'
            args.append('-v')
            db['schema'] = debug_schema
            print 'Debug Mode, --Verbose: Y'
            print 'Writing on: '
            for key in db:
                print key, db[key]
        elif len(sys.argv) > 1:
            for arg in sys.argv:
                arg = str(arg)
                if arg.startswith('-')or arg.startswith('/')or arg.startswith('\\')or arg.endswith('.py'):
                    continue
                elif arg.startswith('_'):
                    apis_slot = arg
                else:
                    try:
                        max_number_subprocesses = int(arg)
                    except ValueError:
                        db['schema'] = arg

        intrusion = Twitter_hunts()

        for index in range(max_number_subprocesses):
            # args has to be build in this way due to the index
            edge_hunter_args = [
                index, max_number_subprocesses, apis_slot] + args
            edge_hunter = multiprocessing.Process(name='followers_followers_hunt' + str(
                index + 1), target=intrusion.followers_followers_hunt, args=edge_hunter_args, kwargs=db)
            sub_processes + [edge_hunter]
            edge_hunter.start()
            # launching the same processes at the same time could cause some a
            # recurrencies on some specific activity wih the db
            time.sleep(60)
        for hunter in sub_processes:
            hunter.join()

            intrusion.disconnector()

    except KeyboardInterrupt:
        print("You interrompted the main process and exited the program. See ya next time")

if __name__ == "__main__":
    followers_followers_hunt()
