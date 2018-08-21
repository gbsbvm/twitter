# -*- coding: utf-8 -*-
"""
this module starts the followers_followers_hunt;
it is conceived to be launch from the command line with an argument as target
given a twitter user as target, a schema will be named after it and all its followers
will be found and stored; then, all the followers of the followers will be found and stored,
creating a network of twitter users with the targeted user in the center of it

data are stored on schema reflecting the same name of the user target;
 following flags:
     [-v; --verbose] Activates verbose mode for the inserts
     [--charlotte;--sophie] Optional database choice

Created on Fri Apr 10 20:56:52 2015

@author: giotto
"""

from modules.twitter_agent import Twitter_hunts
import sys


def followers_hunt(*args, **kwargs):
    try:
        args = []
        db = {
            'database': 'twitter',
            'schema': ''}

        apis_slot = '_fragglecologne'
        if '-v' in sys.argv or '--verbose' in sys.argv:
            args.append('-v')
        if '-t' in sys.argv or '--test' in sys.argv:
            args.append('-t')

        if '--unlimited' in sys.argv or '-u' in sys.argv:
            args.append('-u')

        debug_schema = 'giannirage'

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

        args.append(apis_slot)

        intrusion = Twitter_hunts()
        intrusion.followers_list_hunt(*args, **db)

    except KeyboardInterrupt:
        print("You interrompted the process and exited the program. See ya next time")

if __name__ == "__main__":
    followers_hunt()
