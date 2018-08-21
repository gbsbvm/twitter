# -*- coding: utf-8 -*-
"""
Created on Thu Nov 24 16:11:37 2016

@author: giotto
"""

from modules.twitter_agent import Twitter_hunts
import multiprocessing
import time
import sys

if __name__=="__main__":
    args=[]
    db={
        'database':'twitter',
        'schema':'retweet_mention'}
    apis_slot='_fragglecologne'

    if len(sys.argv)==1:
        args.append('-v')

    elif len(sys.argv)>1:
        for arg in sys.argv:
            arg=str(arg)
            if arg.startswith('-')or arg.startswith('/')or arg.startswith('\\')or arg.endswith('.py'):
                continue
            elif arg.startswith('_'):
                apis_slot=arg
            else:
                db['schema']=arg
    db['schema']=str(db['schema'])
    if '-v' in sys.argv or '--verbose' in sys.argv:
        args.append('-v')
    if '-t' in sys.argv or '--test' in sys.argv:
        args.append('-t')
    args.append(apis_slot)
    try:

        intrusion=Twitter_hunts()
        retweeters_hunt=multiprocessing.Process(name='retweeters_hunt',target=intrusion.retweeters_hunt,args=args,kwargs=db)
        mentions_hunt=multiprocessing.Process(name='mentions_hunt',target=intrusion.mentions_hunt,args=args,kwargs=db)

        retweeters_hunt.start()
        time.sleep(5)
        mentions_hunt.start()

        retweeters_hunt.join()
        mentions_hunt.join()
 #       intrusion.connector('_basic_schema',**db)
#        intrusion.control_table_inserter()

        #the 'open' prevents a sys.exit(0) which wouldn't allow eventual extra codes in the module where this function is called and after this function is called
        intrusion.disconnector()


    except KeyboardInterrupt:
        print("You interrompted the process and exited the program. See ya next time")
