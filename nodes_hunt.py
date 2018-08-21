# -*- coding: utf-8 -*-
"""
this module contains nodes_hunt, a function in charge of dealing with subprocesses
that search and store tweets about one or more subjects and twitter users related to those subjects;
the arguments needed are a list of one or more search terms(topics),

Created on Fri May 15 22:22:42 2015

@author: giotto
"""
import sys
from modules.twitter_agent import Twitter_hunts
import multiprocessing
import time
#from pycallgraph import PyCallGraph
#from pycallgraph.output import GraphvizOutput

if __name__=="__main__":
    '''function in charge of taking care of the creation and termination of each
    subprocesses in a nodes hunt; a nodes hunt is focused on the collection of twitter users
    interested on one or more topics; tweets regarding one or more topics are found, stored
    and used as a base to find twitter users related to the stored tweets;'''
    try:
        apis_slot='_ramonperez12345'
        db={
            'database':'twitter',
            'schema':''}
        debug_schema='burg_eltz'
        args=[]
        lang_parameters=None

        if '-v' in sys.argv or '--verbose' in sys.argv:
            args.append('-v')
        if '-t' in sys.argv or '--test' in sys.argv:
            args.append('-t')
        if len(sys.argv)==1:
            args.append('-v')
            db['schema']=debug_schema
            print 'Debug Mode, --Verbose: Y'
        elif len(sys.argv)>1:
            for arg in sys.argv:
                arg=str(arg)
                if (arg.startswith('--en') or
                    arg.startswith('--de') or
                    arg.startswith('--it') or
                    arg.startswith('--fr') or
                    arg.startswith('--es') or
                    arg.startswith('--nl') or
                    arg.startswith('--pt') or
                    arg.startswith('--sv')):

                    lang_parameters=arg[2:]
                elif arg.startswith('-')or arg.startswith('/')or arg.startswith('\\')or arg.endswith('.py'):
                    continue
                elif arg.startswith('_'):
                    apis_slot=arg

                else:
                    db['schema']=arg

        args.append(apis_slot)

        #lang_paramter MUST be the first argument in args
        args=[lang_parameters,]+args

        #a graphic visualization of the whole hunt
        #graphviz = GraphvizOutput(output_file=str(time.strftime("%Y%m%d"))+'_'+str(db['schema'])+'_nodes_hunt.png')
        #with PyCallGraph(output=graphviz):

        nodes_hunt=Twitter_hunts()
  #      nodes_hunt.connector('_basic_schema',**db)
  #      nodes_hunt.connector(**db)

        tweets_hunt=multiprocessing.Process(name='tweets_hunt',target=nodes_hunt.tweets_hunt, args=args,kwargs=db)
        nodes_hunt.tweets_hunt(*args,**db)
        args.append('-v')
        retweeters_hunt=multiprocessing.Process(name='retweeters_hunt',target=nodes_hunt.retweeters_hunt,args=args,kwargs=db)
        mentions_hunt=multiprocessing.Process(name='mentions_hunt',target=nodes_hunt.mentions_hunt,args=args,kwargs=db)

        tweets_hunt.start()
        time.sleep(60)
        retweeters_hunt.start()
        time.sleep(2)
        mentions_hunt.start()

        tweets_hunt.join()
        retweeters_hunt.join()
        mentions_hunt.join()
##############################################################

      #  nodes_hunt.connector('_basic_schema',**db)
       # nodes_hunt.control_table_inserter()

        #the 'open' prevents a sys.exit(0) which wouldn't allow eventual extra codes in the module where this function is called and after this function is called
        nodes_hunt.disconnector()

    except KeyboardInterrupt:
        print("You interrompted the main process and exited the program. See ya next time")
