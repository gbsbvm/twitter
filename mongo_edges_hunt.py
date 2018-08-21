# -*- coding: utf-8 -*-
"""
this module is in charge of dealing with the single subprocesses that look for edges;
given a schema where tweets and nodes have been stored during the nodes hunt, 
any followed-follower connection among each users is searched and stored; 
the goal is to collect as many potential edges as possible, by looking for 
all the followers ids for each node stored during the nodes hunt;
it is possible to decide how many subprocesses to initiate by giving the max number
of subprocesses desidered as argument

Created on Fri Apr 03 22:29:36 2015

@author: giotto
"""
import multiprocessing
from modules.twitter_agent import Twitter_hunts
import sys
import time

if __name__=="__main__":    
    args=[]
    db={
        'database':'twitter',
        'schema':''}
 
 #   apis_slot='_giannirage'       
    apis_slot='_fragglecologne'
    if '-v' in sys.argv or '--verbose' in sys.argv:
        args.append('-v')
    if '-t' in sys.argv or '--test' in sys.argv:
        args.append('-t')
    if '-u' in sys.argv or '--unlimited' in sys.argv:
        args.append('-u')
        
    debug_schema='ITB_berlin_followers'
    max_number_subprocesses=8
    
    sub_processes=[]
###################################################
    if len(sys.argv)==1:
        apis_slot='_fragglecologne'
        args.append('-v')        
        db['schema']=debug_schema
        print 'Debug Mode, --Verbose: Y'
        print 'Writing on: '
        for key in db:
            print key, db[key]
    elif len(sys.argv)>1: 
        for arg in sys.argv:
            arg=str(arg.lower())
            if arg.startswith('-')or arg.startswith('/')or arg.startswith('\\')or arg.endswith('.py'):                
                continue
            elif arg.startswith('_'):
                apis_slot=arg
            else:
                try:
                    max_number_subprocesses=int(arg)
                except ValueError:
                    db['schema']=arg
                
    try:        
        intrusion=Twitter_hunts()
        for index in range(max_number_subprocesses):
            #args has to be build in this way due to the index
            edge_hunter_args=[index+1,max_number_subprocesses,apis_slot]+args
            edge_hunter=multiprocessing.Process(name='edges_id_hunt'+str(index+1),target=intrusion.followers_id_hunt,args=edge_hunter_args,kwargs=db)
            sub_processes+[edge_hunter]
            edge_hunter.start()
            #launching the same processes at the same time could cause some a recurrencies on some specific activity wih the db
            time.sleep(30)
        for hunter in sub_processes:
            hunter.join() 
    
    except KeyboardInterrupt:
        print("You interrompted the main process and exited the program. See ya next time")        
        sys.exit(1)