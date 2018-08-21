# -*- coding: utf-8 -*-
"""
module used to initialize the collection of trends

Created on Sat Apr 04 12:40:45 2015

@author: giotto
"""
import sys
from modules.twitter_agent import Twitter_hunts
#from pycallgraph import PyCallGraph
#from pycallgraph.output import GraphvizOutput


if __name__ == "__main__":
    args = []
    db = {'database': 'twitter',
          'schema': 'world_trends'}

    apis_slot = '_fragglecologne'
    args.append(apis_slot)
    if '-t' in sys.argv or '--test' in sys.argv:
        args.append('-t')

    try:
        # a graphic visualization of the whole hunt
        #graphviz = GraphvizOutput(output_file='trends_hunt.png')
        # with PyCallGraph(output=graphviz):

        intrusion = Twitter_hunts()
        intrusion.trends_hunt(*args, **db)

    except KeyboardInterrupt:
        print("You interrompted the process and exited the program.\
               See ya next time")
