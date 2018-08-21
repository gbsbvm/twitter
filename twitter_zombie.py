# -*- coding: utf-8 -*-
"""
this module instantiate a zombie object that will send a random tweet through a
specific twitter account. The purpose behind is to keep an account look active by posting
some random status on it

Created on Tue May 19 16:18:26 2015

@author: giotto
"""
import sys
from modules.twitter_agent import Twitter_hunts

if __name__=="__main__":
    account='giannirage'
    if len(sys.argv)==1:
        pass
    elif len(sys.argv)==2:
        account=sys.argv[1]
    zombie=Twitter_hunts()
    zombie.logs('zombie')
    zombie.apis_loader(account)
    
    zombie.zombi_twitter_account()
    zombie.disconnector()