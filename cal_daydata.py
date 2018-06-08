#coding:utf8
from sklearn.metrics.pairwise import euclidean_distances

import math
import numpy as np
import time
import pymongo as pm
import datetime
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

client =pm.MongoClient('192.168.1.109',27017)
db=client.cloudmusic
coll = db.day_new
coll_web = db.webdata

db_old=client.music
coll_sf = db_old.song_feature

def cal_emotion(songs):
    print 'emotioning...'
    sad = 0
    happy =0
    excited = 0
    peaceful =0
    for song in songs:
        _id = song['id']
        #print _id
        r = coll_sf.find_one({'_id':int(_id)})
        
        if r:
            #print r['type']
            if len(r['type'])<=0:
                continue
            if r['type'][0]=='HAPPY':
                happy+=1
            elif r['type'][0]=='SAD':
                sad+=1
            elif r['type'][0]=='EXCITED':
                excited+=1
            else:
                peaceful+=1
    return sad,happy,excited,peaceful


def cal_alldata():
    index =0
    _date = time.strftime('%Y%m%d-',time.localtime(time.time()))
    for item in coll.find({'_id':{'$regex':_date}}).sort('_id',pm.DESCENDING):
        #if index>2:
        #    break
        _id = item['_id']
        print 'id:',_id
        if item.has_key('happy_num'):
            continue
        
        sad_num,happy_num,excited_num,peaceful_num = cal_emotion(item['songs'])
        coll.update({'_id':_id},{"$set":{'happy_num':happy_num,'sad_num':sad_num,'excited_num':excited_num,'peaceful_num':peaceful_num,'numOfnew':len(item['songs'])}})
        
        index+=1
       
'''
def cal_sim(u1,u2):
    print 'cal the sin of two user:',u1['_id'],u2['_id'] 
    if not u1.has_key('numOfnew') or not u2.has_key('numOfnew') or u1['numOfnew']==0 or u2['numOfnew'] ==0:
        return 0.0
    x0 = float(u1['numOfnew'])
    x1 = float(u1['sad_num'])/x0
    x2 = float(u1['excited_num'])/x0
    x3 = float(u1['happy_num'])/x0
    x4 = float(u1['peaceful_num'])/x0

    y0 = float(u2['numOfnew'])
    y1 = float(u2['sad_num'])/y0
    y2 = float(u2['excited_num'])/y0
    y3 = float(u2['happy_num'])/y0
    y4 = float(u2['peaceful_num'])/y0

    x = [x1,x2,x3,x4]
    y = [y1,y2,y3,y4]
    print 'x:',x
    print 'y:',y
    return 1/(1+euclidean_distances(x,y)[0][0])





def find_neighbour(u1,users):
    print 'cal the users of one day...'
    neighbour = None
    max_sim = 0
    for user in users:
        sim = cal_sim(u1,user)
        print u1['_id'],user['_id'],sim
        if sim > max_sim and sim!=1:
            max_sim = sim
            neighbour = user
    return neighbour,max_sim

def cal_daydata():
    print 'cal the users of one day in all dates...'
    f_date = open('day_in_daynew.txt')
    _dates = []
    for item in f_date.readlines():
        item = item.strip()
        _dates.append(item)

    for _date in _dates:
        usersOfday = coll.find({'_id':{'$regex':_date}})
        for user in usersOfday:
            #if user.has_key('neighbourId'):
            #    continue
            neighbour,sim = find_neighbour(user,usersOfday)
            #print 'current:',user['_id'],' neighbour:',neighbour['_id']
            if neighbour:
                coll.update({'_id':user['_id']},{'$set':{'neighbourId':neighbour['_id'],'neighbourSim':sim}})
'''

def filter_date():
    print 'get all dates in the database'
    f_date = open('day_in_daynew.txt','w')
    _dates = []
    for item in coll.find({}).sort('_id',pm.DESCENDING):
        _id = item['_id']
        _date = _id.split('-')[0]
            
            
        if _date in _dates:
            continue

        _dates.append(_date)
        f_date.write(_date+'\n')
        print _dates
            


if __name__ =='__main__':
    #x = [1,0,1,0]
    #y = [1,0,1,0]
    #print euclidean_distances(x,y)[0][0]
    #cal_daydata()
    #filter_date()
    cal_alldata()
