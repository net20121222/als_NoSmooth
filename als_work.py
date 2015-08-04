#!/usr/bin/python
#coding:utf-8

import sys, os, time, string, struct, gevent, re, gzip, shutil, traceback,gzip
import gevent.monkey
from json import loads,dumps
from als_gl import tasks_workqueue,tasks_worksem,tasks_workdict
from als_gl import co_routines_status,co_statussem
from gevent.queue import Queue
from gevent.lock import BoundedSemaphore
from als_log import logger
gevent.monkey.patch_all()

class AlsWorker():
    def __init__(self, config, log, iNum,viewlist):
        # 日志对象
        self.log = log
        # 配置对象
        self.config = config
        # 跟踪ID
        self.workid  = iNum
        # area list
        self.viewlist = viewlist
        self.dir_tmp = {}

    def check_ip(self, ins):
        #检查IP关键域
        m = re.match('(\d+\.\d+\.\d+\.\d+)', ins)
        if m:
           return 1
        else:
           return 0

    def param_check(self, line):
        try:
            # 拆分数据结构
            line.strip('\r\n')
            clinetip,timestamp,visitid,visitdata,streamflag,domain,lasttime,tmpnull = filter(lambda value:value != "-",re.split("[\t\s]+", line))
            if domain.find("?"):
                domain = domain.split("?")[0]
            # 检查关键域
            if not self.check_ip(clinetip):
                self.log.debug('ip format is not correct: %s', line)
                return -1
            inputs = [clinetip, visitid, domain, lasttime]
            # 聚合日志信息
            self.aggresion(*inputs)
        except Exception,ex:
            self.log.error("param_check ERROR:%s" %(ex))
            return -1
        return 0

    def read_gz_file(self,path):
        #读取存储目录日志
        try:
            fd_gz = gzip.open(path,'rb')
            for line in fd_gz:
                #行解析
                self.param_check(line)
            return 0
        except:
            info = sys.exc_info()
            traceback.extract_tb(info[2])
            self.log.error('open file or check file is not correct %s %s %s', info[0], info[1], path)
            return -1
        finally:
            fd_gz.close()

    def ip_into_int(self,ip):
        return reduce(lambda x,y:(x<<8)+y,map(int,ip.split('.')))

    # 查找IP在哪个区域
    def find_view_area(self,ip):
        for iNum in xrange(len(self.viewlist)):
            # 把ip和掩码与的位运算 
            ifind = self.ip_into_int(ip) & self.viewlist[iNum].mask_t
            # 查找ip是否在字典中 
            if ifind in self.viewlist[iNum].netid_t:
                return self.viewlist[iNum].netid_t[ifind]
        return "qita-qita-qita"

    def aggresion(self, *kw):
        try:
            area = self.find_view_area(kw[0])
            custid = int(kw[1])
            domain = kw[2]
            lasttime = int(kw[3])
            iflag = ((lasttime > 61000) and [1] or [0])[0]

            # stringkey = dumps(key)
            dir_key = dumps({"area":area,"domain":domain})
            dir_value = self.dir_tmp.get(dir_key,None)
            if None == dir_value:
                value  = {custid:iflag}
                self.dir_tmp.update({dir_key:value})
            else:
                dir_valuevalue = dir_value.get(custid,None)
                if None == dir_valuevalue:
                    self.dir_tmp[dir_key].update({custid:iflag})
                else:
                    if iflag and not dir_valuevalue:
                        self.dir_tmp[dir_key].update({custid:iflag})
        except:
            info = sys.exc_info()
            traceback.extract_tb(info[2])
            self.log.error('aggresion %s %s', info[0], info[1])
    
    def sync_data(self):
        tasks_worksem.acquire()
        for dir_key,dir_value in self.dir_tmp.iteritems():
            dir_taskvalue = tasks_workdict.get(dir_key,None)
            if dir_taskvalue:
                for dir_keykey,dir_valuevalue in dir_value.iteritems():
                    dir_taskvaluevalue = dir_taskvalue.get(dir_keykey,None)
                    if dir_taskvaluevalue:
                        continue
                    tasks_workdict[dir_key].update({dir_keykey:dir_valuevalue})
            else:
                tasks_workdict.update({dir_key:dir_value})
        tasks_worksem.release()

    def handle_tasks(self):
        #print "work:%dstart" %(self.workid)
        gevent.sleep(0)
        #等待任务队列
        while 1:
            #如果处理队列为空， 则不处理
            if not tasks_workqueue.empty():
                taskinfo = tasks_workqueue.get()
                self.log.debug("WORKID%d:%s" %(self.workid,taskinfo))
                if os.path.exists(taskinfo):
                    #处理日志
                    self.read_gz_file(taskinfo)
                if self.dir_tmp:
                    self.sync_data()
                self.dir_tmp.clear()
                #gevent.sleep(5)
            else:
                co_statussem.acquire()
                if co_routines_status:
                    co_statussem.release()
                    gevent.sleep(2)
                    continue
                co_statussem.release()
                break

            
 


