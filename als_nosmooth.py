#!/usr/bin/python
#coding:utf-8
#

import gevent
import gevent.monkey
gevent.monkey.patch_all()
from gevent.queue import Queue
from gevent.lock import BoundedSemaphore
from als_gl import tasks_workqueue,tasks_worksem,tasks_workdict
from als_gl import co_routines_status,co_statussem
import sys,os, time, string, struct, re, traceback, signal,urllib
from json import loads,dumps
from als_serverconfig import AlsServerConfig
from als_work import AlsWorker 
from als_log import logger

CONFIG_PATH = "/usr/local/src/als_NoSmooth/etc"

if __name__ == '__main__':
    '''
    example:anhui-huangshan-tietong { match-clients {110.122.224.0/20
    area list:subnet mask(at most 32),dirc of ip(key is 110.122.224.0,value is anhui-huangshan-tietong)
    '''      
    class ViewName:
        def __init__(self):
            self.mask_t = 0
            self.netid_t = {}

    def loadconfig():
        #拼接当前路径目录的配置文件
        config_path = os.path.join(CONFIG_PATH, "als_server.ini")
        #取路径名
        dir= os.path.dirname(config_path)
        #判断路径是否存在， 否则创建
        if not os.path.isdir(dir):
            os.mkdir(dir)
        #创建配置对象
        config = AlsServerConfig(config_path)
        #返回配置对象
        return config

    def loginit(config):
        #拼接当前路径目录的配置文件
        log_path = os.path.join(config.HANDLELOGPATH, "als_server.log")
        #拼接当前路径目录的配置文件
        dir= os.path.dirname(log_path)
        #判断路径是否存在， 否则创建
        if not os.path.isdir(dir):
            os.mkdir(dir)
        #创建日志对象
        log=logger(log_path)
        #返回日志对象
        return log

    def sigint_handler(signum, frame):
        #信号处理，全局的信号灯
        global is_sigint_up
        #收到中断信号， 信号灯亮起
        is_sigint_up = True
        log.debug("catched interrupt signal!")
                             
    # 录入view named数据
    def load_view_data(log,view_name_list):
        strView_path = os.path.join(CONFIG_PATH,"named.conf")
        if not os.path.exists(strView_path):
            log.error("name.conf is not exist")
            return -1   
        try:
            with open(strView_path) as ViewNamefd:
                for line in ViewNamefd:
                    # 获取数据类似220.11.11.11/26i;220.22.22.22/26 
                    net_ip = line.split()[4].replace("{","").replace("}","").rstrip(";").split(";")
                    # 处理220.11.11/26,list包含32个class
                    for net_ip_line in net_ip:
                        # 生成[220.11.11,26] 
                        network = net_ip_line.split('/') 
                        mask = 0xffffffff
                        mask <<= 32-int(network[1])
                        # ip转化为int类型 
                        realmask = ip_into_int(network[0])
                        # 判断是否存在mask的flag 
                        bViewflag = True                
                        # 轮询list 
                        for iNum in range(len(view_name_list)):
                            if(mask == view_name_list[iNum].mask_t):
                                view_name_list[iNum].netid_t[realmask] = line.split()[1]
                                bViewflag = False
                                break
                        # 如果不在list中间，则添加到list 
                        if(bViewflag):
                            # 新建结构体
                            View_ip_Infor = ViewName()  
                            View_ip_Infor.mask_t = mask
                            View_ip_Infor.netid_t[realmask] = line.split()[1]
                            view_name_list.append(View_ip_Infor)
        except IOError as ex:
            log.error("Read named.conf Error:%s",str(ex))
            return -1
        return 0

    def startwork():
        co_routines = []
        co_routines_status = 1
        for iNum in range(config.THREAD_NUM):
            worker = AlsWorker(config, log, iNum,view_name_list)
            work = gevent.spawn(worker.handle_tasks)
            co_routines.append(work)
        list_ippath = get_handlefilepath(config)
        for strippath in list_ippath:
            put_querywork(strippath)

        while 1:
            if tasks_workqueue.empty():
                co_statussem.acquire()
                co_routines_status = 0
                co_statussem.release()
                break
            if is_sigint_up:
                gevent.shutdown()
                break
            gevent.sleep(10)
        gevent.joinall(co_routines)
        out_result()  
    
    def split_area(strarea):
        areainfo = strarea.split("-")
        realarea = areainfo[0]
        realsp = areainfo[-1]
        return realarea,realsp

    def out_result():
        dir_result = {}

        for dir_key,dir_value in tasks_workdict.iteritems():
            dir_stringkey = loads(dir_key)
            iProNum = 0
            for dir_valuekey,dir_valuevalue in dir_value.iteritems():
                if dir_valuevalue:
                    iProNum += 1
            realarea,realsp = split_area(dir_stringkey["area"])
            dir_areainfokey = dumps({"area":realarea,"sp":realsp})
            dir_areainfovalue = [len(dir_value),iProNum]
            if dir_result.has_key(dir_stringkey["domain"]):
                dir_areainfovalue_next = dir_result[dir_stringkey["domain"]].get(dir_areainfokey,None)
                if dir_areainfovalue_next:
                    insertvalue = [x+y for x, y in zip(dir_areainfovalue, dir_areainfovalue_next)]
                    dir_result[dir_stringkey["domain"]].update({dir_areainfokey:insertvalue})
                else:
                    dir_result[dir_stringkey["domain"]].update({dir_areainfokey:dir_areainfovalue})
            else:
                dir_result.update({dir_stringkey["domain"]:{dir_areainfokey:dir_areainfovalue}})

        output_reportDetail(dir_result)
        output_reportALL(dir_result)

    def get_doamin(strurl):
        proto, rest = urllib.splittype(strurl)
        res, rest = urllib.splithost(rest)
        result = "unknow" if not res else res
        if result.find(":"):
            result = result.split(":")[0]
        return result

    def output_reportALL(dir_result):
        str_con = ""
        dir_allreport = {}

        for k,v in dir_result.iteritems():
            strurl = get_doamin(k)
            if dir_allreport.has_key(strurl):
                for m,n in v.iteritems():
                    dir_value = dir_allreport[strurl].get(m,None)
                    if dir_value:
                        insertvalue = [x+y for x, y in zip(n, dir_value)]
                        dir_allreport[strurl].update({m:insertvalue})
                    else:
                        dir_allreport[strurl].update({m:n})
            else:
                dir_allreport.update({strurl:v})
                

        for str_key,str_value in dir_allreport.iteritems():    
            iTatalAll,iTatalLost =0,0
            str_PART2 = ""
            for str_keykey,str_valuevalue in str_value.iteritems():
                str_strkeykey = loads(str_keykey)
                str_conPart = "区域:%s\t运营商:%s\t区域人数:%d\t区域延迟人数:%d\t百分比:%d\n" %(str_strkeykey["area"],str_strkeykey["sp"],str_valuevalue[0],str_valuevalue[1],(str_valuevalue[1]*100/str_valuevalue[0]))
                str_PART2 += str_conPart
                iTatalAll += str_valuevalue[0]
                iTatalLost += str_valuevalue[1]
            str_PART1 = str_key + "\t总人数:%d\t总延迟人数:%d\t总百分比:%d\n" %(iTatalAll,iTatalLost,(iTatalLost*100/iTatalAll))
            str_con += str_PART1
            str_con += str_PART2
            str_con += "\n"

        write_result(str_con,"_Allreport.txt")



    def output_reportDetail(dir_result):
        str_con = ""
        for k,v in dir_result.iteritems():
            iTatalAll,iTatalLost =0,0
            str_PART2 = ""
            for m,n in v.iteritems():
                str_m = loads(m)
                str_conPart = "区域:%s\t运营商:%s\t区域人数:%d\t区域延迟人数:%d\t百分比:%d\n" %(str_m["area"],str_m["sp"],n[0],n[1],(n[1]*100/n[0]))
                str_PART2 += str_conPart
                iTatalAll += n[0]
                iTatalLost += n[1]
            str_PART1 = "房间:%s\t房间人数:%d\t房间延迟人数:%d\t百分比:%d\n" %(k,iTatalAll,iTatalLost,(iTatalLost*100/iTatalAll))
            str_con += str_PART1
            str_con += str_PART2
            str_con += "\n"
        #print str_con
        write_result(str_con,"_detailreport.txt")

    def write_result(str_con,strname):
        try:
            time_yes = time.strftime("%Y_%m_%d",time.localtime(time.time()-24*60*60))
            file_name = time_yes + strname
            str_head = "Analysis time:" + time.strftime( "%Y-%m-%d %X", time.localtime() ) + "\t" + time_yes + "_report:\n"
            str_head += str_con
            file_path = os.path.join(config.RESULTFILEPATH,file_name)
            with open(file_path,"a+") as file_fd:
                file_fd.write(str_head)
        except:
            info = sys.exc_info()
            traceback.extract_tb(info[2])
            log.error('write_result %s %s', info[0], info[1])


    def put_querywork(strippath):
        try:
            if os.path.exists(strippath):
                if os.path.isdir(strippath):
                    list_filepath = os.listdir(strippath)
                    #过滤日志
                    for filepath in list_filepath:
                        if os.path.splitext(filepath)[1] == ".gz":
                            str_filepath = os.path.join(strippath, filepath)
                            #添加到日志处理队列
                            tasks_workqueue.put(str_filepath)
                    return 0
        except Exception,ex:
            log.error("put_querywork ERROR:%s" %(ex))
        return -1

    def checkip(ip):
        p = re.compile('^(([01]?\d\d?|2[0-4]\d|25[0-5])\.){3}([01]?\d\d?|2[0-4]\d|25[0-5])$')
        if p.match(ip):
            return ip
        else:
            return ""

    def ip_into_int(ip):
        return reduce(lambda x,y:(x<<8)+y,map(int,ip.split('.')))

    def get_handlefilepath(config):
        list_ippath = []
        time_yes = time.strftime("%Y_%m_%d",time.localtime(time.time()-24*60*60))
        list_ipconfig = filter(lambda ip:ip,map(checkip,config.IPTABLE.split(";")))
        for strip in list_ipconfig:
            strpath = os.path.join(config.HANDLEFILEPATH,time_yes,strip)
            list_ippath.append(strpath)
        return list_ippath

    reload(sys)
    sys.setdefaultencoding('utf-8')

    #创建配置接口
    config = loadconfig()
    #创建日志接口
    log = loginit(config)

    view_name_list = []
    load_view_data(log,view_name_list)
    
    #中断信号处理
    signal.signal(signal.SIGQUIT, sigint_handler)
    signal.signal(signal.SIGINT, sigint_handler)
    signal.signal(signal.SIGHUP, sigint_handler)
    signal.signal(signal.SIGTERM, sigint_handler)
    is_sigint_up = False

    startwork()
