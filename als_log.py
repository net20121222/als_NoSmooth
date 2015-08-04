#!/usr/bin/python
#encoding=utf-8  
import logging  
import time  
import smtplib  
from email.mime.text import MIMEText  
import logging.handlers  
import os  
import sys  

#写入文件的日志等级，由于是详细信息，推荐设为debug  
FILE_LOG_LEVEL="DEBUG"  
#控制台的日照等级，info和warning都可以，可以按实际要求定制  
CONSOLE_LOG_LEVEL="INFO"  
#缓存日志等级，最好设为error或者critical  
MEMOEY_LOG_LEVEL="ERROR"  
#致命错误等级   
URGENT_LOG_LEVEL="CRITICAL"  
#缓存溢出后的邮件标题  
ERROR_THRESHOLD_ACHEIVED_MAIL_SUBJECT="ALS error"  
#缓存溢出的阀值  
ERROR_MESSAGE_THRESHOLD=5  
#致命错误发生后的邮件标题  
CRITICAL_ERROR_ACHEIVED_MAIL_SUBJECT="Fatal error occurred" 

#邮件服务器配置  
MAIL_HOST="mail.exclouds.com"
mail_user="miaolian"
mail_pass="yiyunmiaolian"
mail_postfix="exclouds.com"
FROM="miaolian"+"<"+mail_user+"@"+mail_postfix+">"
MAIL_TO=["miaolian@exclouds.com"]  
  
class OptmizedMemoryHandler(logging.handlers.MemoryHandler):  
    """ 
       由于自带的MemoryHandler达到阀值后，每一条缓存信息会单独处理一次，这样如果阀值设的100， 
      会发出100封邮件，这不是我们希望看到的，所以这里重写了memoryHandler的2个方法， 
      当达到阀值后，把缓存的错误信息通过一封邮件发出去. 
    """  
    def __init__(self, capacity,mail_subject):  
        logging.handlers.MemoryHandler.__init__(self, capacity,flushLevel=logging.ERROR, target=None)  
        self.mail_subject=mail_subject  
        self.flushed_buffers=[]  
    def shouldFlush(self, record):  
        """ 
        检查是否溢出 
        """  
        if len(self.buffer) >= self.capacity:  
            return True  
        else:  
            return False  
    def flush(self):  
        """ 
         缓存溢出时的操作， 
        1.发送邮件 2.清空缓存 3.把溢出的缓存存到另一个列表中，方便程序结束的时候读取所有错误并生成报告 
        """  
        if self.buffer!=[] and len(self.buffer) >= self.capacity:  
            content=""  
            try:
                for record in self.buffer:  
                    message= record.getMessage()  
                    level= record.levelname  
                    created= record.created
                    t=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(created))  
                    content+=t+" "+"*"+level+"* : "+message+"\n"  
                self.mailNotification(self.mail_subject, content)  
                self.flushed_buffers.extend(self.buffer)  
                self.buffer = [] 
            except:
                pass
    def mailNotification(self,subject,content):  
        """ 
                发邮件的方法 
        """  
        try:  
            msg=MIMEText(content)  
            msg['Subject']=subject  
            msg['From']=FROM  
            msg['To']=";".join(MAIL_TO)  
            s=smtplib.SMTP()  
            s.connect(MAIL_HOST)  
            s.login(mail_user,mail_pass)
            s.sendmail(FROM,MAIL_TO,msg.as_string())  
            s.close()  
        except Exception,e:  
            self.logger.error(str(e))  
  
              
MAPPING={"CRITICAL" :50,  
           "ERROR" : 40,  
           "WARNING" : 30,  
           "INFO" : 20,  
           "DEBUG" : 10,  
           "NOTSET" :0,  
           }  
  
class logger:  
    """ 
    logger的配置 
    """  
    def __init__(self,logFile,file_level=FILE_LOG_LEVEL,console_level=CONSOLE_LOG_LEVEL,memory_level=MEMOEY_LOG_LEVEL,urgent_level=URGENT_LOG_LEVEL):  
  
        logging.LogRecord.getMessage = self.print_log_record_on_error(logging.LogRecord.getMessage)
        self.config(logFile, file_level, console_level, memory_level,urgent_level)  
    def print_log_record_on_error(self, func):
        def wrap(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except:
                print >>sys.stderr, "Unable to create log message msg=%r, args=%r " % (getattr(self, 'msg', '?'), getattr(self, 'args', '?'))
        return wrap
    def config(self,logFile,file_level,console_level,memory_level,urgent_level):  
        #生成root logger  
        self.logger = logging.getLogger(logFile)  
        self.logger.setLevel(MAPPING[file_level])  
        #生成RotatingFileHandler，设置文件大小为10M,编码为utf-8，最大文件个数为10个，如果日志文件超过10，则会覆盖最早的日志  
        self.fh = logging.handlers.RotatingFileHandler(logFile,mode='a', maxBytes=1024*1024*10, backupCount=10)  
        self.fh.setLevel(MAPPING[file_level])  
        #生成StreamHandler  
        self.ch = logging.StreamHandler()  
        self.ch.setLevel(MAPPING[console_level])    
        #生成优化过的MemoryHandler,ERROR_MESSAGE_THRESHOLD是错误日志条数的阀值  
        self.mh = OptmizedMemoryHandler(ERROR_MESSAGE_THRESHOLD,ERROR_THRESHOLD_ACHEIVED_MAIL_SUBJECT)  
        self.mh.setLevel(MAPPING[memory_level])  
        #生成SMTPHandler  
        self.sh=logging.handlers.SMTPHandler(MAIL_HOST,FROM,";".join(MAIL_TO),CRITICAL_ERROR_ACHEIVED_MAIL_SUBJECT, credentials=(mail_user,mail_pass))  
        self.sh.setLevel(MAPPING[urgent_level])  
        #设置格式  
        formatter = logging.Formatter("%(asctime)s *%(levelname)s* : %(message)s",'%Y-%m-%d %H:%M:%S')    
        self.ch.setFormatter(formatter)    
        self.fh.setFormatter(formatter)  
        self.mh.setFormatter(formatter)  
        self.sh.setFormatter(formatter)  
        #把所有的handler添加到root logger中  
        self.logger.addHandler(self.ch)    
        self.logger.addHandler(self.fh)  
        self.logger.addHandler(self.mh)  
        self.logger.addHandler(self.sh)  
  
    def debug(self, format, *args):  
        try:
            if format is not None:  
                format += " " + "(" +sys._getframe().f_back.f_code.co_name + ":" + str(sys._getframe().f_back.f_lineno) + ")"
                self.logger.debug(format, *args)  
        except:
            pass
    def info(self, format, *args):  
        try:
            if format is not None:  
                format += " " + "(" +sys._getframe().f_back.f_code.co_name + ":" + str(sys._getframe().f_back.f_lineno) + ")"
                self.logger.info(format, *args)  
        except:
            pass
    def warning(self, format, *args):  
        try:
            if format is not None:  
                format += " " + "(" +sys._getframe().f_back.f_code.co_name + ":" + str(sys._getframe().f_back.f_lineno) + ")"
                self.logger.warning(format, *args)  
        except:
            pass
    def error(self, format, *args):  
        try:
            if format is not None:  
                format += " " + "(" +sys._getframe().f_back.f_code.co_name + ":" + str(sys._getframe().f_back.f_lineno) + ")"
                self.logger.error(format, *args)  
        except:
            pass
    def critical(self, format, *args):  
        try:
            if format is not None:  
                format += " " + "(" +sys._getframe().f_back.f_code.co_name + ":" + str(sys._getframe().f_back.f_lineno) + ")"
                self.logger.critical(format, *args)  
        except:
            pass

if __name__=="__main__":  
    logfile_path="test.log"
    LOG=logger(logfile_path)
    for i in range(15):  
        LOG.error('err')  
    #LOG.critical("Database has gone away")  
