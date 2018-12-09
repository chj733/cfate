#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
 ScriptName: qbench.py
 Author: chun.luo@woqutech.com
 Create Date: 2011-11-25 20:59
 Modify Author: chun.luo@woqutech.com
 Modify Date: 2013-08-15 19:09
 Function: qbench for oracle & mysql   
"""
__revision__ = '0.1'
import time
import sys
import os
import Queue
import random
import traceback
import multiprocessing
import logging
import getopt
import signal
import logging
import smtplib
import string
import pxssh
import pprint
import MySQLdb
import cx_Oracle
import ConfigParser

class qbench(multiprocessing.Process):
    def __init__(self,operation = None,dml_type = None,db_section = None):
        super(qbench,self).__init__();
        self.cf = ConfigParser.ConfigParser();
        self.cf.read("qbench.conf");
	self.gv_db_section 	  = db_section;
        self.gv_ip                = self.cf.get(self.gv_db_section,"ip");
        self.gv_user              = self.cf.get(self.gv_db_section,"user");
        self.gv_pwd               = self.cf.get(self.gv_db_section,"pwd");
        self.gv_port              = self.cf.getint(self.gv_db_section,"port");
        self.gv_dbtype            = self.cf.get(self.gv_db_section,"dbtype");
	if (self.gv_dbtype == 'oracle'):
	    self.gv_sid               = self.cf.get(self.gv_db_section,"sid");
	if (self.gv_dbtype == 'mysql'):
	    self.gv_dbname            = self.cf.get(self.gv_db_section,"dbname");
        self.operation            = operation;
        self.dml_type             = dml_type;
        self.gv_select_count      = 0;
        self.gv_insert_count      = 0;
        self.gv_update_count      = 0;
        self.gv_delete_count      = 0;
        self.gv_execute_count     = 0;
        self.process_id           = None;
        self.gv_member_count      = self.cf.getint(self.gv_db_section,"member");
        self.gv_offer_count       = self.cf.getint(self.gv_db_section,"offer");
        self.gv_logical_run_times = self.cf.getint(self.gv_db_section,"run_times");
        if (self.gv_dbtype == "mysql"):
            self.gv_conn = MySQLdb.Connection(host = self.gv_ip,user = self.gv_user,passwd = self.gv_pwd,\
	    db = self.gv_dbname,port = self.gv_port);
        self.gv_insert_sql = """insert into woqu_offer%s
        (MEMBERID,        
        OWNER,           
        OBJECT_NAME,     
        SUBOBJECT_NAME,  
        OBJECT_ID,       
        DATA_OBJECT_ID,  
        OBJECT_TYPE,     
        CREATED,         
        LAST_DDL_TIME,   
        TIMESTAMP,       
        STATUS,          
        TEMPORARY,       
        GENERATED,            
        SECONDARY,       
        NAMESPACE,       
        EDITION_NAME)  
        values
        (%s,%s,%s,%s,%s,%s,%s,now(),now(),%s,%s,%s,%s,%s,%s,%s)""";
        self.gv_delete_sql = "delete from woqu_offer%s where memberid = %s";
        self.gv_select_sql = """select offerid,memberid,LAST_DDL_TIME from woqu_offer%s 
            where memberid = %s order by offerid limit %s,%s""";
        self.gv_update_sql = "update woqu_offer%s set LAST_DDL_TIME = now(),DATA_OBJECT_ID = 9527 where offerid = %s";
        self.gv_login_sql = "select * from woqu_member where memberid = %s";
        self.gv_seek_prosql = "select * from woqu_offer%s where offerid = %s";
        self.gv_update_count_sql = """update woqu_count set select_rows = %s,insert_rows = %s,update_rows = %s,
                    delete_rows = %s,execute_rows = %s,end_time = now() where process_id = %s and db_desc = %s""";
        self.gv_init_member_sql = """insert into woqu_member(memberid,
        OWNER           ,
        OBJECT_NAME     ,
        SUBOBJECT_NAME  ,
        OBJECT_ID       ,
        DATA_OBJECT_ID  ,
        OBJECT_TYPE     ,
        CREATED         ,
        LAST_DDL_TIME   ,
        TIMESTAMP       ,
        STATUS          ,
        TEMPORARY       ,
        GENERATED       ,
        SECONDARY       ,
        NAMESPACE       ,
        EDITION_NAME )
        values
        (%s,%s,%s,%s,%s,%s,%s,now(),now(),%s,%s,%s,%s,%s,%s,%s)""";
        self.gv_init_offer_sql = """(MEMBERID,        
            OWNER,           
            OBJECT_NAME,     
            SUBOBJECT_NAME,  
            OBJECT_ID,       
            DATA_OBJECT_ID,  
            OBJECT_TYPE,     
            CREATED,         
            LAST_DDL_TIME,   
            TIMESTAMP,       
            STATUS,          
            TEMPORARY,       
            GENERATED,       
            SECONDARY,       
            NAMESPACE,       
            EDITION_NAME)  
            select MEMBERID,        
            OWNER,           
            OBJECT_NAME,     
            SUBOBJECT_NAME,  
            OBJECT_ID,       
            DATA_OBJECT_ID,  
            OBJECT_TYPE,     
            CREATED,         
            LAST_DDL_TIME,   
            TIMESTAMP,       
            STATUS,          
            TEMPORARY,       
            GENERATED,       
            SECONDARY,       
            NAMESPACE,       
            EDITION_NAME
            from woqu_member""";
        if (self.gv_dbtype == "oracle"):
            dsn_tns = cx_Oracle.makedsn(self.gv_ip,self.gv_port,self.gv_sid);
            self.gv_conn              = cx_Oracle.Connection(self.gv_user,self.gv_pwd,dsn_tns);
            self.gv_insert_sql = """insert into woqu_offer%s
        (offerid,MEMBERID,        
        OWNER,           
        OBJECT_NAME,     
        SUBOBJECT_NAME,  
        OBJECT_ID,       
        DATA_OBJECT_ID,  
        OBJECT_TYPE,     
        CREATED,         
        LAST_DDL_TIME,   
        TIMESTAMP,       
        STATUS,          
        TEMPORARY,       
        GENERATED,            
        SECONDARY,       
        NAMESPACE,       
        EDITION_NAME)  
        values
        (seq_woqu_offer.nextval,:v1,'%s','%s','%s',%s,%s,'%s',sysdate,sysdate,'%s','%s','%s','%s','%s',%s,'%s')""";
            self.gv_delete_sql = "delete from woqu_offer%s where memberid = :v1";
            self.gv_select_sql = """select b.* from (select a.*,rownum as rn from (select offerid,memberid,LAST_DDL_TIME from 
	    woqu_offer%s where memberid = :v1 order by offerid) a where rownum <= :v2) b where b.rn >= :v3 """;
            self.gv_update_sql = "update woqu_offer%s set LAST_DDL_TIME = sysdate,DATA_OBJECT_ID = 9527 where offerid = :v1";
            self.gv_login_sql = "select * from woqu_member where memberid = :v1";
            self.gv_seek_prosql = "select * from woqu_offer%s where offerid = :v1";
            self.gv_update_count_sql = """update woqu_count set select_rows = :v1,insert_rows = :v2,update_rows = :v3,
                    delete_rows = :v4,execute_rows = :v5,end_time = sysdate where process_id = :v6 and db_desc = :v7""";
            self.gv_init_member_sql = """insert into woqu_member(memberid,
        OWNER           ,
        OBJECT_NAME     ,
        SUBOBJECT_NAME  ,
        OBJECT_ID       ,
        DATA_OBJECT_ID  ,
        OBJECT_TYPE     ,
        CREATED         ,
        LAST_DDL_TIME   ,
        TIMESTAMP       ,
        STATUS          ,
        TEMPORARY       ,
        GENERATED       ,
        SECONDARY       ,
        NAMESPACE       ,
        EDITION_NAME )
        values
        (:v1,'%s','%s','%s',%s,%s,'%s',sysdate,sysdate,'%s','%s','%s','%s','%s',%s,'%s')""";
            self.gv_init_offer_sql = """(offerid,MEMBERID,        
            OWNER,           
            OBJECT_NAME,     
            SUBOBJECT_NAME,  
            OBJECT_ID,       
            DATA_OBJECT_ID,  
            OBJECT_TYPE,     
            CREATED,         
            LAST_DDL_TIME,   
            TIMESTAMP,       
            STATUS,          
            TEMPORARY,       
            GENERATED,       
            SECONDARY,       
            NAMESPACE,       
            EDITION_NAME)  
            select /*+ parallel(a,8) */seq_woqu_offer.nextval,MEMBERID,        
            OWNER,           
            OBJECT_NAME,     
            SUBOBJECT_NAME,  
            OBJECT_ID,       
            DATA_OBJECT_ID,  
            OBJECT_TYPE,     
            CREATED,         
            LAST_DDL_TIME,   
            TIMESTAMP,       
            STATUS,          
            TEMPORARY,       
            GENERATED,       
            SECONDARY,       
            NAMESPACE,       
            EDITION_NAME
            from woqu_member a""";
        self.gv_cur               = self.gv_conn.cursor();
        
    def woqu_create_ddl(self):
        if (self.gv_dbtype == "mysql"):
            try:
                self.gv_cur.execute("""create table IF NOT EXISTS woqu_count(
                id bigint unsigned auto_increment primary key,
                db_desc varchar(128),
                process_id int,
                select_rows int,
                insert_rows int,
                update_rows int,
                delete_rows int,
                execute_rows int,
                start_time datetime,
                end_time datetime
            ) ENGINE=InnoDB""");
                self.gv_cur.execute("""CREATE TABLE IF NOT EXISTS woqu_member (
          MEMBERID bigint(20) unsigned NOT NULL AUTO_INCREMENT,
          OWNER varchar(30) DEFAULT NULL,
          OBJECT_NAME varchar(128) DEFAULT NULL,
          SUBOBJECT_NAME varchar(30) DEFAULT NULL,
          OBJECT_ID int(11) DEFAULT NULL,
          DATA_OBJECT_ID int(11) DEFAULT NULL,
          OBJECT_TYPE varchar(19) DEFAULT NULL,
          CREATED datetime DEFAULT NULL,
          LAST_DDL_TIME datetime DEFAULT NULL,
          TIMESTAMP varchar(19) DEFAULT NULL,
          STATUS varchar(7) DEFAULT NULL,
          TEMPORARY varchar(1) DEFAULT NULL,
          GENERATED varchar(1) DEFAULT NULL,
          SECONDARY varchar(1) DEFAULT NULL,
          NAMESPACE int(11) DEFAULT NULL,
          EDITION_NAME varchar(512) DEFAULT NULL,
          PRIMARY KEY (MEMBERID)
        ) ENGINE=InnoDB"""
                );
                self.gv_cur.execute("""CREATE TABLE IF NOT EXISTS woqu_offer1 (
          OFFERID bigint(20) unsigned NOT NULL AUTO_INCREMENT,
          MEMBERID bigint(20) DEFAULT NULL,
          OWNER varchar(30) DEFAULT NULL,
          OBJECT_NAME varchar(128) DEFAULT NULL,
          SUBOBJECT_NAME varchar(30) DEFAULT NULL,
          OBJECT_ID int(11) DEFAULT NULL,
          DATA_OBJECT_ID int(11) DEFAULT NULL,
          OBJECT_TYPE varchar(19) DEFAULT NULL,
          CREATED datetime DEFAULT NULL,
          LAST_DDL_TIME datetime DEFAULT NULL,
          TIMESTAMP varchar(19) DEFAULT NULL,
          STATUS varchar(7) DEFAULT NULL,
          TEMPORARY varchar(1) DEFAULT NULL,
          GENERATED varchar(1) DEFAULT NULL,
          SECONDARY varchar(1) DEFAULT NULL,
          NAMESPACE int(11) DEFAULT NULL,
          EDITION_NAME varchar(513) DEFAULT NULL,
          PRIMARY KEY (offerid),
          KEY ind_offer_mid (MEMBERID)
        ) ENGINE=InnoDB"""
                );
                self.gv_cur.execute("""CREATE TABLE IF NOT EXISTS woqu_offer2 (
          OFFERID bigint(20) unsigned NOT NULL AUTO_INCREMENT,
          MEMBERID bigint(20) DEFAULT NULL,
          OWNER varchar(30) DEFAULT NULL,
          OBJECT_NAME varchar(128) DEFAULT NULL,
          SUBOBJECT_NAME varchar(30) DEFAULT NULL,
          OBJECT_ID int(11) DEFAULT NULL,
          DATA_OBJECT_ID int(11) DEFAULT NULL,
          OBJECT_TYPE varchar(19) DEFAULT NULL,
          CREATED datetime DEFAULT NULL,
          LAST_DDL_TIME datetime DEFAULT NULL,
          TIMESTAMP varchar(19) DEFAULT NULL,
          STATUS varchar(7) DEFAULT NULL,
          TEMPORARY varchar(1) DEFAULT NULL,
          GENERATED varchar(1) DEFAULT NULL,
          SECONDARY varchar(1) DEFAULT NULL,
          NAMESPACE int(11) DEFAULT NULL,
          EDITION_NAME varchar(513) DEFAULT NULL,
          PRIMARY KEY (offerid),
          KEY ind_offer_mid (MEMBERID)
        ) ENGINE=InnoDB"""
                );
                self.gv_cur.execute("""CREATE TABLE IF NOT EXISTS woqu_offer3 (
          OFFERID bigint(20) unsigned NOT NULL AUTO_INCREMENT,
          MEMBERID bigint(20) DEFAULT NULL,
          OWNER varchar(30) DEFAULT NULL,
          OBJECT_NAME varchar(128) DEFAULT NULL,
          SUBOBJECT_NAME varchar(30) DEFAULT NULL,
          OBJECT_ID int(11) DEFAULT NULL,
          DATA_OBJECT_ID int(11) DEFAULT NULL,
          OBJECT_TYPE varchar(19) DEFAULT NULL,
          CREATED datetime DEFAULT NULL,
          LAST_DDL_TIME datetime DEFAULT NULL,
          TIMESTAMP varchar(19) DEFAULT NULL,
          STATUS varchar(7) DEFAULT NULL,
          TEMPORARY varchar(1) DEFAULT NULL,
          GENERATED varchar(1) DEFAULT NULL,
          SECONDARY varchar(1) DEFAULT NULL,
          NAMESPACE int(11) DEFAULT NULL,
          EDITION_NAME varchar(513) DEFAULT NULL,
          PRIMARY KEY (offerid),
          KEY ind_offer_mid (MEMBERID)
        ) ENGINE=InnoDB"""
                );
                self.gv_cur.execute("""CREATE TABLE IF NOT EXISTS woqu_offer4 (
          OFFERID bigint(20) unsigned NOT NULL AUTO_INCREMENT,
          MEMBERID bigint(20) DEFAULT NULL,
          OWNER varchar(30) DEFAULT NULL,
          OBJECT_NAME varchar(128) DEFAULT NULL,
          SUBOBJECT_NAME varchar(30) DEFAULT NULL,
          OBJECT_ID int(11) DEFAULT NULL,
          DATA_OBJECT_ID int(11) DEFAULT NULL,
          OBJECT_TYPE varchar(19) DEFAULT NULL,
          CREATED datetime DEFAULT NULL,
          LAST_DDL_TIME datetime DEFAULT NULL,
          TIMESTAMP varchar(19) DEFAULT NULL,
          STATUS varchar(7) DEFAULT NULL,
          TEMPORARY varchar(1) DEFAULT NULL,
          GENERATED varchar(1) DEFAULT NULL,
          SECONDARY varchar(1) DEFAULT NULL,
          NAMESPACE int(11) DEFAULT NULL,
          EDITION_NAME varchar(513) DEFAULT NULL,
          PRIMARY KEY (offerid),
          KEY ind_offer_mid (MEMBERID)
        ) ENGINE=InnoDB"""
                );
        
            except Exception,e:
                print "woqu_create_ddl error %s"%(str(e)); 
            
        if (self.gv_dbtype == "oracle"):
            try:
                self.gv_cur.execute("create sequence seq_woqu_offer cache 10000");
                self.gv_cur.execute("""create table woqu_count(
                id number,
		db_desc varchar2(128),
                process_id number,
                select_rows number,
                insert_rows number,
                update_rows number,
                delete_rows number,
                execute_rows number,
                start_time date,
                end_time date
                )""");
                self.gv_cur.execute("""CREATE TABLE woqu_member (
          MEMBERID number,
          OWNER varchar2(30) ,
          OBJECT_NAME varchar2(128) ,
          SUBOBJECT_NAME varchar2(30) ,
          OBJECT_ID number ,
          DATA_OBJECT_ID number ,
          OBJECT_TYPE varchar2(19) ,
          CREATED date ,
          LAST_DDL_TIME date ,
          TIMESTAMP varchar2(19) ,
          STATUS varchar2(7) ,
          TEMPORARY varchar2(1) ,
          GENERATED varchar2(1) ,
          SECONDARY varchar2(1) ,
          NAMESPACE number ,
          EDITION_NAME varchar2(512) 
        )"""
                );
                self.gv_cur.execute("""CREATE TABLE  woqu_offer1 (
          OFFERID number,
          MEMBERID number ,
          OWNER varchar2(30) ,
          OBJECT_NAME varchar2(128) ,
          SUBOBJECT_NAME varchar2(30) ,
          OBJECT_ID number ,
          DATA_OBJECT_ID number ,
          OBJECT_TYPE varchar2(19) ,
          CREATED date ,
          LAST_DDL_TIME date ,
          TIMESTAMP varchar2(19) ,
          STATUS varchar2(7) ,
          TEMPORARY varchar2(1) ,
          GENERATED varchar2(1) ,
          SECONDARY varchar2(1) ,
          NAMESPACE number ,
          EDITION_NAME varchar2(513) 
        ) """
                );
                self.gv_cur.execute("""CREATE TABLE woqu_offer2 (
          OFFERID number,
          MEMBERID number ,
          OWNER varchar2(30) ,
          OBJECT_NAME varchar2(128) ,
          SUBOBJECT_NAME varchar2(30) ,
          OBJECT_ID number ,
          DATA_OBJECT_ID number ,
          OBJECT_TYPE varchar2(19) ,
          CREATED date ,
          LAST_DDL_TIME date ,
          TIMESTAMP varchar2(19) ,
          STATUS varchar2(7) ,
          TEMPORARY varchar2(1) ,
          GENERATED varchar2(1) ,
          SECONDARY varchar2(1) ,
          NAMESPACE number ,
          EDITION_NAME varchar2(513) 
        ) """
                );
                self.gv_cur.execute("""CREATE TABLE woqu_offer3 (
          OFFERID number,
          MEMBERID number ,
          OWNER varchar2(30) ,
          OBJECT_NAME varchar2(128) ,
          SUBOBJECT_NAME varchar2(30) ,
          OBJECT_ID number ,
          DATA_OBJECT_ID number ,
          OBJECT_TYPE varchar2(19) ,
          CREATED date ,
          LAST_DDL_TIME date ,
          TIMESTAMP varchar2(19) ,
          STATUS varchar2(7) ,
          TEMPORARY varchar2(1) ,
          GENERATED varchar2(1) ,
          SECONDARY varchar2(1) ,
          NAMESPACE number ,
          EDITION_NAME varchar2(513) 
        ) """
                );
                self.gv_cur.execute("""CREATE TABLE woqu_offer4 (
          OFFERID number,
          MEMBERID number ,
          OWNER varchar2(30) ,
          OBJECT_NAME varchar2(128) ,
          SUBOBJECT_NAME varchar2(30) ,
          OBJECT_ID number ,
          DATA_OBJECT_ID number ,
          OBJECT_TYPE varchar2(19) ,
          CREATED date ,
          LAST_DDL_TIME date ,
          TIMESTAMP varchar2(19) ,
          STATUS varchar2(7) ,
          TEMPORARY varchar2(1) ,
          GENERATED varchar2(1) ,
          SECONDARY varchar2(1) ,
          NAMESPACE number ,
          EDITION_NAME varchar2(513) 
        ) """
                );
            
                self.gv_cur.execute("""alter table woqu_offer4 parallel 8""");
                self.gv_cur.execute("""alter table woqu_offer3 parallel 8""");
                self.gv_cur.execute("""alter table woqu_offer2 parallel 8""");
                self.gv_cur.execute("""alter table woqu_offer1 parallel 8""");
        
            except Exception,e:
                print "woqu_create_ddl error %s"%(str(e)); 

    #设置processid
    def woqu_process(self,process_id):
        if (self.gv_dbtype == "mysql"):
            try:
                self.process_id = process_id;
                self.gv_cur.execute("insert into woqu_count(db_desc,process_id,start_time) values ('%s',%s,now())"%\
		(self.gv_ip + ":" + str(self.gv_port) + ":" + self.gv_dbname,self.process_id));
                self.gv_conn.commit();
            except Exception,e:
                print "woqu_process error %s"%(str(e));
    
        if (self.gv_dbtype == "oracle"):
            try:
                self.process_id = process_id;
                self.gv_cur.execute("insert into woqu_count(db_desc,process_id,start_time) values (:v1,:v2,sysdate)",\
	        v1 = self.gv_ip + ":" + str(self.gv_port) + ":" + self.gv_sid,v2 = self.process_id);
                self.gv_conn.commit();
            except Exception,e:
                print "woqu_process error %s"%(str(e));        
    
    
    #登录
    def woqu_login(self,memberid):
        if (self.gv_dbtype == "mysql"):
            try:
                param = (memberid);
                self.gv_select_count = self.gv_select_count + self.gv_cur.execute(self.gv_login_sql,param);
                self.gv_cur.fetchall();
                self.gv_execute_count = self.gv_execute_count + 1;
            except Exception,e:
                print "woqu_login error %s"%(str(e)); 
    
        if (self.gv_dbtype == "oracle"):
            try:
                self.gv_cur.execute(self.gv_login_sql,v1 = memberid);
                self.gv_select_count = self.gv_select_count + self.gv_cur.rowcount;
                self.gv_cur.fetchall();
                self.gv_execute_count = self.gv_execute_count + 1;
            except Exception,e:
                print "woqu_login error %s"%(str(e)); 
                 
    #分页浏览         
    def woqu_list_product(self,memberid):
        if (self.gv_dbtype == "mysql"):
            try:
                table_id = self.process_id%4 + 1;
                param = (table_id,memberid,1,10);
                self.gv_select_count = self.gv_select_count + self.gv_cur.execute(self.gv_select_sql,param);
                rows = self.gv_cur.fetchall();
                for col1 in rows:
                    self.woqu_seek_product(col1[0]);
            
                param = (table_id,memberid,11,20);
                self.gv_select_count = self.gv_select_count + self.gv_cur.execute(self.gv_select_sql,param);
                rows = self.gv_cur.fetchall();
                for col1 in rows:
                    self.woqu_seek_product(col1[0]);
            
                self.gv_execute_count = self.gv_execute_count + 2;
            except Exception,e:
                print "woqu_list_product error %s"%(str(e)); 
            
        if (self.gv_dbtype == "oracle"):
            try:
                table_id = self.process_id%4 + 1;
                self.gv_cur.execute(self.gv_select_sql%(table_id),v1 = memberid,v2 = 1,v3 = 10);
                self.gv_select_count = self.gv_select_count + self.gv_cur.rowcount;
                rows = self.gv_cur.fetchall();
                for col1 in rows:
                    self.woqu_seek_product(col1[0]);
            
                self.gv_cur.execute(self.gv_select_sql%(table_id),v1 = memberid,v2 = 11,v3 = 20);
                self.gv_select_count = self.gv_select_count + self.gv_cur.rowcount;
                rows = self.gv_cur.fetchall();
                for col1 in rows:
                    self.woqu_seek_product(col1[0]);
            
                self.gv_execute_count = self.gv_execute_count + 2;
            except Exception,e:
                print "woqu_list_product error %s"%(str(e)); 
   
    #seek offer
    def woqu_seek_product(self,offerid):
        if (self.gv_dbtype == "mysql"):
            try:
                table_id = self.process_id%4 + 1;
                param = (table_id,offerid);
                self.gv_select_count = self.gv_select_count + self.gv_cur.execute(self.gv_seek_prosql,param);
                self.gv_cur.fetchall();
    
                self.gv_execute_count = self.gv_execute_count + 1;
            except Exception,e:
                print "woqu_seek_product error %s"%(str(e)); 
            
        if (self.gv_dbtype == "oracle"):
            try:
                table_id = self.process_id%4 + 1;
                param = (table_id,offerid);
                self.gv_cur.execute(self.gv_seek_prosql%(table_id),v1 = offerid);
                self.gv_select_count = self.gv_select_count + self.gv_cur.rowcount;
                self.gv_cur.fetchall();
    
                self.gv_execute_count = self.gv_execute_count + 1;
            except Exception,e:
                print "woqu_seek_product error %s"%(str(e)); 
        
    
    #更新产品            
    def woqu_update_product(self,memberid):
        if (self.gv_dbtype == "mysql"): 
            try:
                table_id = self.process_id%4 + 1;
                param = (table_id,memberid);
                self.gv_select_count = self.gv_select_count + self.gv_cur.execute\
		("select offerid from woqu_offer%s where memberid = %s",param);
                rows = self.gv_cur.fetchall();
                for col1 in rows:
                    self.gv_update_count = self.gv_update_count + self.gv_cur.execute(self.gv_update_sql,(table_id,col1[0]));
                    self.gv_execute_count = self.gv_execute_count + 1;
    
                self.gv_conn.commit();
            
                self.gv_execute_count = self.gv_execute_count + 1;
            except Exception,e:
                self.gv_conn.rollback();
                print "woqu_update_product error %s"%(str(e)); 
            
        if (self.gv_dbtype == "oracle"):         
            try:
                table_id = self.process_id%4 + 1;
                param = (table_id,memberid);
                self.gv_cur.execute("select offerid from woqu_offer%s where memberid = :v1"%(table_id),v1 = memberid);
                self.gv_select_count = self.gv_select_count + self.gv_cur.rowcount;
                rows = self.gv_cur.fetchall();
                for col1 in rows:
                    self.gv_cur.execute(self.gv_update_sql%(table_id),v1 = col1[0]); 
                    self.gv_update_count = self.gv_update_count + self.gv_cur.rowcount;
                    self.gv_execute_count = self.gv_execute_count + 1;
    
                self.gv_conn.commit();
            
                self.gv_execute_count = self.gv_execute_count + 1;
            except Exception,e:
                self.gv_conn.rollback();
                print "woqu_update_product error %s"%(str(e)); 
                
    #发布产品           
    def woqu_insert_product(self,memberid):
        if (self.gv_dbtype == "mysql"):   
            try:
                table_id = self.process_id%4 + 1;
                param = (table_id,memberid,'io press','smartIO','smart PL/SQL',9527,9527,\
		'X-DB.com','dbkong','new','n','n','y',9876,'ft is dead!!!');
                self.gv_insert_count = self.gv_insert_count + self.gv_cur.execute(self.gv_insert_sql,param);
                self.gv_conn.commit();
                
                self.gv_execute_count = self.gv_execute_count + 1;
            except Exception,e:
                self.gv_conn.rollback();
                print "woqu_insert_product error %s"%(str(e)); 
            
        if (self.gv_dbtype == "oracle"):   
            try:
                table_id = self.process_id%4 + 1;
                self.gv_cur.execute(self.gv_insert_sql%(table_id,'io press','smartIO','smart PL/SQL',\
		9527,9527,'X-DB.com','dbkong','new','n','n','y',9876,'ft is dead!!!'),v1 = memberid);
                self.gv_insert_count = self.gv_insert_count + self.gv_cur.rowcount;
                self.gv_conn.commit();
                
                self.gv_execute_count = self.gv_execute_count + 1;
            except Exception,e:
                self.gv_conn.rollback();
                print "woqu_insert_product error %s"%(str(e));  
    
    #删除产品          
    def woqu_delete_product(self,memberid):
        if (self.gv_dbtype == "mysql"):
            try:
                table_id = self.process_id%4 + 1;
                param = (table_id,memberid);
                self.gv_delete_count = self.gv_delete_count + self.gv_cur.execute(self.gv_delete_sql,param);
                self.gv_conn.commit();
            
                self.gv_execute_count = self.gv_execute_count + 1;
            except Exception,e:
                self.gv_conn.rollback();
                print "woqu_delete_product error %s"%(str(e)); 
            
        if (self.gv_dbtype == "oracle"):
            try:
                table_id = self.process_id%4 + 1;
                self.gv_cur.execute(self.gv_delete_sql%(table_id),v1 = memberid);
                self.gv_delete_count = self.gv_delete_count + self.gv_cur.rowcount;
                self.gv_conn.commit();
            
                self.gv_execute_count = self.gv_execute_count + 1;
            except Exception,e:
                self.gv_conn.rollback();
                print "woqu_delete_product error %s"%(str(e)); 
                
    #注销退出
    def woqu_logoff(self):
        if (self.gv_dbtype == "mysql"):
            try:
                param = (self.gv_select_count,self.gv_insert_count,self.gv_update_count,self.gv_delete_count,\
		self.gv_execute_count,self.process_id,self.gv_ip + ":" + str(self.gv_port) + ":" + self.gv_dbname);
                self.gv_cur.execute(self.gv_update_count_sql,param);
                self.gv_conn.commit();
    
                self.gv_cur.close();
                self.gv_conn.close();
            
            except Exception,e:
                print "woqu_logoff error %s"%(str(e)); 
            
        if (self.gv_dbtype == "oracle"):
            try:
                self.gv_cur.execute(self.gv_update_count_sql,v1 = self.gv_select_count,\
                v2 = self.gv_insert_count,\
                v3 = self.gv_update_count,\
                v4 = self.gv_delete_count,\
                v5 = self.gv_execute_count,\
                v6 = self.process_id,\
		v7 = self.gv_ip + ":" + str(self.gv_port) + ":" + self.gv_sid);

            
                self.gv_conn.commit();
        
                self.gv_cur.close();
                self.gv_conn.close();
            
            except Exception,e:
                print "woqu_logoff error %s"%(str(e)); 
        
    #清空member
    def woqu_clear_member(self):
        try:
            self.gv_cur.execute("truncate table woqu_member");
        except Exception,e:
            print "woqu_clear_member_error %s"%(str(e));

    #初始化数据    
    def woqu_init_member(self):
        if (self.gv_dbtype == "mysql"):
            try:
                lv_count = 0;
                lv_memberid_start = int((self.process_id - 1) * self.gv_member_count / 4) + 1;
                lv_memberid_end = int(self.process_id * self.gv_member_count / 4) - 1;
                for i in xrange(lv_memberid_start,lv_memberid_end):
                    param = (i,'io press','smartIO','smart PL/SQL',9527,9527,\
                         'X-DB.com','dbkong','new','n','n','y',9876,'ft is dead!!!');
                    self.gv_cur.execute(self.gv_init_member_sql,param);
                    lv_count = lv_count + 1;
                    if (lv_count == 10000):
                        self.gv_conn.commit();
                        lv_count = 0;
                self.gv_conn.commit();
    
            except Exception,e:
                print "woqu_init_member error %s"%(str(e));
            
        if (self.gv_dbtype == "oracle"):
            try:
                lv_count = 0;
                lv_memberid_start = int((self.process_id - 1) * self.gv_member_count / 4) + 1;
                lv_memberid_end = int(self.process_id * self.gv_member_count / 4) - 1;
                for i in xrange(lv_memberid_start,lv_memberid_end):
                    self.gv_cur.execute(self.gv_init_member_sql%('io press','smartIO','smart PL/SQL',\
		    9527,9527,'X-DB.com','dbkong','new','n','n','y',9876,'ft is dead!!!'),v1 = i);
                    lv_count = lv_count + 1;
                    if (lv_count == 10000):
                        self.gv_conn.commit();
                        lv_count = 0;
                self.gv_conn.commit();

            except Exception,e:
                print "woqu_init_member error %s"%(str(e));
           
    def woqu_member_index(self):
	if (self.gv_dbtype == "oracle"):
            try:
                self.gv_cur.execute("create index ind_memberid on woqu_member(memberid) parallel 8");
                self.gv_cur.execute("alter index ind_memberid parallel 1");
                self.gv_cur.execute("alter table woqu_member parallel 1");
                self.gv_cur.execute("analyze table woqu_member estimate statistics");
            except Exception,e:
                print "woqu_member_index error %s"%(str(e));

    def woqu_drop_qbench(self):                                                                                                
        try:    
            if (self.gv_dbtype == "oracle"):
		self.gv_cur.execute("drop sequence seq_woqu_offer");
		self.gv_cur.execute("drop table woqu_count");
                self.gv_cur.execute("drop table woqu_member");                               
                self.gv_cur.execute("drop table woqu_offer1");                                                         
                self.gv_cur.execute("drop table woqu_offer2");                                                          
                self.gv_cur.execute("drop table woqu_offer3");
                self.gv_cur.execute("drop table woqu_offer4");
            if (self.gv_dbtype == "mysql"):
		self.gv_cur.execute("drop table woqu_count");
                self.gv_cur.execute("drop table woqu_member");                               
                self.gv_cur.execute("drop table woqu_offer1");                                                         
                self.gv_cur.execute("drop table woqu_offer2");                                                          
                self.gv_cur.execute("drop table woqu_offer3");
                self.gv_cur.execute("drop table woqu_offer4");
        except Exception,e:                                                                                                     
            print "woqu_drop_qbench error %s"%(str(e)); 


    def woqu_init_offer(self):
        if (self.gv_dbtype == "mysql"):
            try:
                lv_process_count = self.gv_offer_count / self.gv_member_count / 4;
                self.gv_cur.execute("truncate table woqu_offer" + str(self.process_id));
                sql = """insert into woqu_offer""" + str(self.process_id) + self.gv_init_offer_sql;
          
                for i in xrange(0,lv_process_count):
                    self.gv_cur.execute(sql);
                    self.gv_conn.commit();
    
            except Exception,e:
                print "woqu_init_offer error %s"%(str(e));
            
        if (self.gv_dbtype == "oracle"):
            try:
                lv_process_count = self.gv_offer_count / self.gv_member_count / 4;
                self.gv_cur.execute("alter session set db_file_multiblock_read_count = 128");
                self.gv_cur.execute("alter session enable parallel dml");
                self.gv_cur.execute("truncate table woqu_offer" + str(self.process_id));
                sql = """insert /*+ append */into woqu_offer""" + str(self.process_id) + self.gv_init_offer_sql;
          
                for i in xrange(0,lv_process_count):
                    self.gv_cur.execute(sql);
                    self.gv_conn.commit();
                
                self.gv_cur.execute("create index ind_offer%s_offerid on woqu_offer%s(offerid) parallel 8"%\
		(self.process_id,self.process_id));
                self.gv_cur.execute("create index ind_offer%s_memberid on woqu_offer%s(memberid) parallel 8"%\
		(self.process_id,self.process_id));
                self.gv_cur.execute("alter index ind_offer%s_offerid  parallel 1"%(self.process_id));
                self.gv_cur.execute("alter index ind_offer%s_memberid  parallel 1"%(self.process_id));
                self.gv_cur.execute("alter table woqu_offer%s parallel 1"%(self.process_id));
                self.gv_cur.execute("analyze table woqu_offer%s estimate statistics"%(self.process_id));
            
            except Exception,e:
                print "woqu_init_offer error %s"%(str(e));

    def run(self):
        try:
            if (self.operation == 'initmember'):
                self.woqu_init_member();
            if (self.operation == 'initoffer'):
                self.woqu_init_offer();
            if (self.operation == 'dropqb'):
                self.woqu_drop_qbench();
            if (self.operation == 'press'):
                random.seed();
                for i in xrange(0,self.gv_logical_run_times):
                    #随机memberid生成
                    memberid = random.randint(1, self.gv_member_count);
                    #操作，更新，删除，插入，查询
                    if (self.dml_type == 'u'):
                        self.woqu_update_product(memberid);
                    elif (self.dml_type == 'i'):
                        self.woqu_insert_product(memberid);
                    elif (self.dml_type == 'd'):
                        self.woqu_delete_product(memberid);
                    elif (self.dml_type == 'r'):
                        self.woqu_login(memberid);
                        self.woqu_list_product(memberid);

		    #1/10的业务，不产生压力
                    if (memberid > self.gv_member_count - self.gv_member_count/10):
                        time.sleep(1);
                    
                #注销退出
                self.woqu_logoff();    
                     
        except Exception:
            traceback.print_exc();

    def clear_woqu_count(self):
        try:
	    if (self.gv_dbtype == "mysql"):
            	self.gv_cur.execute("delete from woqu_count where db_desc = '%s'"%\
	    	(self.gv_ip + ":" + str(self.gv_port) + ":" + self.gv_dbname));
	    if (self.gv_dbtype == "oracle"):
		self.gv_cur.execute("delete from woqu_count where db_desc = :v1",\
                v1 = self.gv_ip + ":" + str(self.gv_port) + ":" + self.gv_sid);

	    self.gv_conn.commit();

        except Exception:
            traceback.print_exc();

def handler(signum, frame):
    try:
        #释放数据库连接
        for i in xrange(0,len(process_list)):
            if (process_list[i].gv_conn):
                process_list[i].gv_conn.close();        
    except Exception,e:
        print e;
    finally:
        print "qbench exit!!!";
        os._exit(0);

def woqu_qbench_usage():
    print("""**********************************************
*writed by chun.luo@woqutech.com             *
*usage:                                      *
*create ddl:    qbench.py initddl            *
*init member:   qbench.py initmember         *
*init offer:    qbench.py initoffer          *
*begin test:    qbench.py press              *
*drop qbtables: qbench.py dropqb             *
**********************************************""");

gv_process_list = [];
gv_cf = ConfigParser.ConfigParser();
                  
if __name__ == "__main__":
    signal.signal(signal.SIGTERM,handler);
    signal.signal(signal.SIGQUIT,handler);
    signal.signal(signal.SIGINT,handler);
    
    #读取qbench.conf
    try:
	gv_cf.read("qbench.conf");
    except Exception,e:
	print "read qbench.conf get error!";
	os._exit(0);

    if (len(sys.argv) != 2):
        woqu_qbench_usage();
    else:
	if (sys.argv[1] not in ('initddl','dropqb','initmember','initoffer','press')):
	    woqu_qbench_usage();
	    os._exit(0);
	
        for db_section in gv_cf.sections():
	    try:
                #创建表结构
                if sys.argv[1] == 'initddl':
                    pt = qbench(sys.argv[1],'n',db_section);
                    pt.woqu_create_ddl();

                #删除qbench表结构
                elif sys.argv[1] == 'dropqb':
                    pt = qbench(sys.argv[1],'n',db_section);
                    pt.woqu_drop_qbench();

                #初始化member
                elif sys.argv[1] == 'initmember':
                    pt = qbench(sys.argv[1],'n',db_section);
       	            my_process_list = [];
                    pt.woqu_clear_member();

                    for i in xrange(1,5):
                        p = qbench(sys.argv[1],'n',db_section);
                        p.woqu_process(i);
                        p.start();
                        my_process_list.append(p);
                    
                    for t in my_process_list:
                        t.join();
                    
                    #创建索引，分析对象
                    pt.woqu_member_index();
                            
                #初始化offer     
                elif sys.argv[1] == 'initoffer':
                    for i in xrange(1,5):
                        p = qbench(sys.argv[1],'n',db_section);
                        p.woqu_process(i);
                        p.start();
                        gv_process_list.append(p);
                    
                #压力测试      
                elif sys.argv[1] == 'press':
                    #clear count
                    pt = qbench(sys.argv[1],'n',db_section);
                    pt.clear_woqu_count();

                    #get process count
                    ev_select_count = gv_cf.getint(db_section,"select_process_count") ;
                    ev_delete_count = gv_cf.getint(db_section,"delete_process_count") ;
                    ev_update_count = gv_cf.getint(db_section,"update_process_count") ;
                    ev_insert_count = gv_cf.getint(db_section,"insert_process_count") ;

                    for i in xrange(1,ev_select_count + 1):
                        p = qbench(sys.argv[1],'r',db_section);
                        p.woqu_process(i);
                        p.start();
                        gv_process_list.append(p);
                    
                    #delete
                    #ev_delete_count = ...
                    for i in xrange(1,ev_delete_count + 1):
                        p = qbench(sys.argv[1],'d',db_section);
                        p.woqu_process(i);
                        p.start();
                        gv_process_list.append(p);
                        
                    #update
                    #ev_update_count = ...
                    for i in xrange(1,ev_update_count + 1):
                        p = qbench(sys.argv[1],'u',db_section);
                        p.woqu_process(i);
                        p.start();
                        gv_process_list.append(p);
                        
                    #insert
                    #ev_insert_count = ...
                    for i in xrange(1,ev_insert_count + 1):
                        p = qbench(sys.argv[1],'i',db_section);
                        p.woqu_process(i);
                        p.start();
                        gv_process_list.append(p);
	    except Exception,e:
		if (gv_cf.get(self.gv_db_section,"dbtype") == "mysql"):
		    print gv_cf.get(self.gv_db_section,"ip") + ":" + str(gv_cf.get(self.gv_db_section,"port")) + ":" + \
		    gv_cf.get(self.gv_db_section,"dbname") + " get error!!!";
		    print e;

		if (gv_cf.get(self.gv_db_section,"dbtype") == "oracle"):
                    print gv_cf.get(self.gv_db_section,"ip") + ":" + str(gv_cf.get(self.gv_db_section,"port")) + ":" + \
                    gv_cf.get(self.gv_db_section,"dbsid") + " get error!!!";
                    print e;
        #回收所有线程        
        for gt in gv_process_list:
            gt.join();
