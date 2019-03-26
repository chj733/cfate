#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
 ScriptName: q_data_checkX.py
 Author: chun.luo@woqutech.com
 Create Date: 2019-3-2 22:40
 Modify Author: chun.luo@woqutech.com
 Modify Date: 2019-3-2 22:40
 Remark: 数据对比，是真需求，还是伪需求？
 MusicBand: Always remember us this way!!!
"""

"""
    开发环境：192.168.1.51
    source /home/sendoh/qdata-cloud/venvs/ci/bin/activate

    MySQL数据库环境：
    ssh mysql@10.10.30.15
    su -
    /usr/local/mysql/bin/mysql -uqbench -pqbench -S /home/mysql/data/mysqldata1/sock/mysql.sock

    oracle环境：
    ssh root@10.10.100.41
"""

"""
    1.获取表的列表，进行任务均匀分配，可以是多进程／线程，也可以是单线程／进程
    2.进行数据抽取，单表内部是否采用并行（一期先不考虑）
    3.数据根据key进行hash运算，生成hashbucket
    4.短源端，目标端的hashbucket进行配对，在桶内进行等值匹配运算
    5.生成检查校验的结果
    6.程序必须取消操蛋的随机hash seed特性，可以自运行前加入环境变量
    #! /bin/bash
      export PYTHONHASHSEED=0
    或者：
      PYTHONHASHSEED=0 python q_map_join.py
      6千万 offer序列化，real  39m0.677s
      1.79亿，offer key＋hashvalue序列化，37分钟
      3840万，保留数据，拖取时间 30分钟,存档数据37.5GB
      3840万，没有数据，拖取时间 23分钟,存档数据1.5GB

    安装pip工具
    wget  https://bootstrap.pypa.io/get-pip.py
    python get-pip.py

    需要额外安装的python包 pathos，用来解决unpickle对象问题
    pip install pathos
    pip install MySQLdb  or  yum install MySQL-python
    pip install cx_Oracle
    pip install sqlparse
    pip install xlrd

    # oracle的安装包
    oracle-instantclient18.5-basic-18.5.0.0.0-3.x86_64
    oracle-instantclient18.5-sqlplus-18.5.0.0.0-3.x86_64
    oracle-instantclient18.5-tools-18.5.0.0.0-3.x86_64
    oracle-instantclient18.5-devel-18.5.0.0.0-3.x86_64

    # 环境变量设置
    LD_LIBRARY_PATH=/usr/lib/oracle/18.5/client64/lib
    export LD_LIBRARY_PATH

    2019.3.21
    1.按照实体表，虚拟表模式进行元数据任务生成 2.进行虚拟表配对  完成1，2步骤的分离节藕
    修改程序支持上述功能，还未完成测试，明天继续
    
    create sequence seq_missing_rows cache 50000;
    create sequence seq_dismatch_rows cache 50000;
    create sequence seq_surplus_rows cache 50000;

    drop table q_check_dismatch_rows;
    drop table q_check_missing_rows;
    drop table q_check_surplus_rows;

    truncate table q_check_dismatch_rows;
    truncate table q_check_missing_rows;
    truncate table q_check_surplus_rows;

    insert into q_check_dismatch_rows
    (id,twins_id,twins_name,keystr,source_rowid,target_rowid,source_sharding_id,target_sharding_id,row_status)
    select /*+ use_hash(s,t) parallel(s,1) parallel(t,1) */seq_dismatch_rows.nextval,xxxxx
    from s,t
    where s.keystr = t.keystr
    and s.hashvalue <> t.hashvalue;

    insert into q_check_missing_rows
    (id,twins_id,twins_name,keystr,source_rowid,target_rowid,source_sharding_id,target_sharding_id,row_status)
    select /*+ use_hash(s,t) parallel(s,1) parallel(t,1) */seq_missing_rows.nextval,xxxxx
    from s,t
    where s.keystr = t.keystr(+)
    and t.keystr is null;

    insert into q_check_surplus_rows
    (id,twins_id,twins_name,keystr,source_rowid,target_rowid,source_sharding_id,target_sharding_id,row_status)
    select /*+ use_hash(t,s) parallel(s,1) parallel(t,1) */seq_surplus_rows.nextval,xxxxx
    from t,s
    where t.keystr = s.keystr(+)
    and s.keystr is null;

    create table q_check_dismatch_rows
    (id number,
    gmt_create date default sysdate,
    gmt_modify date default sysdate,
    twins_id number,
    twins_name varchar2(512),
    keystr VARCHAR2(2048),
    source_rowid varchar2(128),
    target_rowid varchar2(128),
    source_sharding_id number,
    target_sharding_id number,
    source_stable varchar2(128),
    target_stable varchar2(128),
    task_id number,
    row_status varchar2(64)) nologging pctfree 5;

    create table q_check_missing_rows
    (id number,
    gmt_create date default sysdate,
    gmt_modify date default sysdate,
    twins_id number,
    twins_name varchar2(512),
    keystr VARCHAR2(2048),
    source_rowid varchar2(128),
    target_rowid varchar2(128),
    source_sharding_id number,
    target_sharding_id number,
    source_stable varchar2(128),
    target_stable varchar2(128),
    task_id number,
    row_status varchar2(64)) nologging pctfree 5;

    create table q_check_surplus_rows
    (id number,
    gmt_create date default sysdate,
    gmt_modify date default sysdate,
    twins_id number,
    twins_name varchar2(512),
    keystr VARCHAR2(2048),
    source_rowid varchar2(128),
    target_rowid varchar2(128),
    source_sharding_id number,
    target_sharding_id number,
    source_stable varchar2(128),
    target_stable varchar2(128),
    task_id number,
    row_status varchar2(64)) nologging pctfree 5;

"""
__revision__ = '9527.9527'
import time
import datetime
import decimal
import sys
import os
import Queue
import multiprocessing
import string
import logging
import smtplib
import getopt
import threading
import signal
import cx_Oracle
import ConfigParser
import MySQLdb
import MySQLdb.cursors
import hashlib
import cPickle
import uuid
import xlrd
from pathos.multiprocessing import ProcessingPool as FatWorkerPool

class QTableCat():
    """解析table配置文件，生成任务列表，任务列表保存到数据库"""
    def __init__(self,v_meta_hooker = None,v_crusher_db = None):
        self.cgv_crusher_db = v_crusher_db;
        self.cgv_meta_hooker = v_meta_hooker;
        # tablelist
        self.cgv_table_data_list = [];
        self.cgv_table_title_list = {};
        # twinslist
        self.cgv_twins_data_list = [];
        self.cgv_twins_title_list = {};
        self.cgv_virtual_tabname_scope = [];
        self.cgv_task_name = "";

    def cgf_excel_reader(self,v_excel_name = "",v_excel_type = "tablelist"):
        """excel配置表读取，生成title和data数据,分为tablelist和twinslist"""
        try:
            if (v_excel_type == "tablelist"):
                # 仅仅支持一个sheet页，多余的不会处理，太鸡巴烦了
                if (len(v_excel_name.strip()) > 0):
                    v_excel_file = xlrd.open_workbook(v_excel_name.strip());
                    v_excel_contents = v_excel_file.sheets()[0];

                    if (v_excel_contents.nrows > 0):
                        for i in xrange(v_excel_contents.nrows):
                            if (i == 0):
                                # title的名字与pos的字典对应表生成,初始化cgv_table_title_list
                                for j in xrange(len(v_excel_contents.row_values(i))):
                                    self.cgv_table_title_list[v_excel_contents.row_values(i)[j]] = j;
                            else:
                                # 初始化cgv_table_data_list
                                self.cgv_table_data_list.append(v_excel_contents.row_values(i));
            elif (v_excel_type == "twinslist"):
                # 仅仅支持一个sheet页，多余的不会处理，太鸡巴烦了
                if (len(v_excel_name.strip()) > 0):
                    v_excel_file = xlrd.open_workbook(v_excel_name.strip());
                    v_excel_contents = v_excel_file.sheets()[0];

                    if (v_excel_contents.nrows > 0):
                        for i in xrange(v_excel_contents.nrows):
                            if (i == 0):
                                # title的名字与pos的字典对应表生成,初始化cgv_twins_title_list
                                for j in xrange(len(v_excel_contents.row_values(i))):
                                    self.cgv_twins_title_list[v_excel_contents.row_values(i)[j]] = j;
                            else:
                                # 初始化cgv_twins_data_list
                                self.cgv_twins_data_list.append(v_excel_contents.row_values(i));
        except Exception as e:
            print "QTableCat.cgf_excel_reader get error:" + str(e).replace("'","");
            os._exit(0);

    def cgf_get_tabcol_position(self,v_column_name):
        """根据tab列名获取列标位置"""
        return self.cgv_table_title_list.get(v_column_name);

    def cgf_get_twinscol_position(self,v_column_name):
        """根据twins列名获取列标位置"""
        return self.cgv_twins_title_list.get(v_column_name);

    def cgf_get_table_decstring(self,v_rows = None):
        """获取表描述信息的string，最终用于判断是否重复"""
        v_table_descstring = "";

        # v_table_descstring可以唯一的描述一张表信息，由type,pdb容器名(以后考虑)，owner，tabname，ip，端口，dbname_sid
        v_table_descstring = str(v_rows[self.cgf_get_tabcol_position("db_type")]) + \
        str(v_rows[self.cgf_get_tabcol_position("tab_owner")]) + \
        str(v_rows[self.cgf_get_tabcol_position("real_tname")]) + \
        str(v_rows[self.cgf_get_tabcol_position("db_ipaddr")]) + \
        str(v_rows[self.cgf_get_tabcol_position("db_port")]) + \
        str(v_rows[self.cgf_get_tabcol_position("dbname_sid")]);

        return v_table_descstring;

    def cgf_virtual_tabscope(self):
        """按照tabname进行虚拟表范围划分，相同虚拟表要连续排在一起"""
        try:
            v_start_pos = 0;
            v_end_pos = 0;
            v_now_tname = "";
            v_next_tname = "";
            v_now_sttype = "";
            v_next_sttype = "";
            v_now_dbtype = "";
            v_next_dbtype = "";

            if (len(self.cgv_table_data_list) > 0):
                # 第一行为初始值
                v_now_tname  = self.cgv_table_data_list[0][self.cgf_get_tabcol_position("virtual_tname")];
                v_now_sttype = self.cgv_table_data_list[0][self.cgf_get_tabcol_position("source_target")];
                v_now_dbtype = self.cgv_table_data_list[0][self.cgf_get_tabcol_position("db_type")];

                for i in xrange(1,len(self.cgv_table_data_list)):
                    v_next_tname = self.cgv_table_data_list[i][self.cgf_get_tabcol_position("virtual_tname")];
                    v_next_sttype = self.cgv_table_data_list[i][self.cgf_get_tabcol_position("source_target")];
                    v_next_dbtype = self.cgv_table_data_list[i][self.cgf_get_tabcol_position("db_type")];
                    if (v_now_tname == v_next_tname):
                        # 相等，只需要移动end pos
                        v_end_pos = i;
                    elif (v_now_tname != v_next_tname):
                        # 不相等，说明已经到新的虚拟表范围，也表示上一个虚拟表结束，将其放入列表，并更新tab，start，end
                        self.cgv_virtual_tabname_scope.append([v_now_tname,v_start_pos,v_end_pos,v_now_sttype,v_now_dbtype]);
                        v_now_tname = v_next_tname;
                        v_now_sttype = v_next_sttype;
                        v_now_dbtype = v_next_dbtype;
                        v_start_pos = i;
                        v_end_pos = i;

                # 最后一个虚拟表在for循环里面处理不到，补充录入
                self.cgv_virtual_tabname_scope.append([v_now_tname,v_start_pos,v_end_pos,v_now_sttype,v_now_dbtype]);
        except Exception as e:
            print "QTableCat.cgf_virtual_tabscope get error:" + str(e).replace("'","");
            os._exit(0);

    def cgf_table_meta_gen(self):
        try:
            # 生成virtual table信息，根据cgv_virtual_tabname_scope
            for i in xrange(len(self.cgv_virtual_tabname_scope)):
                v_tab_vname = self.cgv_virtual_tabname_scope[i][0];
                v_db_type = self.cgv_virtual_tabname_scope[i][4];
                v_start_pos = self.cgv_virtual_tabname_scope[i][1];
                v_end_pos = self.cgv_virtual_tabname_scope[i][2];
                v_source_target = self.cgv_virtual_tabname_scope[i][3];
                v_tab_sname = "";
                v_vtable_id = None;
                v_vtab_status = "preparing";
                if (v_source_target == "source"):
                    v_tab_sname = "qs" + self.cgv_meta_hooker.cgf_get_unique_key();
                elif (v_source_target == "target"):
                    v_tab_sname = "qt" + self.cgv_meta_hooker.cgf_get_unique_key();

                v_vtable_id = self.cgv_meta_hooker.cgf_create_vtable_info(v_vtab_name = v_tab_vname,v_stab_name = v_tab_sname,
                    v_vtab_status = v_vtab_status,v_source_target = v_source_target);

                # 创建hashvalue表
                self.cgf_hash_table_ddl(v_tab_name = v_tab_sname,v_is_truncate = False);

                # 每一个virtual table，都会生成一个或多个tab sharding信息
                v_tab_descstring_list = {};
                for j in xrange(v_start_pos,v_end_pos + 1):
                    # 获取tab desc str
                    v_tab_descstring = self.cgf_get_table_decstring(v_rows = self.cgv_table_data_list[j]);
                    # 如果该string存在于v_tab_descstring_list，表示重复，直接忽略，否则存入，进行处理
                    if (v_tab_descstring_list.get(v_tab_descstring) is None):
                        # 存入v_stab_descstring_list
                        v_tab_descstring_list[v_tab_descstring] = 9527;
                        # tab信息进入meta db
                        v_shardtab_name = self.cgv_table_data_list[j][self.cgf_get_tabcol_position("real_tname")];
                        v_shardtab_owner = self.cgv_table_data_list[j][self.cgf_get_tabcol_position("tab_owner")];
                        v_sql_select = self.cgv_table_data_list[j][self.cgf_get_tabcol_position("col_list")];
                        v_sql_where = self.cgv_table_data_list[j][self.cgf_get_tabcol_position("sql_where")];
                        v_pkuk_str = self.cgv_table_data_list[j][self.cgf_get_tabcol_position("pkuk_list")];
                        v_sharding_str = self.cgv_table_data_list[j][self.cgf_get_tabcol_position("sharding_list")];
                        v_ip = self.cgv_table_data_list[j][self.cgf_get_tabcol_position("db_ipaddr")];
                        v_port = self.cgv_table_data_list[j][self.cgf_get_tabcol_position("db_port")];
                        v_sid_dbname = self.cgv_table_data_list[j][self.cgf_get_tabcol_position("dbname_sid")];
                        v_db_user = self.cgv_table_data_list[j][self.cgf_get_tabcol_position("db_user")];
                        v_db_pwd = self.cgv_table_data_list[j][self.cgf_get_tabcol_position("db_pwd")];
                        v_table_id = self.cgv_meta_hooker.cgf_create_tabshard_info(v_vtable_id = v_vtable_id,v_sharding_table_name = v_shardtab_name,
                        v_sharding_table_owner = v_shardtab_owner,v_sharding_status = "preparing",v_sql_text = "",
                        v_sql_select = v_sql_select,v_sql_where = v_sql_where,v_pkuk_str = v_pkuk_str,v_sharding_str = v_sharding_str,
                        v_ip = v_ip,v_port = v_port,v_sid_or_dbname = v_sid_dbname,v_db_user = v_db_user,
                        v_db_pwd = v_db_pwd,v_process_parallel = 1,v_db_type = v_db_type);

                        # 生成table sharding的啦数据task
                        v_task_name = v_shardtab_owner + "." + v_shardtab_name + "." + \
                        time.strftime("%Y%m%d%H%M%S", time.localtime()) + "." + self.cgv_meta_hooker.cgf_get_unique_key();
                        self.cgv_meta_hooker.cgf_create_task_info(v_task_name = v_task_name,v_parent_id = v_table_id,v_task_type = "pull");
                    else:
                        pass;
        except Exception as e:
            print "QTableCat.cgf_table_meta_gen get error:" + str(e).replace("'","");
            os._exit(0);

    def cgf_twins_meta_gen(self):
        """生成校验对的meta信息"""
        try:
            for i in xrange(len(self.cgv_twins_data_list)):
                v_svtab_name = self.cgv_twins_data_list[i][self.cgf_get_twinscol_position("source_virtual_tname")];
                v_tvtab_name = self.cgv_twins_data_list[i][self.cgf_get_twinscol_position("target_virtual_tname")];
                # 根据vtable name获取vtable_id
                v_svtab_id = self.cgv_meta_hooker.cgf_get_vtable_id(v_vtab_name = v_svtab_name);
                v_tvtab_id = self.cgv_meta_hooker.cgf_get_vtable_id(v_vtab_name = v_tvtab_name);
                v_twins_status = "preparing";
                v_twins_id,v_task_name = self.cgv_meta_hooker.cgf_create_twins_info(v_svtab_name = v_svtab_name,
                    v_tvtab_name = v_tvtab_name,v_svtab_id = v_svtab_id,v_tvtab_id = v_tvtab_id,v_twins_status = v_twins_status);

                # 生成twins的任务,在twins gen里面，就是check
                self.cgv_meta_hooker.cgf_create_task_info(v_task_name = v_task_name,v_parent_id = v_twins_id,v_task_type = "check");

        except Exception as e:
            print "QTableCat.cgf_twins_meta_gen get error:" + str(e).replace("'","");
            os._exit(0);

    def cgf_hash_table_ddl(self,v_tab_name = None,v_is_truncate = False):
        """创建对比表，表名按照QT0000000001模式,仅支持Oracle做为对比库,未来可以引入PostgreSQL"""
        try:
            if (self.cgv_crusher_db.cgv_dbtype == "Oracle"):
                if (v_is_truncate == False):
                    # 创建表
                    v_ddl_sql = "create table " + v_tab_name + \
                    "(keystr varchar2(2048),hashvalue varchar2(512),special_key varchar2(256)," + \
                    "tabsharding_id number,datastr clob) pctfree 0 nologging";
                else:
                    # 复用表，直接truncate
                    v_ddl_sql = "Truncate Table " + v_tab_name;

                self.cgv_crusher_db.cgv_db_cur.execute(v_ddl_sql);
            elif (self.cgv_crusher_db.cgv_dbtype == "PostgreSQL"):
                # 牛逼的PostgreSQL，据说支持hashjoin，如果觉得oracle太贵，可以考虑做为对比库
                pass;

        except Exception as e:
            print "QTableCat.cgf_hash_table_ddl get error :" + str(e).replace("'","");

class QConnectDB():
    def __init__(self,v_ip = None,v_port = None,v_sid_or_dbname = None,
                v_db_user = None,v_dbpwd = None,v_dbtype = "Oracle"):
        self.cgv_ip = v_ip;
        self.cgv_port = v_port;
        self.cgv_sid_or_dbname = v_sid_or_dbname;
        self.cgv_dbuser = v_db_user;
        self.cgv_dbpwd = v_dbpwd;
        self.cgv_dbtype = v_dbtype;
        self.cgv_conn_db = None;
        self.cgv_db_cur = None;
        self.cgv_conn_db_for_sscur = None;
        self.cgv_db_sscur = None;
        self.cgv_db_charset = "UTF8";

        # 数据库连接
        self.cgf_connect_db();

    def cgf_charset_name_transfor(self,v_charset = ""):
        """转换为python数据库驱动能识别的字符集名称，否则程序报错"""
        return v_charset;

    def cgf_charset_set(self,v_table_name = None):
        """Oracle仅仅需要设置数据库字符集，MySQL需要探测给定表的字符集"""
        try:
            v_charset = "";
            if (self.cgv_dbtype == "Oracle"):
                # oracle字符集检测
                self.cgv_db_cur.execute("select userenv('language') as oset from dual");
                v_rows = self.cgv_db_cur.fetchall();
                if (len(v_rows) > 0):
                    v_charset = v_rows[0][0];
            if (self.cgv_dbtype == "MySQL"):
                # mysql字符集，需要到表级别
                self.cgv_db_cur.execute("show create table " + v_table_name);
                v_rows = self.cgv_db_cur.fetchall();
                if (len(v_rows) > 0):
                    v_create_text = v_rows[0][1];
                    v_charset = v_create_text[v_create_text.find("CHARSET=") + 8:v_create_text.find("COLLATE=")].strip();

            # 获取python能识别的字符集名称
            self.cgv_db_charset = self.cgf_charset_name_transfor(v_charset = v_charset);

            # Oracle数据库生效字符集环境
            if (self.cgv_dbtype == "Oracle"):
                os.environ['NLS_LANG'] = self.cgv_db_charset;

            # 重连数据库连接，以生效新字符集环境
            self.cgf_reconnect_db();

            return self.cgv_db_charset;
        except Exception as e:
            print "QConnectDB.cgf_charset_set get error:" + self.cgv_ip + ":" + str(self.cgv_port) + ":" + self.cgv_sid_or_dbname;
            print str(e).replace("'","");
            os._exit(0);

    def cgf_connect_db(self):
        """数据库连接"""
        try:
            if (self.cgv_dbtype == "Oracle"):
                v_dsn_tns = cx_Oracle.makedsn(self.cgv_ip,self.cgv_port,self.cgv_sid_or_dbname);
                self.cgv_conn_db = cx_Oracle.Connection(self.cgv_dbuser,self.cgv_dbpwd,v_dsn_tns);
                self.cgv_db_cur = self.cgv_conn_db.cursor();

            if (self.cgv_dbtype == "MySQL"):
                self.cgv_conn_db_for_sscur = MySQLdb.Connection(host = self.cgv_ip,user = self.cgv_dbuser,
                passwd = self.cgv_dbpwd,db = self.cgv_sid_or_dbname,port = self.cgv_port,charset = self.cgv_db_charset,
                cursorclass = MySQLdb.cursors.SSCursor);
                self.cgv_db_sscur = self.cgv_conn_db_for_sscur.cursor();

                self.cgv_conn_db = MySQLdb.Connection(host = self.cgv_ip,user = self.cgv_dbuser,
                passwd = self.cgv_dbpwd,db = self.cgv_sid_or_dbname,port = self.cgv_port,charset = self.cgv_db_charset);
                self.cgv_db_cur = self.cgv_conn_db.cursor();

            if (self.cgv_dbtype == "Vertica"):
                pass;

            if (self.cgv_dbtype == "PostgreSQL"):
                pass;

            if (self.cgv_dbtype == "HBass"):
                pass;

        except Exception, e:
            print "QConnectDB.cgf_connect_db get error:" + self.cgv_ip + ":" + str(self.cgv_port) + ":" + self.cgv_sid_or_dbname;
            print str(e).replace("'","");
            os._exit(0);

    def cgf_disconnect_db(self):
        """数据库关闭连接"""
        try:
            if (self.cgv_db_cur):
                self.cgv_db_cur.close();
            if (self.cgv_conn_db):
                self.cgv_conn_db.close();
            if (self.cgv_db_sscur):
                self.cgv_db_cur.close();
            if (self.cgv_conn_db_for_sscur):
                self.cgv_conn_db_for_sscur.close();
        except Exception, e:
            print "QConnectDB.cgf_disconnect_db get error:" + self.cgv_ip + ":" + self.cgv_port + ":" + self.cgv_sid_or_dbname;
            print str(e).replace("'","");

    def cgf_reconnect_db(self):
        try:
            self.cgf_disconnect_db();
            self.cgf_connect_db();
        except Exception, e:
            print "QConnectDB.cgf_reconnect_db get error:" + self.cgv_ip + ":" + self.cgv_port + ":" + self.cgv_sid_or_dbname;
            print str(e).replace("'","");

class QMetaHooker():
    """负责元数据维护管理，目前采用MySQL库作为元数据管理"""
    def __init__(self,v_meta_db = None):
        self.cgv_conn_db = v_meta_db.cgv_conn_db;
        self.cgv_db_cur = v_meta_db.cgv_db_cur;
        self.cgv_meta_db = v_meta_db;

    def cgf_get_unique_key(self):
        """生成唯一key值函数"""
        try:
            v_unique_key = "";
            if (self.cgv_db_cur):

                v_sql = "insert into q_idlist_table (gmt_create) " + \
                "values(CURRENT_TIMESTAMP)";
                self.cgv_db_cur.execute(v_sql);

                # 获取插入的主键id，必须在事务commit之前，否则为0
                v_unique_key = str(self.cgv_conn_db.insert_id()).rjust(10,"0");
                self.cgv_conn_db.commit();

            return v_unique_key;
        except Exception, e:
            print "QMetaHooker.cgv_get_unique_key get error:" + str(e).replace("'","");
            self.cgv_conn_db.rollback();
            self.cgv_meta_db.cgf_disconnect_db();
            os._exit(0);

    def cgf_create_task_info(self,v_task_name = "",v_parent_id = None,v_task_type = None):
        """任务创建函数，返回taskid，失败的话程序直接退出"""
        try:
            v_task_id = 0;
            if (self.cgv_db_cur):
                if (len(v_task_name) < 1):
                    v_task_name = "Task:" + str(datetime.datetime.now());

                v_sql = "insert into q_task_list (task_name,task_status,task_type,parent_id,begin_time) " + \
                "values('%s','preparing','%s',%s,CURRENT_TIMESTAMP)" % (v_task_name,v_task_type,v_parent_id);

                self.cgv_db_cur.execute(v_sql);
                # 获取插入的主键id，必须在事务commit之前，否则为0
                v_task_id = self.cgv_conn_db.insert_id();
                self.cgv_conn_db.commit();
        except Exception, e:
            print "QMetaHooker.cgv_create_task_info get error:" + str(e).replace("'","");
            self.cgv_conn_db.rollback();
            self.cgv_meta_db.cgf_disconnect_db();
            os._exit(0);

    def cgf_update_task_status(self,v_task_id = None,v_task_status = None,v_error_info = ""):
        """设置task的状态，preparing准备，gooooing进行中，finished全部校验完成,error表示出错"""
        try:
            if (self.cgv_db_cur):
                v_sql = """update q_task_list set task_status = '%s',
                        gmt_modify = CURRENT_TIMESTAMP,
                        error_info = '%s'
                        where id = %s""" % (v_task_status,v_error_info,v_task_id);
                self.cgv_db_cur.execute(v_sql);
                self.cgv_conn_db.commit();
        except Exception as e:
            self.cgv_conn_db.rollback();
            print "QMetaHooker.cgf_update_task_status get error:" + str(e).replace("'","");

    def cgf_update_task_btime(self,v_task_id = None):
        """设置task的开始时间"""
        try:
            if (self.cgv_db_cur):
                v_sql = """update q_task_list set 
                        gmt_modify = CURRENT_TIMESTAMP,
                        begin_time = CURRENT_TIMESTAMP
                        where id = %s""" % (v_task_id);
                self.cgv_db_cur.execute(v_sql);
                self.cgv_conn_db.commit();
        except Exception as e:
            self.cgv_conn_db.rollback();
            print "QMetaHooker.cgf_update_task_btime get error:" + str(e).replace("'","");

    def cgf_update_task_etime(self,v_task_id = None):
        """设置task的开始时间"""
        try:
            if (self.cgv_db_cur):
                v_sql = """update q_task_list set 
                        gmt_modify = CURRENT_TIMESTAMP,
                        end_time = CURRENT_TIMESTAMP
                        where id = %s""" % (v_task_id);
                self.cgv_db_cur.execute(v_sql);
                self.cgv_conn_db.commit();
        except Exception as e:
            self.cgv_conn_db.rollback();
            print "QMetaHooker.cgf_update_task_etime get error:" + str(e).replace("'","");

    def cgf_create_vtable_info(self,v_vtab_name = "",v_stab_name = "",v_vtab_status = "",v_source_target = ""):
        """创建virtual table信息"""
        try:
            v_vtable_id = 0;
            if (self.cgv_db_cur):
                v_sql = "insert into q_virtual_table_list " + \
                "(virtual_table_name,single_table_name,virtual_table_status,source_target)" + \
                "values('%s','%s','%s','%s')" % \
                (v_vtab_name,v_stab_name,v_vtab_status,v_source_target);

                self.cgv_db_cur.execute(v_sql);
                # 获取插入的主键id，必须在事务commit之前，否则为0
                v_vtable_id = self.cgv_conn_db.insert_id();
                self.cgv_conn_db.commit();

            return v_vtable_id;
        except Exception, e:
            print "QMetaHooker.cgv_create_vtable_info get error:" + str(e).replace("'","");
            self.cgv_conn_db.rollback();

    def cgf_get_vtable_id(self,v_vtab_name = ""):
        try:
            v_vtable_id = 0;
            if (self.cgv_db_cur):
                v_sql = "select id from q_virtual_table_list where virtual_table_name = '%s'" % (v_vtab_name);
                self.cgv_db_cur.execute(v_sql);
                v_rows = self.cgv_db_cur.fetchall();

                if (len(v_rows) > 0):
                    v_vtable_id = v_rows[0][0];

            return v_vtable_id;
        except Exception, e:
            print "QMetaHooker.cgf_get_vtable_id get error:" + str(e).replace("'","");

    def cgf_create_tabshard_info(self,v_vtable_id = None,v_sharding_table_name = "",
        v_sharding_table_owner = "",v_sharding_status = "",v_sql_text = "",
        v_sql_select = "",v_sql_where = "",v_pkuk_str = "",v_sharding_str = "",
        v_ip = "",v_port = None,v_sid_or_dbname = "",v_db_user = "",
        v_db_pwd = "",v_process_parallel = 1,v_db_type = ""):
        """创建table sharding信息"""
        try:
            v_tabshard_id = 0;
            if (self.cgv_db_cur):
                v_sql = "insert into q_table_sharding_list " + \
                "(vtable_id,sharding_table_name,sharding_table_owner," + \
                "sharding_status,sql_text,sql_select,sql_where,pkuk_str,sharding_str," + \
                "ip,port,sid_or_dbname,db_user,db_pwd,process_parallel,db_type) " + \
                "values(%s,'%s','%s','%s','%s','%s','%s','%s','%s','%s',%s,'%s','%s','%s',%s,'%s')" % \
                (v_vtable_id,v_sharding_table_name,v_sharding_table_owner,v_sharding_status,
                v_sql_text,v_sql_select,v_sql_where,v_pkuk_str,v_sharding_str,v_ip,v_port,
                v_sid_or_dbname,v_db_user,v_db_pwd,v_process_parallel,v_db_type);

                print v_sql;
                self.cgv_db_cur.execute(v_sql);
                # 获取插入的主键id，必须在事务commit之前，否则为0
                v_tabshard_id = self.cgv_conn_db.insert_id();
                self.cgv_conn_db.commit();

            return v_tabshard_id;
        except Exception, e:
            print "QMetaHooker.cgf_create_tabshard_info get error:" + str(e).replace("'","");
            self.cgv_conn_db.rollback();

    def cgf_create_twins_info(self,v_svtab_name = "",v_tvtab_name = "",v_svtab_id = None,v_tvtab_id = None,v_twins_status = ""):
        """创建twins记录"""
        try:
            v_twins_id = 0;
            if (self.cgv_db_cur):
                # twins name唯一
                v_twins_name = v_svtab_name + "-" + v_tvtab_name + "." + \
                time.strftime("%Y%m%d%H%M%S", time.localtime()) + "." + self.cgf_get_unique_key();

                v_sql = "insert into q_tab_twins_list " + \
                "(twins_name,source_vtable_name,target_vtable_name,source_vtable_id,target_vtable_id,twins_status)" + \
                "values('%s','%s','%s',%s,%s,'%s')" % \
                (v_twins_name,v_svtab_name,v_tvtab_name,v_svtab_id,v_tvtab_id,v_twins_status);

                self.cgv_db_cur.execute(v_sql);
                # 获取插入的主键id，必须在事务commit之前，否则为0
                v_twins_id = self.cgv_conn_db.insert_id();
                self.cgv_conn_db.commit();

            return v_twins_id,v_twins_name;
        except Exception, e:
            print "QMetaHooker.cgv_create_twins_info get error:" + str(e).replace("'","");
            self.cgv_conn_db.rollback();

    def cgf_update_vtable_trows(self,v_vtable_id = None,v_table_rows = None,v_type = "add"):
        """设置表的行数,v_type决定是累加还是直接更新，分为add|final"""
        try:
            if (self.cgv_db_cur):
                v_sql = "";
                # 累加模式
                if (v_type == "add"):
                    v_sql = """update q_virtual_table_list 
                    set virtual_table_rows = virtual_table_rows + %s,
                    gmt_modify = CURRENT_TIMESTAMP
                    where id = %s""" % (v_table_rows,v_vtable_id);
                # 直接更新模式
                elif (v_type == "final"):
                    v_sql = """update q_virtual_table_list 
                    set virtual_table_rows = %s,
                    gmt_modify = CURRENT_TIMESTAMP
                    where id = %s""" % (v_table_rows,v_vtable_id);

                self.cgv_db_cur.execute(v_sql);
                self.cgv_conn_db.commit();
        except Exception as e:
            self.cgv_conn_db.rollback();
            print "QMetaHooker.cgf_update_vtable_trows get error:" + str(e).replace("'","");

    def cgf_update_vtable_epull(self,v_vtable_id = None):
        """设置表的pull结束时间"""
        try:
            if (self.cgv_db_cur):
                v_sql = """update q_virtual_table_list 
                set pull_etime = CURRENT_TIMESTAMP,
                gmt_modify = CURRENT_TIMESTAMP
                where id = %s""" % (v_vtable_id);

                self.cgv_db_cur.execute(v_sql);
                self.cgv_conn_db.commit();
        except Exception as e:
            self.cgv_conn_db.rollback();
            print "QMetaHooker.cgf_update_vtable_epull get error:" + str(e).replace("'","");

    def cgf_update_vtable_bpull(self,v_vtable_id = None):
        """设置表的pull开始时间"""
        try:
            if (self.cgv_db_cur):
                v_sql = """update q_virtual_table_list 
                set pull_btime = CURRENT_TIMESTAMP,
                gmt_modify = CURRENT_TIMESTAMP
                where id = %s""" % (v_vtable_id);

                self.cgv_db_cur.execute(v_sql);
                self.cgv_conn_db.commit();
        except Exception as e:
            self.cgv_conn_db.rollback();
            print "QMetaHooker.cgf_update_vtable_bpull get error:" + str(e).replace("'","");

    def cgf_update_vtable_ecountoff(self,v_vtable_id = None):
        """设置表的countoff结束时间"""
        try:
            if (self.cgv_db_cur):
                v_sql = """update q_virtual_table_list 
                set countoff_etime = CURRENT_TIMESTAMP,
                gmt_modify = CURRENT_TIMESTAMP
                where id = %s""" % (v_vtable_id);

                self.cgv_db_cur.execute(v_sql);
                self.cgv_conn_db.commit();
        except Exception as e:
            self.cgv_conn_db.rollback();
            print "QMetaHooker.cgf_update_vtable_ecountoff get error:" + str(e).replace("'","");

    def cgf_update_vtable_bcountoff(self,v_vtable_id = None):
        """设置表的countoff开始时间"""
        try:
            if (self.cgv_db_cur):
                v_sql = """update q_virtual_table_list 
                set countoff_btime = CURRENT_TIMESTAMP,
                gmt_modify = CURRENT_TIMESTAMP
                where id = %s""" % (v_vtable_id);

                self.cgv_db_cur.execute(v_sql);
                self.cgv_conn_db.commit();
        except Exception as e:
            self.cgv_conn_db.rollback();
            print "QMetaHooker.cgf_update_vtable_bcountoff get error:" + str(e).replace("'","");

    def cgf_update_vtable_status(self,v_vtable_id = None,v_vtable_status = None,v_error_info = ""):
        """设置vtable的状态，preparing准备 counting统计数据量 counted 统计完成，pulling拉数据 pulled拉取完成 error表示出错"""
        try:
            if (self.cgv_db_cur):
                v_sql = """update q_virtual_table_list set virtual_table_status = '%s',
                        gmt_modify = CURRENT_TIMESTAMP,
                        error_info = '%s'
                        where id = %s""" % (v_vtable_status,v_error_info,v_vtable_id);
                self.cgv_db_cur.execute(v_sql);
                self.cgv_conn_db.commit();
        except Exception as e:
            self.cgv_conn_db.rollback();
            print "QMetaHooker.cgf_update_vtable_status get error:" + str(e).replace("'","");

    def cgf_update_twins_bcheck(self,v_twins_id = None):
        """设置twins的check开始时间"""
        try:
            if (self.cgv_db_cur):
                v_sql = """update q_tab_twins_list 
                        set check_begin_time = CURRENT_TIMESTAMP,
                        gmt_modify = CURRENT_TIMESTAMP
                        where id = %s""" % (v_twins_id);

                self.cgv_db_cur.execute(v_sql);
                self.cgv_conn_db.commit();
        except Exception as e:
            self.cgv_conn_db.rollback();
            print "QMetaHooker.cgf_update_twins_bcheck get error:" + str(e).replace("'","");

    def cgf_update_twins_echeck(self,v_twins_id = None):
        """设置twins的check结束时间"""
        try:
            if (self.cgv_db_cur):
                v_sql = """update q_tab_twins_list 
                        set check_end_time = CURRENT_TIMESTAMP,
                        gmt_modify = CURRENT_TIMESTAMP
                        where id = %s""" % (v_twins_id);

                self.cgv_db_cur.execute(v_sql);
                self.cgv_conn_db.commit();
        except Exception as e:
            self.cgv_conn_db.rollback();
            print "QMetaHooker.cgf_update_twins_echeck get error:" + str(e).replace("'","");

    def cgf_update_twins_brecheck(self,v_twins_id = None):
        """设置twins的recheck开始时间"""
        try:
            if (self.cgv_db_cur):
                v_sql = """update q_tab_twins_list 
                        set recheck_begin_time = CURRENT_TIMESTAMP,
                        gmt_modify = CURRENT_TIMESTAMP
                        where id = %s""" % (v_twins_id);

                self.cgv_db_cur.execute(v_sql);
                self.cgv_conn_db.commit();
        except Exception as e:
            self.cgv_conn_db.rollback();
            print "QMetaHooker.cgf_update_twins_brecheck get error:" + str(e).replace("'","");

    def cgf_update_twins_erecheck(self,v_twins_id = None):
        """设置twins的recheck结束时间"""
        try:
            if (self.cgv_db_cur):
                v_sql = """update q_tab_twins_list 
                        set recheck_end_time = CURRENT_TIMESTAMP,
                        gmt_modify = CURRENT_TIMESTAMP
                        where id = %s""" % (v_twins_id);

                self.cgv_db_cur.execute(v_sql);
                self.cgv_conn_db.commit();
        except Exception as e:
            self.cgv_conn_db.rollback();
            print "QMetaHooker.cgf_update_twins_erecheck get error:" + str(e).replace("'","");

    def cgf_update_twins_breport(self,v_twins_id = None):
        """设置twins的report开始时间"""
        try:
            if (self.cgv_db_cur):
                v_sql = """update q_tab_twins_list 
                        set report_begin_time = CURRENT_TIMESTAMP,
                        gmt_modify = CURRENT_TIMESTAMP
                        where id = %s""" % (v_twins_id);

                self.cgv_db_cur.execute(v_sql);
                self.cgv_conn_db.commit();
        except Exception as e:
            self.cgv_conn_db.rollback();
            print "QMetaHooker.cgf_update_twins_breport get error:" + str(e).replace("'","");

    def cgf_update_twins_ereport(self,v_twins_id = None):
        """设置twins的report结束时间"""
        try:
            if (self.cgv_db_cur):
                v_sql = """update q_tab_twins_list 
                        set report_end_time = CURRENT_TIMESTAMP,
                        gmt_modify = CURRENT_TIMESTAMP
                        where id = %s""" % (v_twins_id);

                self.cgv_db_cur.execute(v_sql);
                self.cgv_conn_db.commit();
        except Exception as e:
            self.cgv_conn_db.rollback();
            print "QMetaHooker.cgf_update_twins_ereport get error:" + str(e).replace("'","");

    def cgf_update_twins_brepair(self,v_twins_id = None):
        """设置twins的repair开始时间"""
        try:
            if (self.cgv_db_cur):
                v_sql = """update q_tab_twins_list 
                        set repair_begin_time = CURRENT_TIMESTAMP,
                        gmt_modify = CURRENT_TIMESTAMP
                        where id = %s""" % (v_twins_id);

                self.cgv_db_cur.execute(v_sql);
                self.cgv_conn_db.commit();
        except Exception as e:
            self.cgv_conn_db.rollback();
            print "QMetaHooker.cgf_update_twins_brepair get error:" + str(e).replace("'","");

    def cgf_update_twins_erepair(self,v_twins_id = None):
        """设置twins的repair结束时间"""
        try:
            if (self.cgv_db_cur):
                v_sql = """update q_tab_twins_list 
                        set repair_end_time = CURRENT_TIMESTAMP,
                        gmt_modify = CURRENT_TIMESTAMP
                        where id = %s""" % (v_twins_id);

                self.cgv_db_cur.execute(v_sql);
                self.cgv_conn_db.commit();
        except Exception as e:
            self.cgv_conn_db.rollback();
            print "QMetaHooker.cgf_update_twins_erepair get error:" + str(e).replace("'","");

    def cgf_update_twins_dismatch(self,v_twins_id = None,v_dismatch_rows = 0):
        """设置twins的dismatch rows"""
        try:
            if (self.cgv_db_cur):
                v_sql = """update q_tab_twins_list 
                        set dismatch_rows = %s,
                        gmt_modify = CURRENT_TIMESTAMP
                        where id = %s""" % (v_dismatch_rows,v_twins_id);

                self.cgv_db_cur.execute(v_sql);
                self.cgv_conn_db.commit();
        except Exception as e:
            self.cgv_conn_db.rollback();
            print "QMetaHooker.cgf_update_twins_dismatch get error:" + str(e).replace("'","");

    def cgf_update_twins_missing(self,v_twins_id = None,v_missing_rows = 0):
        """设置twins的missing rows"""
        try:
            if (self.cgv_db_cur):
                v_sql = """update q_tab_twins_list 
                        set missing_rows = %s,
                        gmt_modify = CURRENT_TIMESTAMP
                        where id = %s""" % (v_missing_rows,v_twins_id);

                self.cgv_db_cur.execute(v_sql);
                self.cgv_conn_db.commit();
        except Exception as e:
            self.cgv_conn_db.rollback();
            print "QMetaHooker.cgf_update_twins_missing get error:" + str(e).replace("'","");

    def cgf_update_twins_surplus(self,v_twins_id = None,v_surplus_rows = 0):
        """设置twins的surplus rows"""
        try:
            if (self.cgv_db_cur):
                v_sql = """update q_tab_twins_list 
                        set surplus_rows = %s,
                        gmt_modify = CURRENT_TIMESTAMP
                        where id = %s""" % (v_surplus_rows,v_twins_id);

                self.cgv_db_cur.execute(v_sql);
                self.cgv_conn_db.commit();
        except Exception as e:
            self.cgv_conn_db.rollback();
            print "QMetaHooker.cgf_update_twins_surplus get error:" + str(e).replace("'","");

    def cgf_update_twins_match(self,v_twins_id = None,v_match_rows = 0):
        """设置twins的match rows"""
        try:
            if (self.cgv_db_cur):
                v_sql = """update q_tab_twins_list 
                        set match_rows = %s,
                        gmt_modify = CURRENT_TIMESTAMP
                        where id = %s""" % (v_match_rows,v_twins_id);

                self.cgv_db_cur.execute(v_sql);
                self.cgv_conn_db.commit();
        except Exception as e:
            self.cgv_conn_db.rollback();
            print "QMetaHooker.cgf_update_twins_match get error:" + str(e).replace("'","");

    def cgf_update_twins_status(self,v_twins_id = None,v_twins_status = None,v_error_info = ""):
        """设置twins的状态，preparing准备 checking数据校验过程中 checked数据校验完成，rechecking，rechecked，reporting,reported,repairing，repaired，finished全部校验完成,error表示出错"""
        try:
            if (self.cgv_db_cur):
                v_sql = """update q_tab_twins_list set twins_status = '%s',
                        gmt_modify = CURRENT_TIMESTAMP,
                        error_info = '%s'
                        where id = %s""" % (v_twins_status,v_error_info,v_twins_id);

                self.cgv_db_cur.execute(v_sql);
                self.cgv_conn_db.commit();
        except Exception as e:
            self.cgv_conn_db.rollback();
            print "QMetaHooker.cgf_update_twins_status get error:" + str(e).replace("'","");

    def cgf_get_twins_info(self,v_twins_id = None):
        """获取twins相关的信息，包括twins名称，状态，相关的vtable信息等等"""
        try:
            v_twins_info = {};
            if (self.cgv_db_cur):
                v_sql = """select a.twins_name,a.source_vtable_name,a.target_vtable_name,
                s.single_table_name as source_stable_name,t.single_table_name as target_stable_name,
                s.virtual_table_rows as source_rows,t.virtual_table_rows as target_rows,a.twins_status
                from q_tab_twins_list a,q_virtual_table_list s,q_virtual_table_list t
                where a.id = %s
                and a.source_vtable_id = s.id
                and a.target_vtable_id = t.id""" % (v_twins_id);
                self.cgv_db_cur.execute(v_sql);
                v_twins_col_desc = self.cgv_db_cur.description;
                v_twins_list = self.cgv_db_cur.fetchall();
                if (len(v_twins_list) > 0):
                    # 仅仅处理第一行数据，列名根据desc来进行，以col:value的模式放入v_twins_info map
                    for i in xrange(len(v_twins_col_desc)):
                        v_twins_info[v_twins_col_desc[i][0].lower()] = v_twins_list[0][i];

                return v_twins_info;
        except Exception as e:
            print "QMetaHooker.cgf_get_twins_info get error:" + str(e).replace("'","");

    def cgf_update_sharding_status(self,v_sharding_id = None,v_sharding_status = None,v_error_info = ""):
        """设置twins的状态，starting准备 counting统计数据量 counted统计结束，pulling拉数据 pulled全部校验完成 error表示出错"""
        try:
            if (self.cgv_db_cur):
                v_sql = """update q_table_sharding_list set sharding_status = '%s',
                        gmt_modify = CURRENT_TIMESTAMP,
                        error_info = '%s'
                        where id = %s""" % (v_sharding_status,v_error_info,v_sharding_id);
                self.cgv_db_cur.execute(v_sql);
                self.cgv_conn_db.commit();
        except Exception as e:
            self.cgv_conn_db.rollback();
            print "QMetaHooker.cgf_update_sharding_status get error:" + str(e).replace("'","");

    def cgf_update_sharding_bpull(self,v_sharding_id = None):
        """更新sharding拉取开始时间"""
        try:
            if (self.cgv_db_cur):
                v_sql = """update q_table_sharding_list 
                set pull_btime = CURRENT_TIMESTAMP,
                gmt_modify = CURRENT_TIMESTAMP
                where id = %s""" % (v_sharding_id);
                self.cgv_db_cur.execute(v_sql);
                self.cgv_conn_db.commit();
        except Exception as e:
            self.cgv_conn_db.rollback();
            print "QMetaHooker.cgf_update_sharding_bpull get error:" + str(e).replace("'","");

    def cgf_update_sharding_epull(self,v_sharding_id = None):
        """更新sharding拉取结束时间"""
        try:
            if (self.cgv_db_cur):
                v_sql = """update q_table_sharding_list 
                set pull_etime = CURRENT_TIMESTAMP,
                gmt_modify = CURRENT_TIMESTAMP
                where id = %s""" % (v_sharding_id);
                self.cgv_db_cur.execute(v_sql);
                self.cgv_conn_db.commit();
        except Exception as e:
            self.cgv_conn_db.rollback();
            print "QMetaHooker.cgf_update_sharding_epull get error:" + str(e).replace("'","");

    def cgf_update_sharding_bcountoff(self,v_sharding_id = None):
        """更新sharding统计计数开始时间"""
        try:
            if (self.cgv_db_cur):
                v_sql = """update q_table_sharding_list 
                set countoff_btime = CURRENT_TIMESTAMP,
                gmt_modify = CURRENT_TIMESTAMP
                where id = %s""" % (v_sharding_id);
                self.cgv_db_cur.execute(v_sql);
                self.cgv_conn_db.commit();
        except Exception as e:
            self.cgv_conn_db.rollback();
            print "QMetaHooker.cgf_update_sharding_bcountoff get error:" + str(e).replace("'","");

    def cgf_update_sharding_ecountoff(self,v_sharding_id = None):
        """更新sharding统计计数结束时间"""
        try:
            if (self.cgv_db_cur):
                v_sql = """update q_table_sharding_list 
                set countoff_etime = CURRENT_TIMESTAMP,
                gmt_modify = CURRENT_TIMESTAMP
                where id = %s""" % (v_sharding_id);
                self.cgv_db_cur.execute(v_sql);
                self.cgv_conn_db.commit();
        except Exception as e:
            self.cgv_conn_db.rollback();
            print "QMetaHooker.cgf_update_sharding_ecountoff get error:" + str(e).replace("'","");

    def cgf_update_sharding_trows(self,v_sharding_id = None,v_sharding_rows = 0,v_type = "add"):
        """更新sharding的行数"""
        try:
            if (self.cgv_db_cur):
                v_sql = "";
                # 增量模式／更新模式
                if (v_type == "add"):
                    v_sql = """update q_table_sharding_list 
                    set gmt_modify = CURRENT_TIMESTAMP,
                    sharding_table_rows = sharding_table_rows + %s
                    where id = %s""" % (v_sharding_rows,v_sharding_id);
                elif (v_type == "final"):
                    v_sql = """update q_table_sharding_list 
                    set gmt_modify = CURRENT_TIMESTAMP,
                    sharding_table_rows = %s
                    where id = %s""" % (v_sharding_rows,v_sharding_id);

                self.cgv_db_cur.execute(v_sql);
                self.cgv_conn_db.commit();
        except Exception as e:
            self.cgv_conn_db.rollback();
            print "QMetaHooker.cgf_update_sharding_trows get error:" + str(e).replace("'","");

    def cgf_update_sharding_pkuk(self,v_sharding_id = None,v_pkuk_str = ""):
        """更新sharding的pkuk，sahrding str"""
        try:
            if (self.cgv_db_cur):
                v_sql = """update q_table_sharding_list 
                set gmt_modify = CURRENT_TIMESTAMP,
                pkuk_str = '%s'
                where id = %s""" % (v_pkuk_str,v_sharding_id);

                self.cgv_db_cur.execute(v_sql);
                self.cgv_conn_db.commit();
        except Exception as e:
            self.cgv_conn_db.rollback();
            print "QMetaHooker.cgf_update_sharding_keystr get error:" + str(e).replace("'","");
            
class QTablePipe():
    """TablePipe，负责表数据的抽取，转换，加载,主要基于数据库来计算"""
    def __init__(self,v_pkuk_str = "",v_sharding_str = "",v_fetch_MB = 256,
                v_sql_where = "",v_select_cols = "",v_is_need_data = True,
                v_tab_owner_source = None,v_tab_name_source = None,
                v_tab_owner_target = None,v_tab_name_target = None,
                v_source_db = None,v_crusher_db = None,v_meta_hooker = None,
                v_process_parallel = 1,v_hash_type = "hash",v_line_factor = None,
                v_task_id = 0,v_vtable_id = 0,v_sharding_id = 0):
        # 查询SQL信息
        self.cgv_sql = "";
        self.cgv_count_sql = "";
        self.cgv_estimate_sql = "";
        self.cgv_key_sql = "";
        self.cgv_sql_where = v_sql_where;
        self.cgv_select_cols = v_select_cols.lower().replace(" ","");
        self.cgv_selectcol_list = v_select_cols.lower().replace(" ","").split(",");
        # 列描述信息
        self.cgv_curcol_desc_list = None;
        # key信息
        self.cgv_pkuk_str = v_pkuk_str.lower().replace(" ","").rstrip(",").lstrip(",");
        self.cgv_sharding_str = v_sharding_str.lower().replace(" ","").rstrip(",").lstrip(",");
        self.cgv_key_str = self.cgv_pkuk_str + "," + self.cgv_sharding_str
        self.cgv_key_list = self.cgv_key_str.split(",");
        # 查询并行度，oracle有效
        self.cgv_process_parallel = v_process_parallel;
        # 源端表信息
        self.cgv_tab_owner_source = v_tab_owner_source;
        self.cgv_tab_name_source = v_tab_name_source;
        # 目标端表信息
        self.cgv_tab_owner_target = v_tab_owner_target;
        self.cgv_tab_name_target = v_tab_name_target;
        # 数据库信息
        self.cgv_crusher_db = v_crusher_db;
        self.cgv_source_db = v_source_db;
        # 元数据信息维护
        self.cgv_meta_hooker = v_meta_hooker;
        # 行数限制
        self.cgv_fetch_MB = v_fetch_MB;
        self.cgv_block_rows = 500000;
        # block的col:pos,pos:col字典
        self.cgv_col_pos_list = {};
        self.cgv_pos_col_list = {};
        # block的key列的列标
        self.cgv_keycol_position = [];
        # block的数据列列标
        self.cgv_datacol_position = [];
        # data列列名（不包含key列）
        self.cgv_datacol = [];
        # 每一批数据的获取list
        self.cgv_data_block = [];
        # key_str,hash_value
        self.cgv_is_need_data = v_is_need_data;
        self.cgv_keystr_hashvalue = [];
        self.cgv_hash_type = v_hash_type;

        # taskid,twinsid,shardingid初始化
        self.cgv_task_id = v_task_id;
        self.cgv_vtable_id = v_vtable_id;
        self.cgv_sharding_id = v_sharding_id;        

        # 表的数据字符集相关
        self.cgv_source_charset = "";
        self.cgv_target_charset = "";

        # 换行符的填充因子
        self.cgv_line_factor = v_line_factor;

        ###################### 初始化信息 ######################
        # 确定表的key col，优先用户自己定义的key col，如没定义，自动获取确定
        self.cgf_get_key_cols();
        # 初始化select col，会自动补全key列
        self.cgf_define_selectcols();
        # 生成数据查询sql，数据计数sql
        self.cgf_make_table_sql();
        # 评估每一个block的rows大小，block是每次fetch的单元，必须控制大小，否则程序内存开销扛不住
        self.cgf_estimate_blockrows(v_set_col_desc = True,v_count_off = False);

        # 初始化列名描述相关内容
        self.cgf_init_datacol_kvlist();
        self.cgf_init_keycol_position();
        self.cgf_init_datacol_position();

        print self.cgv_col_pos_list;
        print self.cgv_keycol_position;
        print self.cgv_datacol_position;
        print self.cgv_key_str;
        print self.cgv_key_list;

    def cgf_define_selectcols(self):
        """定义最终的select查询列，需要考虑用户自定义查询列不带key列时的key列补齐"""
        v_col_count = 0;
        if (len(self.cgv_select_cols) > 0):
            for i in xrange(len(self.cgv_key_list)):
                if (len(self.cgv_key_list[i]) > 0):
                    # 在select cols里面寻找是否包含该key列
                    for j in xrange(len(self.cgv_selectcol_list)):
                        # 包含了就直接进入下一个key
                        if (self.cgv_key_list[i] == self.cgv_selectcol_list[j]):
                            v_col_count = 0;
                            # 退出当前循环
                            break;

                        # 计数器增加
                        v_col_count = v_col_count + 1;

                    # 根据计数器判断整轮循环完毕，不包含，说明key列缺乏，需要补充
                    if (v_col_count >= len(self.cgv_selectcol_list)):
                        self.cgv_select_cols = self.cgv_select_cols + "," + self.cgv_key_list[i];
                        # 计数器清零
                        v_col_count = 0;

            self.cgv_selectcol_list = self.cgv_select_cols.lower().replace(" ","").split(",");


    def cgf_make_table_sql(self):
        """拼接查询SQL"""
        v_sql = "";
        v_count_sql = "";
        v_estimate_sql = "";

        if (self.cgv_source_db.cgv_dbtype == "Oracle"):
            # Oracle需要增加rowid列，可以应对无索引的场景
            if (len(self.cgv_select_cols) > 0):
                v_sql = "select /*+ parallel(a," + str(self.cgv_process_parallel) + \
                ") */ a.rowid || '''' as rowid," + self.cgv_select_cols + " from " + self.cgv_tab_owner_source + "." + \
                self.cgv_tab_name_source + " a " + self.cgv_sql_where;

                v_estimate_sql = "select a.rowid," + self.cgv_select_cols + " from " + self.cgv_tab_owner_source + "." + \
                self.cgv_tab_name_source + " a ";
            else:
                v_sql = "select /*+ parallel(a," + str(self.cgv_process_parallel) + \
                ") */ a.rowid || '''' as rowid,a.* from " + self.cgv_tab_owner_source + "." + \
                self.cgv_tab_name_source + " a " + self.cgv_sql_where;

                v_estimate_sql = "select a.rowid || '''' as rowid,a.* from " + self.cgv_tab_owner_source + "." + \
                self.cgv_tab_name_source + " a ";

            # 统计行数sql
            v_count_sql = "select /*+ parallel(a," + str(self.cgv_process_parallel) + \
                ") */ count(*) as row_counts from " + self.cgv_tab_owner_source + "." + \
                self.cgv_tab_name_source + " a " + self.cgv_sql_where;

        if (self.cgv_source_db.cgv_dbtype == "MySQL"):
            # MySQL直接进行拼接
            if (len(self.cgv_select_cols) > 0):
                v_sql = "select " + self.cgv_select_cols + " from " + self.cgv_tab_owner_source + "." + \
                self.cgv_tab_name_source + " " + self.cgv_sql_where;

                v_estimate_sql = "select " + self.cgv_select_cols + " from " + self.cgv_tab_owner_source + "." + \
                self.cgv_tab_name_source + " limit 100 ";
            else:
                v_sql = "select * from " + self.cgv_tab_owner_source + "." + \
                self.cgv_tab_name_source + " " + self.cgv_sql_where;

                v_estimate_sql = "select * from " + self.cgv_tab_owner_source + "." + \
                self.cgv_tab_name_source + " limit 100 ";


            # 统计行数sql
            v_count_sql = "select count(*) as row_counts from " + self.cgv_tab_owner_source + "." + \
                self.cgv_tab_name_source + " " + self.cgv_sql_where;


        self.cgv_sql = v_sql;
        self.cgv_count_sql = v_count_sql;
        self.cgv_estimate_sql = v_estimate_sql;

    def cgf_get_key_cols(self):
        """根据表名，owner，获取该表的主键／唯一键列名，初始化cgv_key_list，cgv_key_str，cgv_key_sql"""
        try:
            v_sql = "";
            if (self.cgv_source_db.cgv_db_cur):
                if (self.cgv_source_db.cgv_dbtype == "Oracle"):
                    v_sql = """
                        select lower(COLUMN_NAME) as COLUMN_NAME 
                        from dba_ind_columns 
                        where (index_name,index_owner) in
                        (select * from
                        (select INDEX_NAME,INDEX_OWNER
                        from dba_constraints
                        where owner = upper(:owner)
                        and TABLE_NAME = upper(:table_name)
                        and CONSTRAINT_TYPE in ('P','U')
                        and STATUS = 'ENABLED'
                        order by CONSTRAINT_TYPE)
                        where rownum <= 1)
                        order by COLUMN_POSITION
                    """;
                    self.cgv_source_db.cgv_db_cur.execute(v_sql,owner = self.cgv_tab_owner_source,table_name = self.cgv_tab_name_source);
                elif (self.cgv_source_db.cgv_dbtype == "MySQL"):
                    v_sql = """
                        select isf.NAME as index_column 
                        from INFORMATION_SCHEMA.INNODB_SYS_FIELDS as isf 
                        where isf.INDEX_ID=(select isi.INDEX_ID 
                        from INFORMATION_SCHEMA.INNODB_SYS_INDEXES as isi 
                        join INFORMATION_SCHEMA.INNODB_SYS_TABLES as ist 
                        on isi.TABLE_ID=ist.TABLE_ID 
                        where ist.name='%s/%s' 
                        and isi.type in (3,2)  limit 1)
                    """ % (self.cgv_tab_owner_source.lower(),self.cgv_tab_name_source.lower());
                    self.cgv_source_db.cgv_db_cur.execute(v_sql);

                # cgv_key_list，cgv_key_str
                if (len(self.cgv_pkuk_str) < 1):
                    # 清空初始化引入的空值
                    self.cgv_key_list[:] = [];

                    #### key str为pkuk＋sharding ####
                    # pkuk
                    v_pkuk_list = self.cgv_source_db.cgv_db_cur.fetchall();
                    for i in xrange(len(v_pkuk_list)):
                        self.cgv_key_list.append(v_pkuk_list[i][0]);
                        self.cgv_pkuk_str = self.cgv_pkuk_str + str(v_pkuk_list[i][0]) + ",";

                    self.cgv_pkuk_str = self.cgv_pkuk_str.rstrip(",");                    

                    # sharding str
                    if (len(self.cgv_sharding_str) > 0):
                        v_sharding_list = self.cgv_sharding_str.split(",");
                        for i in xrange(len(v_sharding_list)):
                            self.cgv_key_list.append(v_sharding_list[i][0]);

                    # 重新生成key str
                    self.cgv_key_str = "";
                    for i in xrange(len(self.cgv_key_list)):
                        self.cgv_key_str = self.cgv_key_str + str(self.cgv_key_list[i]) + ","

                    self.cgv_key_str =  self.cgv_key_str.rstrip(",");

                    
                    if (len(self.cgv_pkuk_str) < 1 or self.cgv_pkuk_str is None):
                        # 没有UK或PK的表，不做处理
                        self.cgv_meta_hooker.cgf_update_sharding_status(v_sharding_id = self.cgv_sharding_id,
                        v_sharding_status = "error",v_error_info = "No pk and uk,No define!!!");
                        os._exit(0);
                    else:
                        # 反填pkuk str
                        self.cgv_meta_hooker.cgf_update_sharding_pkuk(v_sharding_id = self.cgv_sharding_id,
                            v_pkuk_str = self.cgv_pkuk_str);

                # cgv_key_sql
                self.cgv_key_sql = "select " + self.cgv_key_str + " from " + self.cgv_tab_owner_source + \
                "." + self.cgv_tab_name_source + " " + self.cgv_sql_where;    
        except Exception as e:
            print "QTablePipe.cgf_get_key_cols get error:" + str(e).replace("'","");
            self.cgv_meta_hooker.cgf_update_sharding_status(v_sharding_id = self.cgv_sharding_id,
                v_sharding_status = "error",v_error_info = "QTablePipe.cgf_get_key_cols get error:" + str(e).replace("'",""));
            self.cgv_meta_hooker.cgf_update_task_status(v_task_id = self.cgv_task_id,
                v_task_status = "error",v_error_info = "QTablePipe.cgf_get_key_cols get error:" + str(e).replace("'",""));
            os._exit(0);

    def cgf_get_object_size(self,obj,seen=None):
        """Recursively finds size of objects"""
        size = sys.getsizeof(obj)
        if seen is None:
            seen = set()
        obj_id = id(obj)
        if obj_id in seen:
            return 0
        # Important mark as seen *before* entering recursion to gracefully handle
        # self-referential objects
        seen.add(obj_id)
        if isinstance(obj, dict):
            size += sum([self.cgf_get_object_size(v, seen) for v in obj.values()])
            size += sum([self.cgf_get_object_size(k, seen) for k in obj.keys()])
        elif hasattr(obj, '__dict__'):
            size += self.cgf_get_object_size(obj.__dict__, seen)
        elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, bytearray)):
            size += sum([self.cgf_get_object_size(i, seen) for i in obj])
        return size

    def cgf_estimate_blockrows(self,v_set_col_desc = True,v_count_off = True):
        """根据v_fetch_MB评估fetch的行数，统计SQL要处理的行数，并更新到metahooker"""
        try:
            # 更新任务信息，任务启动，btime刷新
            self.cgv_meta_hooker.cgf_update_task_status(v_task_id = self.cgv_task_id,
                v_task_status = "gooooing",v_error_info = "");
            self.cgv_meta_hooker.cgf_update_task_btime(v_task_id = self.cgv_task_id);

            ################################## 评估fetch mb的行数 ##################################
            self.cgv_source_db.cgf_reconnect_db();

            v_data_list = [];
            v_data_size = 0;
            v_row_count = 0;

            # 采用estimate sql进行行数评估，mysql已经自带limit 100，oracle没关系，直接fetchmany
            if (self.cgv_source_db.cgv_db_cur):
                # 根据sql获取列信息
                self.cgv_source_db.cgv_db_cur.execute(self.cgv_estimate_sql);
                if (v_set_col_desc == True):
                    self.cgv_curcol_desc_list = self.cgv_source_db.cgv_db_cur.description;

                # 评估完全的sql数据，一个block能支持的行数
                v_data_list = self.cgv_source_db.cgv_db_cur.fetchmany(100);
                v_data_size = self.cgf_get_object_size(v_data_list);
            
            # 除数不能为零
            if (len(v_data_list) > 0 and v_data_size > 0):
                self.cgv_block_rows = self.cgv_fetch_MB * 1024 * 1024 / (v_data_size / len(v_data_list));
            else:
                self.cgv_block_rows = 250000;

            print str(self.cgv_block_rows);
            ################################## 统计tabsharding的数据行数，较花时间 ##################################
            self.cgv_source_db.cgf_reconnect_db();
            # count统计不涉及到大量数据返回，oracle／mysql不用区别游标
            if (v_count_off == True):
                # 查询行数
                if (self.cgv_source_db.cgv_db_cur):
                    # 该tabshard进入到统计状态
                    self.cgv_meta_hooker.cgf_update_sharding_status(v_sharding_id = self.cgv_sharding_id,
                        v_sharding_status = "counting",v_error_info = "");

                    # 计数开始
                    self.cgv_meta_hooker.cgf_update_sharding_bcountoff(v_sharding_id = self.cgv_sharding_id);

                    # counting............
                    self.cgv_source_db.cgv_db_cur.execute(self.cgv_count_sql);
                    v_row_count = self.cgv_source_db.cgv_db_cur.fetchone()[0];

                    # 计数结束
                    self.cgv_meta_hooker.cgf_update_sharding_status(v_sharding_id = self.cgv_sharding_id,
                        v_sharding_status = "counted",v_error_info = "");
                    self.cgv_meta_hooker.cgf_update_sharding_ecountoff(v_sharding_id = self.cgv_sharding_id);
                    
                # 更新tabshard rows
                self.cgv_meta_hooker.cgf_update_sharding_trows(v_sharding_id = self.cgv_sharding_id,
                    v_sharding_rows = v_row_count,v_type = "final");
                # 累加virtualtable的rows
                self.cgv_meta_hooker.cgf_update_vtable_trows(v_vtable_id = self.cgv_vtable_id,v_table_rows = v_row_count,v_type = "add");
        except Exception as e:
            # estimate出错，子进程直接退出
            print "QTablePipe.cgf_estimate_blockrows get error:" + str(e).replace("'","");
            self.cgv_meta_hooker.cgf_update_sharding_status(v_sharding_id = self.cgv_sharding_id,
                v_sharding_status = "error",v_error_info = "QTablePipe.cgf_estimate_blockrows get error:" + str(e).replace("'",""));
            self.cgv_meta_hooker.cgf_update_task_status(v_task_id = self.cgv_task_id,
                v_task_status = "error",v_error_info = "QTablePipe.cgf_estimate_blockrows get error:" + str(e).replace("'",""));
            os._exit(0);

    # 函数获取列名对应的列标
    def cgf_get_col_position(self,v_column_name):
        return self.cgv_col_pos_list.get(v_column_name.lower());

    # 函数获取列标对应的列名
    def cgf_get_pos_colname(self,v_col_pos):
        return self.cgv_pos_col_list.get(v_col_pos);

    # 初始化key列的列标,如果key列不存在，子进程直接退出
    def cgf_init_keycol_position(self):
        for i in xrange(len(self.cgv_key_list)):
            v_position = self.cgf_get_col_position(self.cgv_key_list[i]);
            if (v_position is None):
                print "QTablePipe.cgf_init_keycol_position get error:" + str(self.cgv_key_list[i]) + \
                " dose not exist!!!";
                self.cgv_meta_hooker.cgf_update_sharding_status(v_sharding_id = self.cgv_sharding_id,
                v_sharding_status = "error",
                v_error_info = "QTablePipe.cgf_init_keycol_position get error:" + \
                str(self.cgv_key_list[i]) + " dose not exist!!!");
                os._exit(0);
            else:
                self.cgv_keycol_position.append(v_position);

    # 初始化数据列的列标，如果数据列布存在，子进程直接退出
    def cgf_init_datacol_position(self):
        # 因为目标端，源端的字段顺序可能不一样，所以data列名先做好排序，cgv_datacol_position的顺序会一致，出来的hash才不会错
        self.cgv_datacol.sort();
        for i in xrange(len(self.cgv_datacol)):
            v_position = self.cgf_get_col_position(self.cgv_datacol[i]);
            if (v_position is None):
                print "QTablePipe.cgf_init_datacol_position get error:" + str(self.cgv_datacol[i]) + \
                " dose not exist!!!";
                self.cgv_meta_hooker.cgf_update_sharding_status(v_sharding_id = self.cgv_sharding_id,
                v_sharding_status = "error",
                v_error_info = "QTablePipe.cgf_init_datacol_position get error:" + \
                str(sself.cgv_datacol[i]) + " dose not exist!!!");
                os._exit(0);
            else:
                self.cgv_datacol_position.append(v_position);

    # 初始化cgv_col_pos_list,cgv_datacol
    def cgf_init_datacol_kvlist(self):
        try:
            if (self.cgv_curcol_desc_list):
                v_datacol_all = [];
                # 填充kvlist和datacol_all
                for i in xrange(len(self.cgv_curcol_desc_list)):
                    # 初始化col:pos字典
                    self.cgv_col_pos_list[self.cgv_curcol_desc_list[i][0].lower()] = i;
                    # 初始化pos:col字典
                    self.cgv_pos_col_list[i] = self.cgv_curcol_desc_list[i][0].lower();
                    # oracle数据库，rowid列进行特殊处理
                    if (self.cgv_source_db.cgv_dbtype == "Oracle"):
                        # rowid特殊列，不算入datacol
                        if (self.cgv_curcol_desc_list[i][0].lower() != "rowid"):
                            v_datacol_all.append(self.cgv_curcol_desc_list[i][0].lower());
                    else:
                        # 普通数据库，正常处理
                        v_datacol_all.append(self.cgv_curcol_desc_list[i][0].lower());

                # data列需要从all中排除key列
                self.cgv_datacol = self.cgf_diff_list(v_datacol_all,self.cgv_key_list);
        except Exception as e:
            print "QTablePipe.cgf_init_datacol_kvlist get error :" + str(e).replace("'","");

    # 查找a有b没有的元素并返回
    def cgf_diff_list(self,v_list_a,v_list_b):
        v_list_return = [];
        v_is_append = True;
        for i in xrange(len(v_list_a)):
            # 默认做append，最终看后续的查找情况
            v_is_append = True;

            for j in xrange(len(v_list_b)):
                # 如果找到，本轮结束，控制标志位不append
                if (v_list_a[i] == v_list_b[j]):
                    v_is_append = False;
                    break;

            if (v_is_append == True):
                v_list_return.append(v_list_a[i]);

        return v_list_return;

    def cgf_unicode_to_string(self,v_input_ustr = None):
        """unicode数据转换为str"""
        if (isinstance(v_input_ustr,unicode)):
            return v_input_ustr.encode("utf-8");

        return v_input_ustr;
    
    def cgf_get_keystr_value(self,v_row):
        """获取keystr的字符拼接value，即每一行的key值,为了便于追溯还原key列，用^进行分割"""
        v_key_str = "";
        v_split_str = unichr(8364).encode("utf-8");
        for i in xrange(len(self.cgv_keycol_position)):
            # 分隔符为 €
            v_key_str = v_key_str + str(self.cgf_unicode_to_string(v_row[self.cgv_keycol_position[i]])) + v_split_str;

        return v_key_str.rstrip(v_split_str);

    # 获取datastr的字符拼接value，即每一行的datavalue值
    def cgf_get_datastr_value(self,v_row = None):
        v_data_str = "";
        v_split_str = unichr(8364).encode("utf-8");
        for i in xrange(len(self.cgv_datacol_position)):
            # 分隔符为 €
            v_data_str = v_data_str + str(self.cgf_unicode_to_string(v_row[self.cgv_datacol_position[i]])) + v_split_str;

        if (self.cgv_line_factor is None):
            return v_data_str.rstrip(v_split_str);
        else:
            # 换行填充因子存在，进行替换
            return v_data_str.rstrip(v_split_str).replace("\r",self.cgv_line_factor).replace("\n",self.cgv_line_factor);

    # 获取data数据的json字符拼接
    def cgf_get_datastr_json(self,v_row = None):
        # a = '{"a":"1902:02:03","b":2,"c":3,"d":4,"e":5}';
        v_datastr_json = "";
        for i in xrange(len(self.cgv_datacol_position)):
            # 分隔符为 €
            v_col_name = self.cgf_get_pos_colname(v_col_pos = self.cgv_datacol_position[i]);
            v_data_str = str(self.cgf_unicode_to_string(v_row[self.cgv_datacol_position[i]]));
            v_datastr_json = v_datastr_json + '"' + v_col_name + '"' + ':' + '"' + v_data_str + '"' + ','

        v_datastr_json = '{' + v_datastr_json.rstrip(',') + '}';

        if (self.cgv_line_factor is None):
            return v_datastr_json;
        else:
            # 换行填充因子存在，进行替换
            return v_datastr_json.replace("\r",self.cgv_line_factor).replace("\n",self.cgv_line_factor);

    # 生成keystr,hashvalue表
    def cgf_create_hashvalue_table(self):
        if (len(self.cgv_data_block) > 0):
            try:
                v_rowid_position = 0;
                v_special_key = "";

                # 确认rowid列
                if (self.cgv_source_db.cgv_dbtype == "Oracle"):
                    v_rowid_position = self.cgf_get_col_position("rowid");
                # hashvalue生成
                for i in xrange(len(self.cgv_data_block)):
                    # 生成keystr
                    v_key_str = self.cgf_get_keystr_value(self.cgv_data_block[i]);
                    # 生成datastr
                    v_data_str = self.cgf_get_datastr_value(self.cgv_data_block[i]);
                    # 生成rowid数据,仅仅oracle特有，其他数据库该列为空
                    if (self.cgv_source_db.cgv_dbtype == "Oracle"):
                        v_special_key = self.cgv_data_block[i][v_rowid_position];

                    # 生成hash(datacol),默认采用hash函数，mac本每秒330万，md5每秒160万
                    if (self.cgv_hash_type == "hash"):
                        v_hash_value = hash(v_data_str);
                    elif (self.cgv_hash_type == "md5"):
                        v_hash_value = hashlib.md5(v_data_str);

                    #key／value/special_key/sharding_id入hash索引
                    if (self.cgv_is_need_data == False):
                        # 不存档数据串
                        self.cgv_keystr_hashvalue.append([v_key_str,v_hash_value,v_special_key,self.cgv_sharding_id]);
                    else:
                        # 存档数据串
                        self.cgv_keystr_hashvalue.append([v_key_str,v_hash_value,v_special_key,self.cgv_sharding_id,v_data_str]);

                # 清空cgv_data_block数据，减少序列化空间
                self.cgv_data_block[:] = [];
            except Exception as e:
                print "QTablePipe.cgf_create_hashvalue_table get error : " + str(e).replace("'","");
                self.cgv_meta_hooker.cgf_update_sharding_status(v_sharding_id = self.cgv_sharding_id,
                v_sharding_status = "error",v_error_info = "QTablePipe.cgf_create_hashvalue_table get error : " + str(e).replace("'",""));
                self.cgv_meta_hooker.cgf_update_task_status(v_task_id = self.cgv_task_id,
                v_task_status = "error",v_error_info = "QTablePipe.cgf_create_hashvalue_table get error:" + str(e).replace("'",""));
                os._exit(0);


    def cgf_data_transfor(self):
        """ 抽取数据，计算key－hashvalue，并录入到对比库表中"""
        try:
            #初始化连接，避免mysql的坑
            self.cgv_source_db.cgf_reconnect_db();

            # sharding进入pulling数据拉取模式，设置拉取开始时间
            self.cgv_meta_hooker.cgf_update_sharding_status(v_sharding_id = self.cgv_sharding_id,
                v_sharding_status = "pulling",v_error_info = "");
            self.cgv_meta_hooker.cgf_update_sharding_bpull(v_sharding_id = self.cgv_sharding_id);

            v_is_continue = True;
            v_count_rows = 0;
            v_mysql_rows = 0;

            # oracle数据库的抽取转换
            if (self.cgv_source_db.cgv_dbtype == "Oracle"):
                # 字符集设置初始化
                self.cgv_source_charset = self.cgv_source_db.cgf_charset_set(v_table_name = self.cgv_tab_name_source);

                if (self.cgv_source_db.cgv_db_cur):
                    self.cgv_source_db.cgv_db_cur.execute(self.cgv_sql);
                    while (v_is_continue):
                        self.cgv_data_block = self.cgv_source_db.cgv_db_cur.fetchmany(self.cgv_block_rows);
                        if (len(self.cgv_data_block) > 0):
                            
                            # 生成源端数据的key－hashv
                            self.cgf_create_hashvalue_table();

                            # 设定目标端字符集环境
                            self.cgv_target_charset = self.cgv_crusher_db.cgf_charset_set(v_table_name = self.cgv_tab_name_target);

                            # 写入对比端数据库
                            if (self.cgv_crusher_db.cgv_db_cur):
                                if (self.cgv_is_need_data == False):
                                    v_sql = "insert into " + self.cgv_tab_owner_target + "." + self.cgv_tab_name_target + \
                                    " (keystr,hashvalue,special_key,tabsharding_id) values (:keystr,:hashvalue,:special_key,:tabsharding_id)";
                                else:
                                    v_sql = "insert into " + self.cgv_tab_owner_target + "." + self.cgv_tab_name_target + \
                                    " (keystr,hashvalue,special_key,tabsharding_id,datastr) values (:keystr,:hashvalue,:special_key,:tabsharding_id,:datastr)";

                                if (len(self.cgv_keystr_hashvalue) > 0):
                                    self.cgv_crusher_db.cgv_db_cur.executemany(v_sql,self.cgv_keystr_hashvalue[0:len(self.cgv_keystr_hashvalue)]);
                                    self.cgv_crusher_db.cgv_conn_db.commit();

                            # 清空self.cgv_keystr_hashvalue
                            self.cgv_keystr_hashvalue[:] = [];

                            # 统计rows
                            v_count_rows = v_count_rows + len(self.cgv_data_block);
                        else:
                            v_is_continue = False;

                    # 完成数据抽取，更新sharding元数据，task任务更新状态
                    self.cgv_meta_hooker.cgf_update_sharding_epull(v_sharding_id = self.cgv_sharding_id);
                    self.cgv_meta_hooker.cgf_update_sharding_status(v_sharding_id = self.cgv_sharding_id,
                        v_sharding_status = "pulled",v_error_info = "");
                    self.cgv_meta_hooker.cgf_update_task_status(v_task_id = self.cgv_task_id,
                        v_task_status = "finished",v_error_info = "");
                    self.cgv_meta_hooker.cgf_update_task_etime(v_task_id = self.cgv_task_id);

            # MySQL数据库的抽取转换
            elif (self.cgv_source_db.cgv_dbtype == "MySQL"): 
                # 设置MySQL的字符集环境
                self.cgv_source_charset = self.cgv_source_db.cgf_charset_set(v_table_name = self.cgv_tab_name_source);

                if (self.cgv_source_db.cgv_db_sscur):                    
                    self.cgv_source_db.cgv_db_sscur.execute(self.cgv_sql);
                    # mysql的游标坑爹，fetch会消耗内存，采用迭代模式获取数据，到达批次数量，就提交到目标库
                    for v_rows in self.cgv_source_db.cgv_db_sscur:
                        # 数据写入内存block
                        self.cgv_data_block.append(v_rows);
                        # 计数器增加
                        v_mysql_rows = v_mysql_rows + 1;
                        # 到达批次数量上限，刷新入目标库
                        if (v_mysql_rows >= self.cgv_block_rows):
                            
                            # 生成源端数据的key－hashv
                            self.cgf_create_hashvalue_table();
                            
                            # 设置oracle的字符集环境
                            self.cgv_target_charset = self.cgv_crusher_db.cgf_charset_set(v_table_name = self.cgv_tab_name_target);

                            # 写入对比端数据库
                            if (self.cgv_crusher_db.cgv_db_cur):
                                if (self.cgv_is_need_data == False):
                                    v_sql = "insert into " + self.cgv_tab_owner_target + "." + self.cgv_tab_name_target + \
                                    " (keystr,hashvalue,special_key,tabsharding_id) values (:keystr,:hashvalue,:special_key,:tabsharding_id)";
                                else:
                                    v_sql = "insert into " + self.cgv_tab_owner_target + "." + self.cgv_tab_name_target + \
                                    " (keystr,hashvalue,special_key,tabsharding_id,datastr) values (:keystr,:hashvalue,:special_key,:tabsharding_id,:datastr)";

                                if (len(self.cgv_keystr_hashvalue) > 0):
                                    self.cgv_crusher_db.cgv_db_cur.executemany(v_sql,self.cgv_keystr_hashvalue[0:len(self.cgv_keystr_hashvalue)]);
                                    self.cgv_crusher_db.cgv_conn_db.commit();

                            # 清空self.cgv_keystr_hashvalue
                            self.cgv_keystr_hashvalue[:] = [];

                            # 统计rows
                            v_count_rows = v_count_rows + len(self.cgv_data_block);
                            # MySQL rows置为0
                            v_mysql_rows = 0;
                    
                    ############################################### 剩余数据补齐 #########################################
                    # 生成源端数据的key－hashv
                    self.cgf_create_hashvalue_table();

                    # 设置oracle的字符集环境
                    self.cgv_target_charset = self.cgv_crusher_db.cgf_charset_set(v_table_name = self.cgv_tab_name_target);

                    # 写入对比端数据库
                    if (self.cgv_crusher_db.cgv_db_cur):
                        if (self.cgv_is_need_data == False):
                            v_sql = "insert into " + self.cgv_tab_owner_target + "." + self.cgv_tab_name_target + \
                                " (keystr,hashvalue,special_key,tabsharding_id) values (:keystr,:hashvalue,:special_key,:tabsharding_id)";
                        else:
                            v_sql = "insert into " + self.cgv_tab_owner_target + "." + self.cgv_tab_name_target + \
                                " (keystr,hashvalue,special_key,tabsharding_id,datastr) values (:keystr,:hashvalue,:special_key,:tabsharding_id,:datastr)";

                        if (len(self.cgv_keystr_hashvalue) > 0):
                            self.cgv_crusher_db.cgv_db_cur.executemany(v_sql,self.cgv_keystr_hashvalue[0:len(self.cgv_keystr_hashvalue)]);
                            self.cgv_crusher_db.cgv_conn_db.commit();

                    # 清空self.cgv_keystr_hashvalue
                    self.cgv_keystr_hashvalue[:] = [];

                    # 统计rows
                    v_count_rows = v_count_rows + len(self.cgv_data_block);
                    ############################################### 剩余数据补齐 #########################################

                    # 完成数据抽取，更新sharding元数据，task任务更新状态
                    self.cgv_meta_hooker.cgf_update_sharding_epull(v_sharding_id = self.cgv_sharding_id);
                    self.cgv_meta_hooker.cgf_update_sharding_status(v_sharding_id = self.cgv_sharding_id,
                        v_sharding_status = "pulled",v_error_info = "");
                    self.cgv_meta_hooker.cgf_update_task_status(v_task_id = self.cgv_task_id,
                        v_task_status = "finished",v_error_info = "");
                    self.cgv_meta_hooker.cgf_update_task_etime(v_task_id = self.cgv_task_id);

            #初始化连接，避免mysql的坑
            self.cgv_source_db.cgf_reconnect_db();
        except Exception as e:
            print "QTablePipe.cgf_data_transfor get error :" + str(e).replace("'","");
            #涉及对比库端事务的进行回滚
            self.cgv_crusher_db.cgv_conn_db.rollback();
            # 处理出错，更新sharding的状态为error
            self.cgv_meta_hooker.cgf_update_sharding_status(v_sharding_id = self.cgv_sharding_id,
                v_sharding_status = "error",v_error_info = "QTablePipe.cgf_data_transfor get error :" + str(e).replace("'",""));
            self.cgv_meta_hooker.cgf_update_task_status(v_task_id = self.cgv_task_id,
                v_task_status = "error",v_error_info = "QTablePipe.cgf_data_transfor get error:" + str(e).replace("'",""));
            os._exit(0);

class QTableCrusher():
    """负责数据的对比校验，支持数据block，数据表，对比完毕生成结果"""
    def __init__(self,v_task_id = 0,v_twins_id = 0,v_process_parallel = 1,
                v_crusher_db = None,v_meta_hooker = None,
                v_need_surplus = False,v_literate_check = False):
        # taskid,twinsid
        self.cgv_task_id = v_task_id;
        self.cgv_twins_id = v_twins_id;
        # 查询并行度，oracle有效
        self.cgv_process_parallel = v_process_parallel;
        # 数据库信息
        self.cgv_crusher_db = v_crusher_db;
        # 生成一个dml cursor
        self.cgv_dml_cur = self.cgv_crusher_db.cgv_conn_db.cursor();
        # 元数据信息维护
        self.cgv_meta_hooker = v_meta_hooker;

        #数据block
        self.cgv_data_block = [];
        self.cgv_missing_block = [];
        self.cgv_dismatch_block = [];
        self.cgv_surplus_block = [];

        # twins名称,状态
        self.cgv_twins_name = "";
        self.cgv_twins_status = "";
        # 相关vtable的信息，包括single name
        self.cgv_source_vtname = "";
        self.cgv_target_vtname = "";
        self.cgv_source_stname = "";
        self.cgv_target_stname = "";
        # vtable 行数
        self.cgv_source_rows = 0;
        self.cgv_target_rows = 0;
        # 是否需要校验目标端多出的数据，默认不校验
        self.cgv_need_surplus = v_need_surplus;
        # 是否需要字面校验，默认不开启，太耗费时间
        self.cgv_literate_check = v_literate_check;

        # 初始化
        self.cgf_init_twins_info();

    def cgf_init_twins_info(self):
        """初始化twins，vtable相关的信息"""
        v_twins_info = self.cgv_meta_hooker.cgf_get_twins_info(v_twins_id = self.cgv_twins_id);
        self.cgv_twins_name = v_twins_info.get("twins_name");
        self.cgv_source_vtname = v_twins_info.get("source_vtable_name");
        self.cgv_target_vtname = v_twins_info.get("target_vtable_name");
        self.cgv_source_stname = v_twins_info.get("source_stable_name");
        self.cgv_target_stname = v_twins_info.get("target_stable_name");
        self.cgv_source_rows = v_twins_info.get("source_rows");
        self.cgv_target_rows = v_twins_info.get("target_rows");
        self.cgv_twins_status = v_twins_info.get("twins_status");

    def cgf_dismiss_data_split(self):
        """划分dismatch和missing block"""
        self.cgv_missing_block[:] = [];
        self.cgv_dismatch_block[:] = [];

        for i in xrange(len(self.cgv_data_block)):
            # 目前选的是target_sharding_id，为空表示missing，不为空表示dismatch
            # 请一定注意列号，现在是写死的
            if (self.cgv_data_block[i][6] is None):
                self.cgv_missing_block.append(self.cgv_data_block[i]);
            else:
                self.cgv_dismatch_block.append(self.cgv_data_block[i]);

    def cgf_twinstab_crusher(self):
        """超级粉碎机，生产miss/dismatch/surplus数据"""
        try:
            # 更新twins信息，task信息，进入干活状态，checking.....
            self.cgv_meta_hooker.cgf_update_twins_status(v_twins_id = self.cgv_twins_id,
                v_twins_status = "checking",v_error_info = "");
            self.cgv_meta_hooker.cgf_update_twins_bcheck(v_twins_id = self.cgv_twins_id);
            self.cgv_meta_hooker.cgf_update_task_status(v_task_id = self.cgv_task_id,
                v_task_status = "gooooing",v_error_info = "");
            self.cgv_meta_hooker.cgf_update_task_btime(v_task_id = self.cgv_task_id);

            # 确定SQL，有Oracle的hashjoin，真是大神器，节省很多代码
            v_sql = "";
            v_is_continue = True;
            v_dismatch_rows = 0;
            v_missing_rows = 0;
            v_surplus_rows = 0;

            if (self.cgv_crusher_db.cgv_db_cur):
                ##################################dismatch/missing rows处理##################################
                if (self.cgv_literate_check == True):
                    # 字面值比较
                    v_sql = """select /*+ use_hash(s,t) parallel(s,%s) parallel(t,%s) */
                    %s as twins_id,'%s' as twins_name,
                    s.keystr,s.rowid || '' as source_rowid,t.rowid || '' as target_rowid,
                    s.TABSHARDING_ID as source_sharding_id,
                    t.TABSHARDING_ID as target_sharding_id,
                    '%s' as source_stable,'%s' as target_stable,'new' as row_status,%s as task_id
                    from %s s,%s t
                    where s.keystr = t.keystr(+)
                    and (DBMS_LOB.SUBSTR(s.datastr) != DBMS_LOB.SUBSTR(t.datastr) or t.keystr is null)""" % \
                    (self.cgv_process_parallel,self.cgv_process_parallel,self.cgv_twins_id,self.cgv_twins_name,
                     self.cgv_source_stname,self.cgv_target_stname,self.cgv_task_id,self.cgv_source_stname,self.cgv_target_stname);
                    print v_sql;
                elif (self.cgv_literate_check == False):
                    # hashvalue比较
                    v_sql = """select /*+ use_hash(s,t) parallel(s,%s) parallel(t,%s) */
                    %s as twins_id,'%s' as twins_name,
                    s.keystr,s.rowid || '' as source_rowid,t.rowid || '' as target_rowid,
                    s.TABSHARDING_ID as source_sharding_id,
                    t.TABSHARDING_ID as target_sharding_id,
                    '%s' as source_stable,'%s' as target_stable,'new' as row_status,%s as task_id
                    from %s s,%s t
                    where s.keystr = t.keystr(+)
                    and (s.hashvalue != t.hashvalue or t.keystr is null)""" % \
                    (self.cgv_process_parallel,self.cgv_process_parallel,self.cgv_twins_id,self.cgv_twins_name,
                     self.cgv_source_stname,self.cgv_target_stname,self.cgv_task_id,self.cgv_source_stname,self.cgv_target_stname);
                    print v_sql;

                self.cgv_crusher_db.cgv_db_cur.execute(v_sql);

                while (v_is_continue):
                    # 500000万行，大约是64+16=80MB的内存大小，按照q_check_surplus_rows估算
                    self.cgv_data_block = self.cgv_crusher_db.cgv_db_cur.fetchmany(500000);

                    if (len(self.cgv_data_block) > 0):
                        # 分割数据
                        self.cgf_dismiss_data_split();

                        if (len(self.cgv_dismatch_block) > 0):
                            # dismatch结果记录
                            if (self.cgv_dml_cur):
                                v_dismatch_sql = """insert into q_check_dismatch_rows
                                        (id,twins_id,twins_name,keystr,source_rowid,target_rowid,
                                        source_sharding_id,target_sharding_id,source_stable,target_stable,row_status,task_id)
                                        values
                                        (seq_dismatch_rows.nextval,:twins_id,:twins_name,:keystr,:source_rowid,:target_rowid,
                                        :source_sharding_id,:target_sharding_id,:source_stable,:target_stable,:row_status,:task_id)""";
                                self.cgv_dml_cur.executemany(v_dismatch_sql,self.cgv_dismatch_block[0:len(self.cgv_dismatch_block)]);
                                self.cgv_crusher_db.cgv_conn_db.commit();

                                # 计数器累计
                                v_dismatch_rows = v_dismatch_rows + len(self.cgv_dismatch_block);
                        if (len(self.cgv_missing_block) > 0):
                            # missing结果记录
                            if (self.cgv_dml_cur):
                                v_missing_sql = """insert into q_check_missing_rows
                                        (id,twins_id,twins_name,keystr,source_rowid,target_rowid,
                                        source_sharding_id,target_sharding_id,source_stable,target_stable,row_status,task_id)
                                        values
                                        (seq_missing_rows.nextval,:twins_id,:twins_name,:keystr,:source_rowid,:target_rowid,
                                        :source_sharding_id,:target_sharding_id,:source_stable,:target_stable,:row_status,:task_id)""";
                                self.cgv_dml_cur.executemany(v_missing_sql,self.cgv_missing_block[0:len(self.cgv_missing_block)]);
                                self.cgv_crusher_db.cgv_conn_db.commit();

                                # 计数器累计
                                v_missing_rows = v_missing_rows + len(self.cgv_missing_block);
                    else:
                        v_is_continue = False;

                ##################################surplus rows处理##################################
                if (self.cgv_need_surplus == True):
                    # 需要检查surplus rows，标志重置
                    v_is_continue = True;

                    # 多余的rows，只需要根据keystr做null判断即可，不存在字面值和hash值的比较
                    v_sql = """select /*+ use_hash(t,s) parallel(s,%s) parallel(t,%s) */
                    %s as twins_id,'%s' as twins_name,
                    t.keystr,s.rowid || '' as source_rowid,t.rowid || '' as target_rowid,
                    s.TABSHARDING_ID as source_sharding_id,
                    t.TABSHARDING_ID as target_sharding_id,
                    '%s' as source_stable,'%s' as target_stable,'new' as row_status,%s as task_id
                    from %s t,%s s
                    where t.keystr = s.keystr(+)
                    and s.keystr is null""" % \
                    (self.cgv_process_parallel,self.cgv_process_parallel,self.cgv_twins_id,self.cgv_twins_name,
                     self.cgv_source_stname,self.cgv_target_stname,self.cgv_task_id,self.cgv_target_stname,self.cgv_source_stname);
                    print v_sql;

                    self.cgv_crusher_db.cgv_db_cur.execute(v_sql);

                    while (v_is_continue):
                        # 500000万行，大约是64+16=80MB的内存大小，按照q_check_surplus_rows估算
                        self.cgv_surplus_block = self.cgv_crusher_db.cgv_db_cur.fetchmany(500000);

                        if (len(self.cgv_surplus_block) > 0):
                            if (self.cgv_dml_cur):
                                v_surplus_sql = """insert into q_check_surplus_rows
                                            (id,twins_id,twins_name,keystr,source_rowid,target_rowid,
                                            source_sharding_id,target_sharding_id,source_stable,target_stable,row_status,task_id)
                                            values
                                            (seq_surplus_rows.nextval,:twins_id,:twins_name,:keystr,:source_rowid,:target_rowid,
                                            :source_sharding_id,:target_sharding_id,:source_stable,:target_stable,:row_status,:task_id)""";
                                self.cgv_dml_cur.executemany(v_surplus_sql,self.cgv_surplus_block[0:len(self.cgv_surplus_block)]);
                                self.cgv_crusher_db.cgv_conn_db.commit();

                                # 计数器累计
                                v_surplus_rows = v_surplus_rows + len(self.cgv_surplus_block);
                        else:
                            v_is_continue = False;

            # 更新twins信息，task信息，干活结束
            self.cgv_meta_hooker.cgf_update_twins_status(v_twins_id = self.cgv_twins_id,
                v_twins_status = "checked",v_error_info = "");
            self.cgv_meta_hooker.cgf_update_twins_echeck(v_twins_id = self.cgv_twins_id);
            self.cgv_meta_hooker.cgf_update_task_status(v_task_id = self.cgv_task_id,
                v_task_status = "finished",v_error_info = "");
            self.cgv_meta_hooker.cgf_update_task_etime(v_task_id = self.cgv_task_id);
            self.cgv_meta_hooker.cgf_update_twins_dismatch(v_twins_id = self.cgv_twins_id,v_dismatch_rows = v_dismatch_rows);
            self.cgv_meta_hooker.cgf_update_twins_missing(v_twins_id = self.cgv_twins_id,v_missing_rows = v_missing_rows);
            self.cgv_meta_hooker.cgf_update_twins_surplus(v_twins_id = self.cgv_twins_id,v_surplus_rows = v_surplus_rows);
        except Exception as e:
            print "QTableCrusher.cgf_twinstab_crusher get error :" + str(e).replace("'","");
            self.cgv_crusher_db.cgv_conn_db.rollback();
            self.cgv_meta_hooker.cgf_update_twins_status(v_twins_id = self.cgv_twins_id,v_twins_status = "error",
                v_error_info = "QTableCrusher.cgf_twinstab_crusher get error :" + str(e).replace("'",""));
            self.cgv_meta_hooker.cgf_update_task_status(v_task_id = self.cgv_task_id,v_task_status = "error",
                v_error_info = "QTableCrusher.cgf_twinstab_crusher get error :" + str(e).replace("'",""));

class QDataViewX():
    """负责对比结果的详细数据列表生成"""
    def __init__(self, arg):
        super(QView, self).__init__()
        self.arg = arg
        
class QDataRepair():
    """负责对比数据的修复"""
    def __init__(self, arg):
        super(QDataRepair, self).__init__()
        self.arg = arg

class QDataRecheck():
    """负责结果数据的再次校验，可以调用前面的crusher"""
    def __init__(self, arg):
        super(QDataRecheck, self).__init__()
        self.arg = arg

if __name__ == "__main__":
    # 设置环境变量，主要针对于Oracle字符集
    v_source_db = QConnectDB(v_ip = "10.10.30.15",v_port = 3306,v_sid_or_dbname = "qbench",
                v_db_user = "qbench",v_dbpwd = "qbench",v_dbtype = "MySQL");

    v_crusher_db = QConnectDB(v_ip = "10.10.100.141",v_port = 1522,v_sid_or_dbname = "RACOne_1",
                v_db_user = "qbench",v_dbpwd = "qbench",v_dbtype = "Oracle");

    v_meta_db = QConnectDB(v_ip = "10.10.30.15",v_port = 3306,v_sid_or_dbname = "qbench",
                v_db_user = "qbench",v_dbpwd = "qbench",v_dbtype = "MySQL");

    v_meta_hooker = QMetaHooker(v_meta_db = v_meta_db);

    #v_mycat = QTableCat(v_meta_hooker = v_meta_hooker,v_crusher_db = v_crusher_db);
    #v_mycat.cgf_excel_reader(v_excel_name = "/home/oracle/chun.luo/tablelist.xlsx",v_excel_type = "tablelist");
    #v_mycat.cgf_virtual_tabscope();
    #v_mycat.cgf_table_meta_gen();
    #v_mycat.cgf_excel_reader(v_excel_name = "/home/oracle/chun.luo/twinslist.xlsx",v_excel_type = "twinslist");
    #v_mycat.cgf_twins_meta_gen();
    #v_sdb_pipe1 = QTablePipe(v_pkuk_str = "",v_sharding_str = "",v_fetch_MB = 256,
    #            v_sql_where = " limit 10000 ",v_select_cols = "",
    #            v_tab_owner_source = "qbench",v_tab_name_source = "my_a",
    #            v_tab_owner_target = "qbench",v_tab_name_target = "qs0000000208",
    #            v_source_db = v_source_db,v_crusher_db = v_crusher_db,v_meta_hooker = v_meta_hooker,
    #            v_process_parallel = 1,v_hash_type = "hash",v_task_id = 20,
    #            v_vtable_id = 18,v_sharding_id = 20);
    #
    #v_sdb_pipe1.cgf_estimate_blockrows(v_set_col_desc = False,v_count_off = True);
    #v_sdb_pipe1.cgf_data_transfor();

    v_table_crusher = QTableCrusher(v_task_id = 40,v_twins_id = 4,v_process_parallel = 1,
                v_crusher_db = v_crusher_db,v_meta_hooker = v_meta_hooker,
                v_need_surplus = True,v_literate_check = True);
    v_table_crusher.cgf_twinstab_crusher();
"""
    v_sdb_pipe1 = QTablePipe(v_key_str = "",v_fetch_MB = 256,
                v_sql_where = "",v_select_cols = "",
                v_tab_owner_source = "qbench",v_tab_name_source = "my_a",
                v_tab_owner_target = "qbench",v_tab_name_target = "qs0000000158",
                v_source_db = v_source_db,v_crusher_db = v_crusher_db,v_meta_hooker = v_meta_hooker,
                v_process_parallel = 1,v_hash_type = "hash",
                v_twins_id = 2,v_sharding_id = 2,v_source_target = "source");

    v_sdb_pipe1.cgf_estimate_blockrows(v_set_col_desc = False,v_count_off = True);
    #v_sdb_pipe1.cgf_data_transfor();

    v_sdb_pipe2.cgf_estimate_blockrows(v_set_col_desc = False,v_count_off = True);
    #v_sdb_pipe2.cgf_data_transfor();

    v_tdb_pipe1.cgf_estimate_blockrows(v_set_col_desc = False,v_count_off = True);        
    #v_tdb_pipe1.cgf_data_transfor();





    #v_db_pipe.cgf_data_transfor();


delete from q_tab_twins_list;
delete from q_sharding_block_list;
delete from q_task_list;
delete from q_table_sharding_list;

drop table q_tab_twins_list;
drop table q_sharding_block_list;
drop table q_task_list;
drop table q_table_sharding_list;

commit;


    v_db_pipe = QTablePipe(v_key_str = "id",v_fetch_MB = 256,
                v_sql_where = "",v_select_cols = "",
                v_tab_owner_source = "qbench",v_tab_name_source = "my_a",
                v_tab_owner_target = "qbench",v_tab_name_target = "qt00000001",
                v_source_db = v_source_db,v_target_db = v_target_db,v_meta_hooker = v_meta_hooker,
                v_process_parallel = 1,v_hash_type = "hash",
                v_twins_id = 2,v_sharding_id = 2,v_source_target = "source");

    v_db_pipe = QTablePipe(v_key_str = "keystr, haShvalue",v_fetch_MB = 1,
                v_sql_where = "where 1 = 2",v_select_cols = "special_key",
                v_tab_owner_target = "sbtest23",v_tab_name_target = "sbtest1",
                v_tab_owner_source = "qbench",v_tab_name_source = "qt00000001",
                v_source_db = v_target_db,v_target_db = v_target_db,v_meta_hooker = v_meta_hooker,
                v_process_parallel = 1,v_hash_type = "hash",
                v_twins_id = 2,v_sharding_id = 2,v_source_target = "source");

    

10.10.30.15 3306  qbench qbench   能不能在这个环境，帮我造一张offer表，offerid是唯一键，数据7千万,offerid就从1递增即可
/usr/local/mysql/bin/mysql -uqbench -pqbench -S /home/mysql/data/mysqldata1/sock/mysql.sock
sbtest23.sbtest1

oracle:
10.10.160.172

print "ID of inserted record is ", int(conn.insert_id()) #最新插入行的主键ID，conn.insert_id()一定要在conn.commit()之前，否则会返回0

create table QT00000001(keystr varchar2(2048),hashvalue varchar2(512),special_key varchar2(256)) pctfree 0 nologging ;
create table QT00000002(keystr varchar2(2048),hashvalue varchar2(512),special_key varchar2(256)) pctfree 0 nologging ;
insert into q_idlist_table(gmt_create) values(CURRENT_TIMESTAMP);


insert into q_tab_twins_list(source_tab_uname,target_tab_uname,source_tab_sname,
target_tab_sname,source_table_rows,target_table_rows,twins_status,source_dbtype,target_dbtype,
source_table_type,target_table_type) values
('sbtest1','sbtest1','qt00000001','qt00000002',0,0,'starting','MySQL','Oracle','sharding','normal');


insert into q_table_sharding_list(tab_twins_id,sharding_table_name,sharding_table_owner,sharding_status,source_target,ip,port,sid_or_dbname,db_user,db_pwd) values
(2,'sbtest1','sbtest23','starting','source','10.10.30.15',3306,'sbtest23','qbench','qbench');

"""

        
        
        


        
        
        
