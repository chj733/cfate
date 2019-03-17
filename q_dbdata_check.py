#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
 ScriptName: q_dbdata_check.py
 Author: chun.luo@woqutech.com
 Create Date: 2019-2-10 22:40
 Modify Author: chun.luo@woqutech.com
 Modify Date: 2019-2-10 22:40
 Remark: 数据对比，是真需求，还是伪需求？
"""

"""
    create_sql:create table mytopic(id number,serial_num number,chat_word varchar2(256),gmt_created date,gmt_modified date,remark varchar2(512));
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
#import MySQLdb
import hashlib
import cPickle

class QBlockStore():
    """维护所有的block信息列表，包括block的所有属性,分配block的信息参照此类"""
    def __init__(self,v_ip = None,v_port = None,v_sid_or_dbname = None,v_db_user = None,v_dbpwd = None):


        self.cgv_ip = v_ip;
        self.cgv_port = v_port;
        self.cgv_sid_or_dbname = v_sid_or_dbname;
        self.cgv_dbuser = v_db_user;
        self.cgv_dbpwd = v_dbpwd;
        self.cgv_conn_db = None;
        self.cgv_db_cur = None;

    def cgf_connect_db(self):
        try:
            self.cgv_conn_db = MySQLdb.Connection(host = self.cgv_ip,user = self.cgv_dbuser,
            passwd = self.cgv_dbpwd,db = self.cgv_sid_or_dbname,port = self.cgv_port);
            self.cgv_db_cur = self.cgv_conn_db.cursor();
        except Exception, e:
            print "QBlockStore.cgf_connect_db metadb connect error:" + self.cgv_ip + ":" + self.cgv_port + ":" + self.cgv_sid_or_dbname;
            print e;
            os._exit(0);


    def cgf_disconnect_db(self):
        try:
            if (self.cgv_db_cur):
                self.cgv_db_cur.close();
            if (self.cgv_conn_db):
                self.cgv_conn_db.close();
        except Exception, e:
            print "QBlockStore.cgf_disconnect_db get error : " + e;

    def cgf_update_block(self,v_block_file_name = None,v_checked_rows = None,v_block_check_status = None):
        try:
            if (self.cgv_db_cur):
                if (v_block_check_status == "finish"):
                    # finish才更新end time
                    v_sql = """update q_sharding_block_list set gmt_modify = CURRENT_TIMESTAMP,
                               checked_rows = %s,end_time = CURRENT_TIMESTAMP,block_check_status = '%s'
                               where block_file_name = '%s'
                            """ % (v_checked_rows,v_block_check_status,v_block_file_name);
                else:
                    v_sql = """update q_sharding_block_list set gmt_modify = CURRENT_TIMESTAMP,
                               checked_rows = %s,block_check_status = '%s'
                               where block_file_name = '%s'
                            """ % (v_checked_rows,v_block_check_status,v_block_file_name);
                self.cgv_db_cur.execute(v_sql);
                self.cgv_db_cur.commit();
        except Exception as e:
            self.cgv_db_cur.rollback();

    def cgf_update_sharding(self,v_sharding_id = None,v_sharding_check_status = None):
        try:
            if (self.cgv_db_cur):
                # 获取sharding下的所有block已经校验的数据数量，自动更新数字
                self.cgv_db_cur.execute("select sum(checked_rows) from q_sharding_block_list where sharding_id  = %s" % (sharding_id));
                v_checked_rows = self.cgv_db_cur.fetchmany(1);
                if (v_sharding_check_status == "finish"):
                    # 检查block是否都已经finish
                    self.cgv_db_cur.execute("select count(*) from q_sharding_block_list where sharding_id  = %s and block_check_status <> 'finish'" % (sharding_id));
                    v_not_finish = self.cgv_db_cur.fetchmany(1);
                    if (v_not_finish < 1):
                        # 已经finish，更新end time，status等
                        v_sql = """update q_table_sharding_list set gmt_modify = CURRENT_TIMESTAMP,
                                   checked_rows = %s,end_time = CURRENT_TIMESTAMP,sharding_check_status = '%s'
                                   where id = %s
                                """ % (v_checked_rows,v_sharding_check_status,v_sharding_id);
                    else:
                        # 没有全部finish，只更新checked rows
                        v_sql = """update q_table_sharding_list set gmt_modify = CURRENT_TIMESTAMP,
                                   checked_rows = %s where id = %s
                                """ % (v_checked_rows,v_sharding_id);
                else:
                    v_sql = """update q_table_sharding_list set gmt_modify = CURRENT_TIMESTAMP,
                               checked_rows = %s,sharding_check_status = '%s'
                               where id = %s
                            """ % (v_checked_rows,v_sharding_check_status,v_sharding_id);
                self.cgv_db_cur.execute(v_sql);
                self.cgv_db_cur.commit();
        except Exception as e:
            self.cgv_db_cur.rollback();

    def cgf_update_table(self,v_table_unique_name = None,v_table_check_status = None):
        try:
            if (self.cgv_db_cur):
                # 获取table下的所有sharding已经校验的数据数量，自动更新数字
                self.cgv_db_cur.execute("""select sum(checked_rows) from q_table_sharding_list 
                    where table_id in (select id from q_table_list where table_unique_name = '%s')""" % (v_table_unique_name));
                v_checked_rows = self.cgv_db_cur.fetchmany(1);
                if (v_table_check_status == "finish"):
                    # 检查sharding是否都已经finish
                    self.cgv_db_cur.execute("""select count(*) from q_table_sharding_list 
                    where table_id in (select id from q_table_list where table_unique_name = '%s')
                    and sharding_check_status <> 'finish'""" % (v_table_unique_name));
                    v_not_finish = self.cgv_db_cur.fetchmany(1);
                    if (v_not_finish < 1):
                        # 已经finish，更新end time，status等
                        v_sql = """update q_table_list set gmt_modify = CURRENT_TIMESTAMP,
                                   checked_rows = %s,end_time = CURRENT_TIMESTAMP,table_check_status = '%s'
                                   where table_unique_name = '%s'
                                """ % (v_checked_rows,v_table_check_status,v_table_unique_name);
                    else:
                        # 没有全部finish，只更新checked rows
                        v_sql = """update q_table_list set gmt_modify = CURRENT_TIMESTAMP,
                                   checked_rows = %s where table_unique_name = '%s'
                                """ % (v_checked_rows,v_table_unique_name);
                else:
                    v_sql = """update q_table_list set gmt_modify = CURRENT_TIMESTAMP,
                                   checked_rows = %s,table_check_status = '%s'
                                   where table_unique_name = '%s'
                            """ % (v_checked_rows,v_table_check_status,v_table_unique_name);
                self.cgv_db_cur.execute(v_sql);
                self.cgv_db_cur.commit();
        except Exception as e:
            self.cgv_db_cur.rollback();


class QKeyBlock():
    """
    1.KeyBlock用于存放key数据，一个keyblock建议不要超过512MB，超出，就采用多个block存放
    """
    def __init__(self,v_data_block = None,v_key_str = None,v_curcol_desc_list = None,v_hash_type = "hash"):
        # block的列属性原始描述,来自于cursor.description
        self.cgv_curcol_desc_list = v_curcol_desc_list;
        # block的所有列名与列标的kv对
        self.cgv_column_kvlist = {};
        # block的key列的列标
        self.cgv_keycol_position = [];
        # block的数据列列标
        self.cgv_datacol_position = [];
        # block数组，大小受限，否则内存容易溢出
        self.cgv_data_block = v_data_block;
        # block的hash索引，索引树为dict hash表，索引叶子为block的数组坐标
        self.cgv_hash_index = {};
        # key列列名
        self.cgv_keycol = v_key_str.lower().split(",");
        # data列列名（不包含key列）
        self.cgv_datacol = [];
        self.cgv_hash_type = v_hash_type;

        #初始化列名描述相关内容
        self.cgf_init_datacol_kvlist();
        self.cgf_init_keycol_position();
        self.cgf_init_datacol_position();

    # 初始化cgv_column_kvlist,cgv_datacol
    def cgf_init_datacol_kvlist(self):
        try:
            if (self.cgv_curcol_desc_list):
                v_datacol_all = [];
                #填充kvlist和datacol_all
                for i in xrange(len(self.cgv_curcol_desc_list)):
                    self.cgv_column_kvlist[self.cgv_curcol_desc_list[i][0].lower()] = i;
                    v_datacol_all.append(self.cgv_curcol_desc_list[i][0].lower());

                # data列与key列一致
                self.cgv_datacol = v_datacol_all;
        except Exception as e:
            print "QBlock.cgf_init_datacol_kvlist get error :" + e;

    # 函数获取列名对应的列标
    def cgf_get_column_position(self,v_column_name):
        return self.cgv_column_kvlist.get(v_column_name.lower());

    # 初始化key列的列标
    def cgf_init_keycol_position(self):
        for i in xrange(len(self.cgv_keycol)):
            self.cgv_keycol_position.append(self.cgf_get_column_position(self.cgv_keycol[i]));

    # 初始化数据列的列标
    def cgf_init_datacol_position(self):
        for i in xrange(len(self.cgv_datacol)):
            self.cgv_datacol_position.append(self.cgf_get_column_position(self.cgv_datacol[i]));

    # 生成hash索引，key为keystr，value为[hash(datacol),row_position]
    def cgf_create_hash_index(self):
        if (len(self.cgv_data_block) > 0):
            try:
                for i in xrange(len(self.cgv_data_block)):
                    # 生成keystr
                    v_key_str = self.cgf_get_keystr_value(self.cgv_data_block[i]);

                    # 生成hash(datacol),默认采用hash函数，mac本每秒330万，md5每秒160万
                    v_data_str = self.cgf_get_datastr_value(self.cgv_data_block[i]);
                    if (self.cgv_hash_type == "hash"):
                        v_hash_value = hash(v_data_str);
                    elif (self.cgv_hash_type == "md5"):
                        v_hash_value = hashlib.md5(v_data_str);

                    # 生成row_position
                    v_row_position = i;

                    #key／value入hash索引
                    self.cgv_hash_index[v_key_str] = [v_hash_value,v_row_position];
            except Exception as e:
                print "QBlock.cgf_create_hash_index get error :" + e;

    # 根据key str，走hashmap查找返回行记录
    def cgf_get_block_row(self,v_key_str):
        """v_key_str为key值的str连接，是hashmap的key"""
        if (v_key_str):
            # 获取行的position，即索引的rowid
            v_block_row_position = self.cgv_hash_index.get(v_key_str)[1];
            # 根据rowid访问block的具体行
            v_block_row = self.cgv_data_block[v_block_row_position];

            return v_block_row;
        else:
            return None;

    # 获取keystr的字符拼接value，即每一行的key值
    def cgf_get_keystr_value(self,v_row):
        v_key_str = "";
        for i in xrange(len(self.cgv_keycol_position)):
            v_key_str = v_key_str + str(v_row[self.cgv_keycol_position[i]]);

        return v_key_str;

    # 获取datastr的字符拼接value，即每一行的datavalue值
    def cgf_get_datastr_value(self,v_row):
        v_data_str = "";
        for i in xrange(len(self.cgv_datacol_position)):
            v_data_str = v_data_str + str(v_row[self.cgv_datacol_position[i]]);

        return v_data_str;

    # 清空block的数据和hash索引
    def cgf_block_clear_data(self):
        self.cgv_data_block[:] = [];
        self.cgv_hash_index.clear();

class QBlockCrusher():
    """进去的是Block，出来的是粉末，有属性的粉末"""
    def __init__(self,v_source_block = None,v_target_block = None,v_base_block = "source",
        v_is_surplus = False,v_is_missing = True,v_is_dismatch = True,v_literal_check = False):
        # 源端block
        self.cgv_source_block = v_source_block;
        # 目标端block
        self.cgv_target_block = v_target_block;
        # 基准block方
        self.cgv_base_block = v_base_block;
        # 是否进行多余行计算
        self.cgv_is_show_surplus = v_is_surplus;
        # 是否进行miss行计算
        self.cgv_is_show_missing = v_is_missing;
        # 是否进行dismatch行计算
        self.cgv_is_show_dismatch = v_is_dismatch;
        self.cgv_missing_rows = [];
        self.cgv_dismatch_rows = [];
        self.cgv_surplus_rows = [];
        # 是否需要字面级别的对比校验
        self.cgv_literal_check = v_literal_check;

    # 对比两个数组的元素，是否全部相等
    def cgf_array_list_check(self,v_source_row,v_target_row):
        if (len(v_source_row) > 0 and len(v_target_row) > 0):
            v_is_match = True;
            for i in xrange(len(v_source_row)):
                if (v_source_row[i] != v_target_row[i]):
                    v_is_match = False;
                    break;

            return v_is_match;

    # 获取missing rows数据量
    def cgf_get_missing_number(self):
        return len(self.cgv_missing_rows);

    # 获取surplus rows数据量
    def cgf_get_surplus_number(self):
        return len(self.cgv_surplus_rows);

    # 获取dismatch rows数据量
    def cgf_get_dismatch_number(self):
        return len(self.cgv_dismatch_rows);

    # 获取source rows数据量
    def cgf_get_source_number(self):
        return len(self.cgv_source_block.cgv_hash_index);

    # 获取target rows数据量
    def cgf_get_target_number(self):
        return len(self.cgv_target_block.cgv_hash_index);

    # 获取match的rows数据量
    # match = source rows - missing - dismatch
    def cgf_get_match_number(self):
        v_number = self.cgf_get_source_number() - self.cgf_get_missing_number() - self.cgf_get_dismatch_number();
        return v_number;

    # 返回missing的数据行
    def cgf_get_missing_rows(self):
        return self.cgv_missing_rows;

    # 返回surplus多余的数据行
    def cgf_get_surplus_rows(self):
        return self.cgv_surplus_rows;

    # 返回不匹配的数据行
    def cgf_get_dismatch_rows(self):
        return self.cgv_dismatch_rows;

    # 进行源端，目标端的block数据等值对比，即等值碎石机
    def cgf_block_equal_crush(self):
        v_source_block = None;
        v_target_block = None;
        
        # cgv_base_block控制源端还是目标端为基准
        if (self.cgv_base_block == 'source'):
            v_source_block = self.cgv_source_block;
            v_target_block = self.cgv_target_block;
        elif (self.cgv_base_block == 'target'):
            v_source_block = self.cgv_target_block;
            v_target_block = self.cgv_source_block;

        # 根据hash索引进行循环比对
        for v_key_str in v_source_block.cgv_hash_index:
            v_source_checksum = None;
            v_source_rowid = None; 
            v_target_checksum = None;
            v_target_rowid = None;

            # 根据key str获取checksum
            try:
                v_source_checksum,v_source_rowid = v_source_block.cgv_hash_index.get(v_key_str);
            except Exception as e:
                v_source_checksum = None;
                v_source_rowid = None;
            try:
                v_target_checksum,v_target_rowid = v_target_block.cgv_hash_index.get(v_key_str);
            except Exception as e:
                v_target_checksum = None;
                v_target_rowid = None;   

            # 源端存在，目标端不存在，即missing
            if (v_source_checksum is not None and v_target_checksum is None):
                self.cgv_missing_rows.append(v_source_block.cgf_get_block_row(v_key_str));

            # 相同的key str，但是checksum不相同，存储source／target对值
            if (v_source_checksum is not None and v_target_checksum is not None and v_source_checksum != v_target_checksum):
                v_dismatch_row_pair = [v_source_block.cgf_get_block_row(v_key_str),
                                       v_target_block.cgf_get_block_row(v_key_str)];
                self.cgv_dismatch_rows.append(v_dismatch_row_pair);

            # 如果采用字面对比literal模式，在checksum相同的情况下，需要进一步对比具体值
            if (self.cgv_literal_check == True and v_source_checksum == v_target_checksum and v_source_checksum is not None and v_target_checksum is not None):
                v_source_row = v_source_block.cgf_get_block_row(v_key_str);
                v_target_row = v_target_block.cgf_get_block_row(v_key_str);
                if (self.cgf_array_list_check(v_source_row,v_target_row) == False):
                    self.cgv_dismatch_rows.append([v_source_row,v_target_row]);

        # 如果需要比较目标端比源端多余的数据，需要再次循环，此时以target为base做一次missing运算
        for v_key_str in v_target_block.cgv_hash_index:
            v_source_checksum = None;
            v_source_rowid = None;
            v_target_checksum = None;
            v_target_rowid = None;

            # 根据key str获取checksum
            try:
                v_source_checksum,v_source_rowid = v_source_block.cgv_hash_index.get(v_key_str);
            except Exception as e:
                v_source_checksum = None;
                v_source_rowid = None;
            try:
                v_target_checksum,v_target_rowid = v_target_block.cgv_hash_index.get(v_key_str);
            except Exception as e:
                v_target_checksum = None;
                v_target_rowid = None;  

            # 目标端存在，源端不存在，即surplus，表述目标端比源端多的数据
            if (v_target_checksum is not None and v_source_checksum is None):
                self.cgv_surplus_rows.append(v_target_block.cgf_get_block_row(v_key_str));    


class QKeyBlockCreater():
    """挖掘掠夺机，负责从数据库抽取Key，生成KeyBlock"""
    def __init__(self,v_ip = None,v_port = None,v_sid_or_dbname = None,v_db_user = None,v_source_target = None,
                 v_dbpwd = None,v_dbtype = "Oracle",v_sql = None,v_sql_where = None,v_block_MB = 256,v_hash_type = "hash",
                 v_table_owner = None,v_tab_unique_name = None,v_table_name = None,v_key_str = None,v_file_path = None):
        self.cgv_ip = v_ip;
        self.cgv_port = v_port;
        self.cgv_sid_or_dbname = v_sid_or_dbname;
        self.cgv_dbuser = v_db_user;
        self.cgv_dbpwd = v_dbpwd;
        self.cgv_dbtype = v_dbtype;
        self.cgv_sql = v_sql;
        self.cgv_sql_where = v_sql_where;
        self.cgv_block_MB = v_block_MB;
        self.cgv_block_rows = 0;
        self.cgv_tab_unique_name = v_tab_unique_name;
        self.cgv_key_list = v_key_str.split(",");
        self.cgv_key_str = v_key_str;
        self.cgv_key_sql = "";
        self.cgv_deamon_block = None;
        self.cgv_curcol_desc_list = None;
        self.cgv_hash_type = v_hash_type;
        self.cgv_source_target = v_source_target;
        self.cgv_file_path = v_file_path;
        self.cgv_conn_db = None;
        self.cgv_db_cur = None;
        self.cgv_table_owner = v_table_owner;
        self.cgv_table_name = v_table_name;

    def cgf_connect_db(self):
        try:
            if (self.cgv_dbtype == "Oracle"):
                v_dsn_tns = cx_Oracle.makedsn(self.cgv_ip,self.cgv_port,self.cgv_sid_or_dbname);
                self.cgv_conn_db = cx_Oracle.Connection(self.cgv_dbuser,self.cgv_dbpwd,v_dsn_tns);
                self.cgv_db_cur = self.cgv_conn_db.cursor();

            if (self.cgv_dbtype == "MySQL"):
                self.cgv_conn_db = MySQLdb.Connection(host = self.cgv_ip,user = self.cgv_dbuser,
                passwd = self.cgv_dbpwd,db = self.cgv_sid_or_dbname,port = self.cgv_port);
                self.cgv_db_cur = self.cgv_conn_db.cursor();
        except Exception, e:
            print self.cgv_dbtype + " connect error:" + self.cgv_ip + ":" + self.cgv_port + ":" + self.cgv_sid_or_dbname;
            print e;

    def cgf_disconnect_db(self):
        try:
            if (self.cgv_db_cur):
                self.cgv_db_cur.close();
            if (self.cgv_conn_db):
                self.cgv_conn_db.close();
        except Exception, e:
            print "cgf_disconnect_db get error : " + e;

    def cgf_estimate_blockrows(self):
        """评估一个block大约包含多少行key，block大小由block_MB决定"""
        try:
            if (self.cgv_conn_db):
                v_my_cursor = self.cgv_conn_db.cursor();
                v_my_cursor.execute(self.cgv_key_sql);
                # cgv_curcol_desc_list初始化
                self.cgv_curcol_desc_list = v_my_cursor.description;
                v_data_list = v_my_cursor.fetchmany(100);
                v_data_size = self.cgf_get_object_size(v_data_list);
                self.cgv_block_rows = self.cgv_block_MB * 1024 * 1024 / (v_data_size / len(v_data_list));
                v_my_cursor.close();
        except Exception as e:
            self.cgv_block_rows = 1000000;
            print e;

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

    def cgf_write_keyblock_file(self):
        """导出key数据，序列化写入到keyblock文件"""            
        try:
            v_is_continue = True;
            v_data_list = None;
            v_count = 0;
            # 获取sql的结果集，50000一个批次进行处理
            self.cgv_db_cur.execute(self.cgv_key_sql);
            while (v_is_continue):
                v_data_list = self.cgv_db_cur.fetchmany(self.cgv_block_rows);
                if (len(v_data_list) > 0):
                    # 生成key block
                    v_key_block = QKeyBlock(v_data_list,self.cgv_key_str,self.cgv_curcol_desc_list,self.cgv_hash_type);
                    # open keyblock文件
                    v_keyblock_name = self.cgv_file_path + "/" + self.cgv_tab_unique_name + "." + str(v_count);
                    v_keyblock_handle = open(v_keyblock_name,"wb");

                    # write keyblock文件
                    cPickle.dump(v_key_block,v_keyblock_handle,protocol=-1);

                    # 关闭文件句柄
                    v_keyblock_handle.close();

                    # 更新keyblockstore

                    # 计数器增加
                    v_count = v_count + 1;
                else:
                    v_is_continue = False;
        except Exception as e:
            print "cgf_write_keyblock_file get error: " + e;
            os._exit(0);


    def cgf_get_key_cols(self):
        """根据表名，owner，获取该表的主键／唯一键列名，初始化cgv_key_list，cgv_key_str，cgv_key_sql"""
        if (self.cgv_dbtype == "Oracle"):
            v_sql = """
                select COLUMN_NAME from dba_ind_columns 
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
            if (self.cgv_db_cur):
                self.cgv_db_cur.execute(v_sql,owner = self.cgv_table_owner,table_name = self.cgv_table_name);
                # cgv_key_list，cgv_key_str
                if (self.cgv_key_str == None):
                    self.cgv_key_list = self.cgv_db_cur.fetchall();
                    self.cgv_key_str = "";
                    for i in xrange(len(v_data_list)):
                        self.cgv_key_str = self.cgv_key_str + str(self.cgv_key_list[i]) + ","

                    self.cgv_key_str =  self.cgv_key_str.rstrip(',');

                # cgv_key_sql
                self.cgv_key_sql = "select " + self.cgv_key_str + " from " + self.cgv_table_owner + "." + self.cgv_table_name + " " + self.cgv_sql_where;
            else:
                return None;

        if (self.cgv_dbtype == "MySQL"):
            """待李春补充"""
            pass;

if __name__ == "__main__":
    myQPlunder = QKeyBlockCreater(v_ip = "10.10.160.181",v_port = "1521",v_sid_or_dbname = "rac2",v_db_user = "qbench",v_source_target = "source",
                 v_dbpwd = "qbench",v_dbtype = "Oracle",v_sql = "select * from offer where rownum < 60000000",v_sql_where = "where rownum < 60000000",v_block_MB = 128,v_hash_type = "hash",
                 v_table_owner = "qbench",v_tab_unique_name = "offer",v_table_name = "offer",v_key_str = "offerid",v_file_path = "/oswbb/oracle/dbdata");
    myQPlunder.cgf_connect_db();
    myQPlunder.cgf_get_key_cols();
    myQPlunder.cgf_estimate_blockrows();
    myQPlunder.cgf_write_keyblock_file();
    """
    v_cur_desc = [('A',  'cx_Oracle.DATETIME', 23, None, None, None, 1), 
                ('B',  'cx_Oracle.FIXED_CHAR', 1, 2, None, None, 1),
                ('C',  'cx_Oracle.BLOB', None, None, None, None, 1), 
                ('D',  'cx_Oracle.CLOB', None, None, None, None, 1), 
                ('E',  'cx_Oracle.LONG_STRING', None, None, None, None, 1)];

    v_data_block1 = [['2019-2-21 19:23:21','a','fuck girl','suck girl','hello'],
                    ['2019-2-21 19:23:22','b','fuck girl','suck girl','hello'],
                    ['2019-2-21 19:23:23','c','fuck girl','suck girl','hello'],
                    ['2019-2-21 19:23:24','d','fuck girl','suck girl','hello'],
                    ['2019-2-21 19:23:25','e','fuck girl','suck girl','hello'],
                    ['2019-2-21 19:23:25','f','fuck girl','suck girl','hello']];

    v_data_block2 = [['2019-2-21 19:23:21','a','fuck girl9','suck girl','hello'],
                    ['2019-2-21 19:23:22','b','fuck girl','suck girl','hello'],
                    ['2019-2-21 19:23:23','c','fuck girl','suck girl','hello'],
                    ['2019-2-21 10:23:24','d','fuck girl','suck girl','hello'],
                    ['2019-2-21 19:23:25','e','fuck girl','suck girl','hello'],
                    ['2019-2-21 19:23:25','g','fuck girl','suck girls','hello']];

    v_key_list = "B";

    myblock1 = QBlock(v_data_block1,v_key_list,v_cur_desc);
    myblock1.cgf_init_datacol_kvlist();
    myblock1.cgf_init_keycol_position();
    myblock1.cgf_init_datacol_position();
    myblock1.cgf_create_hash_index();
    myblock2 = QBlock(v_data_block2,v_key_list,v_cur_desc);
    myblock2.cgf_init_datacol_kvlist();
    myblock2.cgf_init_keycol_position();
    myblock2.cgf_init_datacol_position();
    myblock2.cgf_create_hash_index();
    print myblock1.cgv_column_kvlist;
    print myblock1.cgv_datacol;
    print myblock1.cgv_keycol;
    print myblock1.cgv_keycol_position;
    print myblock1.cgv_datacol_position;
    print myblock1.cgv_hash_index;
    print "*********************"
    print myblock2.cgv_column_kvlist;
    print myblock2.cgv_datacol;
    print myblock2.cgv_keycol;
    print myblock2.cgv_keycol_position;
    print myblock2.cgv_datacol_position;
    print myblock2.cgv_hash_index;

    mycrusher = QCrusher(v_source_block = myblock1,v_target_block = myblock2,v_base_block = "source",
        v_is_surplus = False,v_is_missing = True,v_is_dismatch = True,v_literal_check = True);

    print mycrusher.cgf_block_equal_crush();
    print mycrusher.cgf_get_missing_number();
    print mycrusher.cgf_get_surplus_number();
    print mycrusher.cgf_get_dismatch_number();
    print mycrusher.cgf_get_missing_rows();
    print mycrusher.cgf_get_surplus_rows();
    print mycrusher.cgf_get_dismatch_rows();
    """

select 
from q_task_list a,q_tab_twins_list b,q_table_sharding_list s,q_table_sharding_list t
where a.id = b.task_id and b.id = s.sharding_id and b.id = t.sharding_id
and a.task_status != "finished"
and b.twins_status != "finished"
"""
          CREATE TABLE `q_task_list` (
          `id` int NOT NULL AUTO_INCREMENT COMMENT '设置主键自增',
          `gmt_create` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
          `gmt_modify` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
          `task_name` varchar(1024) NOT NULL COMMENT '任务名称',
          `task_status` varchar(64) NOT NULL COMMENT 'starting准备，finished全部校验完成,error表示出错',
          `begin_time` datetime  DEFAULT null COMMENT '检查开始时间',
          `end_time` datetime  DEFAULT null COMMENT '检查结束时间',
          `error_info` varchar(2048) default NULL COMMENT '出错信息',
          PRIMARY KEY (`id`) USING BTREE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='任务状态表';

          CREATE TABLE `q_tab_twins_list` (
          `id` int NOT NULL AUTO_INCREMENT COMMENT '设置主键自增',
          `task_id` int NOT NULL COMMENT 'q_task_list.id',
          `gmt_create` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
          `gmt_modify` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
          `source_tab_uname` varchar(512) NOT NULL COMMENT '',
          `target_tab_uname` varchar(512) NOT NULL COMMENT '',
          `source_tab_sname` varchar(512) NOT NULL COMMENT '表名简称,QTXXXXXXXX',
          `target_tab_sname` varchar(512) NOT NULL COMMENT '表名简称,QTXXXXXXXX',
          `source_tab_charset` varchar(512) default NULL COMMENT '数据的字符集',
          `target_tab_charset` varchar(512) default NULL COMMENT '数据的字符集',
          `source_table_rows` int DEFAULT 0 COMMENT '源表数据行数',
          `target_table_rows` int DEFAULT 0 COMMENT '目标表数据行数',
          `dismatch_rows` int DEFAULT 0 COMMENT '不匹配的数据行数',
          `missing_rows` int DEFAULT 0 COMMENT '少的数据行数',
          `surplus_rows` int DEFAULT 0 COMMENT '多的数据行数',
          `match_rows` int DEFAULT 0 COMMENT '匹配的数据行数',
          `source_tabpull_btime` datetime  DEFAULT null COMMENT '源端拉取开始时间',
          `source_tabpull_etime` datetime  DEFAULT null COMMENT '源端拉取结束时间',
          `target_tabpull_btime` datetime  DEFAULT null COMMENT '目标拉取开始时间',
          `target_tabpull_etime` datetime  DEFAULT null COMMENT '目标拉取结束时间',
          `check_begin_time` datetime  DEFAULT null COMMENT '检查开始时间',
          `check_end_time` datetime  DEFAULT null COMMENT '检查结束时间',
          `twins_status` varchar(64) NOT NULL COMMENT 'starting准备 pulling拉数据 checking数据校验过程中 finished全部校验完成,error表示出错',
          `source_dbtype` varchar(16) NOT NULL COMMENT 'Oracle|MySQL|PostGreSQL|HBase|Vertica|Hive',
          `target_dbtype` varchar(16) NOT NULL COMMENT 'Oracle|MySQL|PostGreSQL|HBase|Vertica|Hive',
          `error_info` varchar(2048) default NULL COMMENT '出错信息',
          PRIMARY KEY (`id`) USING BTREE,
          UNIQUE KEY `ix_ts_sname` (`source_tab_sname`) USING BTREE,
          UNIQUE KEY `ix_tt_sname` (`target_tab_sname`) USING BTREE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='校验表列表对，每个对一行，数据分片合并成sharding代替';

        CREATE TABLE `q_table_sharding_list` (
          `id` int NOT NULL AUTO_INCREMENT COMMENT '设置主键自增',
          `gmt_create` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
          `gmt_modify` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
          `tab_twins_id` int NOT NULL COMMENT 'q_tab_twins_list.id',
          `sharding_table_name` varchar(256) NOT NULL COMMENT '数据分片的表名',
          `sharding_table_owner` varchar(64) NOT NULL COMMENT '数据分片的表owner，oracle为schemaname，mysql为username',
          `sharding_table_rows` int DEFAULT 0 COMMENT '分片表数据行数',
          `sharding_countoff_btime` datetime  DEFAULT null COMMENT '统计数据量开始时间',
          `sharding_countoff_etime` datetime  DEFAULT null COMMENT '统计数据量结束时间',
          `sharding_pull_btime` datetime  DEFAULT null COMMENT '拉取开始时间',
          `sharding_pull_etime` datetime  DEFAULT null COMMENT '拉取结束时间',
          `sharding_status` varchar(64) NOT NULL COMMENT 'starting准备 counting统计数据量 pulling拉数据 pulled全部校验完成 error表示出错',
          `sql_text` varchar(4096) default NULL COMMENT 'sql text',
          `sql_select` varchar(4096) default NULL COMMENT '自定义select的column，逗号隔开，默认为空',
          `sql_where` varchar(4096) default NULL COMMENT 'where语句',
          `pkuk_str` varchar(512) default NULL COMMENT 'pkuk列，按照逗号分隔，默认为pk，没有PK，则为uk',
          `sharding_str` varchar(512) default NULL COMMENT 'sharding分区键',
          `source_target` varchar(10) default 'source' COMMENT '目标端还是源端，代表了对应于tab twins的source／target',
          `ip` varchar(16) default NULL COMMENT 'ip地址',
          `port` int default NULL COMMENT '端口',
          `sid_or_dbname` varchar(64) default NULL COMMENT 'oracle sid或mysql user',
          `db_user` varchar(64) DEFAULT null comment '数据库连接用户',
          `db_pwd` varchar(128) DEFAULT null comment '数据库连接用户密码',
          `process_parallel` int NOT NULL default 1 COMMENT '并行度，主要适用于oracle数据库',
          `error_info` varchar(2048) default NULL COMMENT '出错信息',
          PRIMARY KEY (`id`) USING BTREE,
           KEY `ix_tb_id` (`tab_twins_id`) USING BTREE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='sharding列表，普通表就一个分片，即原表本身';

        CREATE TABLE `q_sharding_block_list` (
          `id` int NOT NULL AUTO_INCREMENT COMMENT '设置主键自增',
          `gmt_create` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
          `gmt_modify` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
          `sharding_id` int NOT NULL COMMENT 'q_table_sharding_list.id',
          `block_file_name` varchar(512) NOT NULL COMMENT 'block路径名',
          `block_id` int default NULL COMMENT 'block编号',
          `block_MB` int default 256 COMMENT 'block大小，MB为单位',
          `block_rows` int DEFAULT null COMMENT 'block数据行数',
          `checked_rows` int DEFAULT null COMMENT '已检查的block数据行数',
          `begin_time` datetime  DEFAULT null COMMENT '检查开始时间',
          `end_time` datetime  DEFAULT null COMMENT '检查结束时间',
          `block_check_status` varchar(64) NOT NULL COMMENT 'preparing准备过程中 checking数据校验过程中 finished全部校验完成,error表示出错',
          `error_info` varchar(2048) default NULL COMMENT '出错信息',
          PRIMARY KEY (`id`) USING BTREE,
          unique KEY `ix_bfile_name` (`block_file_name`) USING BTREE,
           KEY `ix_sharding_id` (`sharding_id`) USING BTREE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='block列表，一个sharding对应一个或多个block';

        CREATE TABLE `q_idlist_table` (
          `id` int NOT NULL AUTO_INCREMENT COMMENT '设置主键自增',
          `gmt_create` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY (`id`) USING BTREE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='做为sequence使用';
"""


