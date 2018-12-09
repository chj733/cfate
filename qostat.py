#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
 ScriptName: qostat.py
 Author: chun.luo@woqutech.com
 Create Date: 2016-6-11 19:08
 Modify Author: chun.luo@woqutech.com
 Modify Date: 2016-6-16 13:08
 Function: Display Oracle performance statistics
"""

"""
    待解决的问题：
    1）db time如何计算？ Done
    2）展示的array里面，需要标记分类，目前topevent可能与event有重复数据 Done
    3) 刷屏格式排版问题，未完成  Done
    4）如何捕获ctrl＋C，来终止主程与子线程？未完成  Done
    5）计算逻辑复杂，如何交接出去？   noway
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
#import paramiko

gv_conf_file = ConfigParser.ConfigParser();
gv_qostatthread_list = [];

gv_big_sql = """
            select event as name,to_char(sysdate,'yyyy-mm-dd hh24:mi:ss') as gmt_create,total_waits as raw_value,
	    10 * time_waited as time_waited,'system_event' as data_from,'event' as cal_type from v$system_event
            where event in ('db file sequential read', 'db file scattered read',
              'log file sync', 'log file parallel write',
              'direct path read','direct path write',
              'direct path read temp','direct path write temp')
            union all
            select decode(name,'CPU used by this session','CPU time(s)','DB time','DB time(s)','opened cursors current','opened cursors',name) as name,
	    to_char(sysdate,'yyyy-mm-dd hh24:mi:ss') as gmt_create,
            decode(name,'CPU used by this session',value / 100,
            'DB time',value / 100,
            'gc cr blocks served',value * (select value from v$parameter where name = 'db_block_size' and rownum = 1),
            'gc current blocks served',value * (select value from v$parameter where name = 'db_block_size' and rownum = 1),
            'gc cr blocks received',value * (select value from v$parameter where name = 'db_block_size' and rownum = 1),
            'gc current blocks received',value * (select value from v$parameter where name = 'db_block_size' and rownum = 1),
	    'ges messages sent',200 * value,'gcs messages sent',200 * value,
            value)
            as raw_value,null,'sysstat' as data_from,
            decode(name,'gc cr blocks received','rac','gc cr blocks served','rac','gc current blocks received','rac','gc current blocks served','rac',
              'physical read total bytes','mbps','physical write total bytes','mbps','physical read total IO requests','iops',
              'physical write total IO requests','iops','gcs messages sent','rac','ges messages sent','rac','opened cursors current','cur_val',
		'compute_val')as cal_type from v$sysstat
            where name in ('gc cr blocks received',
                                        'gc current blocks received',
                                        'gc cr blocks served',
                                        'gc current blocks served',
                                        'gcs messages sent',
                                        'ges messages sent',
                                        'gc cr block receive time',
                                        'gc current block receive time',
                                        'gc cr block send time',
                                        'gc current block send time', 'physical read total bytes',
                                                        'physical write total bytes',
                                                        'physical read total IO requests',
                                                        'physical write total IO requests', 'redo writes',
                                                        'redo size','session logical reads', 'redo size',
                                        'physical writes', 'physical reads', 'parse count (hard)',
                                        'parse count (total)', 'execute count', 'user commits','opened cursors current',
                                        'db block changes', 'SQL*Net roundtrips to/from client',
                                        'bytes received via SQL*Net from client',
                                        'bytes sent via SQL*Net to client','DB time','CPU used by this session')
            union all
            select name,to_char(sysdate,'yyyy-mm-dd hh24:mi:ss') as gmt_create,200 * value as raw_value,null,'dlm_misc' as data_from,'rac' as cal_type from v$dlm_misc
            where name in ('gcs msgs received','ges msgs received')
            union all
            select 'Up time(Day)' as name,to_char(sysdate,'yyyy-mm-dd hh24:mi:ss') as gmt_create,round(sysdate-startup_time,1) as raw_value,null,'instance' as data_from,'cur_val' as cal_type from v$instance
            union all
            select 'User Active Sessions' as name,to_char(sysdate,'yyyy-mm-dd hh24:mi:ss'), count(*), null,'session','cur_val' from v$session where status = 'ACTIVE' AND type = 'USER'
            union all
            select 'ConCur Trans',to_char(sysdate,'yyyy-mm-dd hh24:mi:ss'),  count(*), null,'transaction','cur_val' from v$transaction
            union all
            select event,to_char(sysdate,'yyyy-mm-dd hh24:mi:ss'),counts as raw_value,null,'session','topevent' from (
                    select a.event,count(*) as counts
                    from v$session a
                    where a.type = 'USER'
                    and a.wait_class not in ('Idle')
                    group by a.event
                    order by 2 desc)
                    where rownum <= 5""";

gv_process_sql = """select to_char(sysdate,'yyyy-mm-dd hh24:mi:ss') as gmt_create,count(*) as Processes,
                    round(avg(PGA_ALLOC_MEM),2) as avg_pga,
                    sum(pga_alloc_mem) as total_pga
                    from v$process""";

gv_tempfile_sql = """select to_char(sysdate,'yyyy-mm-dd hh24:mi:ss') as gmt_create,sum(a.phyrds + a.phywrts) as iops,
                     sum(a.phyblkrd * b.block_size + a.phyblkwrt * b.block_size)  as mbps
                     from v$tempfile b,v$tempstat a where b.file# = a.file#""";


#程序中断，Ctrl+C处理
def gf_exit_handler(signum,frame):
    try:
    	global gv_continue_boolean;
    	gv_continue_boolean = False;
        	#释放数据库游标与连接,线程运行标志位false
            #for i in xrange(0,len(gv_qostatthread_list)):
            #    gv_qostatthread_list[i].cf_disconnect_db();
    except Exception,e:
	   pass;

#数字转换K,M,G,T,P函数
def gf_number_convert(gfv_my_number):
    try:
    	if (gfv_my_number >= 1024*1024*1024*1024*1024):
    	    return str("%.1f" % (float(gfv_my_number) / (1024*1024*1024*1024*1024))) + "P";
    	elif (gfv_my_number >= 1024*1024*1024*1024):
    	    return str("%.1f" % (float(gfv_my_number) / (1024*1024*1024*1024))) + "T";
    	elif (gfv_my_number >= 1024*1024*1024):
    	    return str("%.1f" % (float(gfv_my_number) / (1024*1024*1024))) + "G";
    	elif (gfv_my_number >= 1024*1024):
    	    return str("%.1f" % (float(gfv_my_number) / (1024*1024))) + "M";
    	elif (gfv_my_number >= 1024):
    	    return str("%.1f" % (float(gfv_my_number) / (1024))) + "K";
    	else:
    	    return str("%.1f" % float(gfv_my_number));
    except Exception,e:
        return '';

#字符串格式化1
def gf_string_handle(gfv_metric_name,gfv_metric_value,gfv_len,gfv_blank_what,gfv_is_centre = 0):
    try:
    	#右边补齐
    	if (gfv_is_centre == 0):
    	    if (len(gfv_metric_name + gfv_metric_value) < gfv_len -2):
    	        return('|' + gfv_metric_name.ljust(gfv_len - 2 - len(gfv_metric_value),gfv_blank_what) + gfv_metric_value + '|');
            else:
                return gfv_metric_name + gfv_metric_value;

    	#中间对齐
    	elif (gfv_is_centre == 1):
    	    if (len(gfv_metric_name + gfv_metric_value) < gfv_len -2):
    	        return('|' + gfv_metric_name.center(gfv_len - 2 - len(gfv_metric_value),gfv_blank_what) + gfv_metric_value + '|');
            else:
                return gfv_metric_name + gfv_metric_value;

        #左边补气
        elif (gfv_is_centre == 2):
            if (len(gfv_metric_name + gfv_metric_value) < gfv_len -2):
                return('|' + gfv_metric_name.rjust(gfv_len - 2 - len(gfv_metric_value),gfv_blank_what) + gfv_metric_value + '|');
            else:
                return gfv_metric_name + gfv_metric_value;
    except Exception,e:
        return '';

#字符串格式化2
def gf_string_pad(gfv_name,gfv_len,gfv_blank_what,gfv_position):
    try:
        if (gfv_position == "r"):
            if (len(str(gfv_name)) < gfv_len):
                return str(gfv_name).ljust(gfv_len,gfv_blank_what);
            else:
                return str(gfv_name);
        if (gfv_position == "l"):
            if (len(str(gfv_name)) < gfv_len):
                return str(gfv_name).rjust(gfv_len,gfv_blank_what);
            else:
                return str(gfv_name);
        if (gfv_position == "c"):
            if (len(str(gfv_name)) < gfv_len):
                return str(gfv_name).center(gfv_len,gfv_blank_what);
            else:
                return str(gfv_name);
    except Exception,e:
        return ' ';

#字符串上色
def gf_string_color(gfv_string,gfv_color):
    try:
    	if (gfv_string):
    	    if (gfv_color == 'darkblack'):
    	        return '\033[1;30;40m' + gfv_string + '\033[0m';
    	    elif (gfv_color == 'darkred'):
    	        return '\033[1;31;40m' + gfv_string + '\033[0m';
    	    elif (gfv_color == 'darkgreen'):
    	        return '\033[1;32;40m' + gfv_string + '\033[0m';
    	    elif (gfv_color == 'darkyellow'):
    	        return '\033[1;33;40m' + gfv_string + '\033[0m';
    	    elif (gfv_color == 'darkblue'):
    	        return '\033[1;34;40m' + gfv_string + '\033[0m';
    	    elif (gfv_color == 'magenta'):
    	        return '\033[1;35;40m' + gfv_string + '\033[0m';
    	    elif (gfv_color == 'blue'):
    	        return '\033[1;36;40m' + gfv_string + '\033[0m';
    	    elif (gfv_color == 'white'):
    	        return '\033[1;0;40m' + gfv_string + '\033[0m';
    	    else:
    	        return '\033[1;0;40m' + gfv_string + '\033[0m';
    except Exception,e:
        return '\033[1;0;40m' + ' ' + '\033[0m';

#screen打印方法函数
def gf_suck_pstat_print(cgv_perf_stat_array,cgv_core_stat_array,cgv_pgapro_stat_array,cgv_iolatency_stat_array,cgv_iombps_stat_array,cgv_racgc_stat_array,cgv_network_stat_array,cgv_topevent_stat_array):
    #清理屏幕
    os.system("clear");
    #cgv_iolatency_stat_array数组长度不固定，为避免越界，强行扩容上去
    #cgv_pgapro_stat_array顺带扩容
    for i in xrange(30):
        cgv_iolatency_stat_array.extend([['','','']]);
        cgv_pgapro_stat_array.extend([['','','']]);

    #woqu商业标志打印
    print gf_string_color(gf_string_handle('WOQUTech Oracle Performance Sniffer','-',120,'-',1),'darkblue');
    #表头打印
    #|Core Metric---------------------------||PGA & Session & Process---------------||IO Latency----------------------------|
    print gf_string_color(gf_string_handle('Core Metric','-',40,'-'),'white') + \
    gf_string_color(gf_string_handle('PGA & Session & Process','-',40,'-'),'blue') + \
    gf_string_color(gf_string_handle('IO Latency','-',40,'-'),'darkgreen');
    #第一栏指标打印，核心指标，pga，IO延迟
    for i in xrange(len(cgv_core_stat_array)):
        print gf_string_color(gf_string_handle(cgv_core_stat_array[i][0],gf_number_convert(cgv_core_stat_array[i][1]),40,' '),'white') + \
          gf_string_color(gf_string_handle(cgv_pgapro_stat_array[i][0],gf_number_convert(cgv_pgapro_stat_array[i][1]),40,' '),'blue') + \
          gf_string_color(gf_string_handle(cgv_iolatency_stat_array[i][0],gf_number_convert(cgv_iolatency_stat_array[i][1]),40,' '),'darkgreen');

    print gf_string_color(gf_string_handle('-','-',120,'-'),'darkblue');

    #第二栏指标打印，iops mbps network rac
    print gf_string_color(gf_string_handle('IOPS & IOBytes','-',40,'-'),'darkblue') + gf_string_color(gf_string_handle('RAC GC','-',64,'-'),'darkyellow');
    for i in xrange(len(cgv_iombps_stat_array)):
        if (i <= 2):
            print gf_string_color(gf_string_handle(cgv_iombps_stat_array[i][0],gf_number_convert(cgv_iombps_stat_array[i][1]),40,' '),'darkblue') + \
                  gf_string_color(gf_string_handle(cgv_racgc_stat_array[i][0],gf_number_convert(cgv_racgc_stat_array[i][1]),64,' '),'darkyellow');
        elif (i == 3):
            print gf_string_color(gf_string_handle(cgv_iombps_stat_array[i][0],gf_number_convert(cgv_iombps_stat_array[i][1]),40,' '),'darkblue') + \
                  gf_string_color(gf_string_handle('Oracle Network','-',64,'-'),'magenta');
        elif (i > 3 and i <= 6):
            print gf_string_color(gf_string_handle(cgv_iombps_stat_array[i][0],gf_number_convert(cgv_iombps_stat_array[i][1]),40,' '),'darkblue') + \
                  gf_string_color(gf_string_handle(cgv_network_stat_array[i - 4][0],gf_number_convert(cgv_network_stat_array[i - 4][1]),64,' '),'magenta');
        else:
            print gf_string_color(gf_string_handle(cgv_iombps_stat_array[i][0],gf_number_convert(cgv_iombps_stat_array[i][1]),40,' '),'darkblue') + \
            gf_string_color(gf_string_handle(' ',' ',64,' '),'magenta');

    print gf_string_color(gf_string_handle('-','-',104,'-'),'darkblue');

    #第三栏，top等待事件
    print gf_string_color(gf_string_handle('Top 5 Events','-',40,'-'),'darkred');
    for i in xrange(len(cgv_topevent_stat_array)):
        print gf_string_color(gf_string_handle(cgv_topevent_stat_array[i][0],gf_number_convert(cgv_topevent_stat_array[i][1]),40,' '),'darkred');

    print gf_string_color(gf_string_handle('-','-',40,'-'),'darkred');
    

#line模式打印函数方法
def gf_suck_vstat_print(v_model,v_istitle,v_threadlist):
    v_count = 1;
    v_body_text = "";
    v_tittle_text = gf_string_pad("INST",23," ","r") + gf_string_pad("Lreads",10," ","r") + gf_string_pad("Exec",10," ","r") + \
                        gf_string_pad("UCommit",10," ","r") + gf_string_pad("RedoSize",10," ","r") + \
                        gf_string_pad("ASS",10," ","r") + gf_string_pad("Process",10," ","r") + \
                        gf_string_pad("DBtime",10," ","r") + gf_string_pad("CPUtime",10," ","r") + \
                        gf_string_pad("IOPS",10," ","r") + gf_string_pad("IObytes",10," ","r") + \
                        gf_string_pad("Dflat(ms)",10," ","r") + gf_string_pad("Lflat(ms)",10," ","r") + \
                        gf_string_pad("GCbytes",10," ","r") + gf_string_pad("Netout",10," ","r") + \
                        gf_string_pad("Netin",10," ","r");

    if (v_threadlist):
        for i in xrange(len(v_threadlist)):
            if (v_count < len(v_threadlist)):
                v_body_text = v_body_text + gf_string_pad(str(v_threadlist[i].cgv_sid),23," ","r") + \
                        gf_string_pad(gf_number_convert(v_threadlist[i].cgv_perf_stat_result.get("session logical reads")),10," ","r") + \
                        gf_string_pad(gf_number_convert(v_threadlist[i].cgv_perf_stat_result.get("execute count")),10," ","r") + \
                        gf_string_pad(gf_number_convert(v_threadlist[i].cgv_perf_stat_result.get("user commits")),10," ","r") + \
                        gf_string_pad(gf_number_convert(v_threadlist[i].cgv_perf_stat_result.get("redo size")),10," ","r") + \
                        gf_string_pad(gf_number_convert(v_threadlist[i].cgv_perf_stat_result.get("User Active Sessions")),10," ","r") + \
                        gf_string_pad(gf_number_convert(v_threadlist[i].cgv_perf_stat_result.get("Processes")),10," ","r") + \
                        gf_string_pad(gf_number_convert(v_threadlist[i].cgv_perf_stat_result.get("DB time(s)")),10," ","r") + \
                        gf_string_pad(gf_number_convert(v_threadlist[i].cgv_perf_stat_result.get("CPU time(s)")),10," ","r") + \
                        gf_string_pad(gf_number_convert(v_threadlist[i].cgv_perf_stat_result.get("Datafile IOPS")),10," ","r") + \
                        gf_string_pad(gf_number_convert(v_threadlist[i].cgv_perf_stat_result.get("Datafile IOBytes")),10," ","r") + \
                        gf_string_pad(gf_number_convert(v_threadlist[i].cgv_perf_stat_result.get("db file sequential read(ms)")),10," ","r") + \
                        gf_string_pad(gf_number_convert(v_threadlist[i].cgv_perf_stat_result.get("log file parallel write(ms)")),10," ","r") + \
                        gf_string_pad(gf_number_convert(v_threadlist[i].cgv_perf_stat_result.get("RAC GCBytes")),10," ","r") + \
                        gf_string_pad(gf_number_convert(v_threadlist[i].cgv_perf_stat_result.get("bytes sent via SQL*Net to client")),10," ","r") + \
                        gf_string_pad(gf_number_convert(v_threadlist[i].cgv_perf_stat_result.get("bytes received via SQL*Net from client")),10," ","r") + "\n";
            else:
                v_body_text = v_body_text + gf_string_pad(str(v_threadlist[i].cgv_sid),23," ","r") + \
                        gf_string_pad(gf_number_convert(v_threadlist[i].cgv_perf_stat_result.get("session logical reads")),10," ","r") + \
                        gf_string_pad(gf_number_convert(v_threadlist[i].cgv_perf_stat_result.get("execute count")),10," ","r") + \
                        gf_string_pad(gf_number_convert(v_threadlist[i].cgv_perf_stat_result.get("user commits")),10," ","r") + \
                        gf_string_pad(gf_number_convert(v_threadlist[i].cgv_perf_stat_result.get("redo size")),10," ","r") + \
                        gf_string_pad(gf_number_convert(v_threadlist[i].cgv_perf_stat_result.get("User Active Sessions")),10," ","r") + \
                        gf_string_pad(gf_number_convert(v_threadlist[i].cgv_perf_stat_result.get("Processes")),10," ","r") + \
                        gf_string_pad(gf_number_convert(v_threadlist[i].cgv_perf_stat_result.get("DB time(s)")),10," ","r") + \
                        gf_string_pad(gf_number_convert(v_threadlist[i].cgv_perf_stat_result.get("CPU time(s)")),10," ","r") + \
                        gf_string_pad(gf_number_convert(v_threadlist[i].cgv_perf_stat_result.get("Datafile IOPS")),10," ","r") + \
                        gf_string_pad(gf_number_convert(v_threadlist[i].cgv_perf_stat_result.get("Datafile IOBytes")),10," ","r") + \
                        gf_string_pad(gf_number_convert(v_threadlist[i].cgv_perf_stat_result.get("db file sequential read(ms)")),10," ","r") + \
                        gf_string_pad(gf_number_convert(v_threadlist[i].cgv_perf_stat_result.get("log file parallel write(ms)")),10," ","r") + \
                        gf_string_pad(gf_number_convert(v_threadlist[i].cgv_perf_stat_result.get("RAC GCBytes")),10," ","r") + \
                        gf_string_pad(gf_number_convert(v_threadlist[i].cgv_perf_stat_result.get("bytes sent via SQL*Net to client")),10," ","r") + \
                        gf_string_pad(gf_number_convert(v_threadlist[i].cgv_perf_stat_result.get("bytes received via SQL*Net from client")),10," ","r");

            v_count = v_count + 1;       

    if (v_istitle == 1):
        print gf_string_color(v_tittle_text,"blue");
    if (v_istitle == 0):
        print v_body_text;

def gf_suck_topvs_print(v_threadlist):
    #清理屏幕
    os.system("clear");

    v_topas_perf_dict = {};
    # 填充v_topas_perf_dict，格式为：{key:[db1_value,db2_value],,,,}
    if (len(v_threadlist) > 0):
        for v_key,v_value in v_threadlist[0].cgv_perf_stat_result.items():
            v_topvs_value = [];
            for i in xrange(len(v_threadlist)):
                try:
                    v_topvs_value.append(v_threadlist[i].cgv_perf_stat_result.get(v_key));
                except Exception, e:
                    v_topvs_value.extend("0");

            v_topas_perf_dict[v_key] = v_topvs_value;

    if (v_topas_perf_dict):
        v_text = "";
        v_db_info = "";
        # title打印
        for i in xrange(len(v_threadlist)):
            v_db_info = v_db_info + gf_string_handle(str(v_threadlist[i].cgv_ip) + ":" + str(v_threadlist[i].cgv_port) + ":" + str(v_threadlist[i].cgv_sid),'-',35,'-',1);

        v_text = gf_string_handle('Core Metric','-',45,'-') + v_db_info;
        print gf_string_color(v_text,"blue");
        v_text = "";

        for x in (v_topas_perf_dict.get("session logical reads")):
            v_text = v_text + gf_string_handle(gf_number_convert(x),' ',35,' ',1);

        v_text = gf_string_handle("session logical reads",' ',45,' ',2) + v_text;
        print v_text;
        v_text = "";

        for x in (v_topas_perf_dict.get("execute count")):
            v_text = v_text + gf_string_handle(gf_number_convert(x),' ',35,' ',1);

        v_text = gf_string_handle("execute count",' ',45,' ',2) + v_text;
        print v_text;
        v_text = "";

        for x in (v_topas_perf_dict.get("user commits")):
            v_text = v_text + gf_string_handle(gf_number_convert(x),' ',35,' ',1);

        v_text = gf_string_handle("user commits",' ',45,' ',2) + v_text;
        print v_text;
        v_text = "";

        for x in (v_topas_perf_dict.get("redo writes")):
            v_text = v_text + gf_string_handle(gf_number_convert(x),' ',35,' ',1);

        v_text = gf_string_handle("redo writes",' ',45,' ',2) + v_text;
        print v_text;
        v_text = "";


        for x in (v_topas_perf_dict.get("db block changes")):
            v_text = v_text + gf_string_handle(gf_number_convert(x),' ',35,' ',1);

        v_text = gf_string_handle("db block changes",' ',45,' ',2) + v_text;
        print v_text;
        v_text = "";

        for x in (v_topas_perf_dict.get("redo size")):
            v_text = v_text + gf_string_handle(gf_number_convert(x),' ',35,' ',1);

        v_text = gf_string_handle("redo size",' ',45,' ',2) + v_text;
        print v_text;
        v_text = "";

        for x in (v_topas_perf_dict.get("ConCur Trans")):
            v_text = v_text + gf_string_handle(gf_number_convert(x),' ',35,' ',1);

        v_text = gf_string_handle("ConCur Trans",' ',45,' ',2) + v_text;
        print v_text;
        v_text = "";

        for x in (v_topas_perf_dict.get("parse count (hard)")):
            v_text = v_text + gf_string_handle(gf_number_convert(x),' ',35,' ',1);

        v_text = gf_string_handle("parse count (hard)",' ',45,' ',2) + v_text;
        print v_text;
        v_text = "";

        v_text = gf_string_handle('PGA & Session & Process','-',45,'-') + v_db_info;
        print gf_string_color(v_text,"blue");
        v_text = "";

        for x in (v_topas_perf_dict.get("DB time(s)")):
            v_text = v_text + gf_string_handle(gf_number_convert(x),' ',35,' ',1);

        v_text = gf_string_handle("DB time(s)",' ',45,' ',2) + v_text;
        print v_text;
        v_text = "";

        for x in (v_topas_perf_dict.get("CPU time(s)")):
            v_text = v_text + gf_string_handle(gf_number_convert(x),' ',35,' ',1);

        v_text = gf_string_handle("CPU time(s)",' ',45,' ',2) + v_text;
        print v_text;
        v_text = "";

        for x in (v_topas_perf_dict.get("User Active Sessions")):
            v_text = v_text + gf_string_handle(gf_number_convert(x),' ',35,' ',1);

        v_text = gf_string_handle("User Active Sessions",' ',45,' ',2) + v_text;
        print v_text;
        v_text = "";

        for x in (v_topas_perf_dict.get("Processes")):
            v_text = v_text + gf_string_handle(gf_number_convert(x),' ',35,' ',1);

        v_text = gf_string_handle("Processes",' ',45,' ',2) + v_text;
        print v_text;
        v_text = "";

        v_text = gf_string_handle('IO Latency','-',45,'-') + v_db_info;
        print gf_string_color(v_text,"blue");
        v_text = "";

        for x in (v_topas_perf_dict.get("db file sequential read(ms)")):
            v_text = v_text + gf_string_handle(gf_number_convert(x),' ',35,' ',1);

        v_text = gf_string_handle("db file sequential read(ms)",' ',45,' ',2) + v_text;
        print v_text;
        v_text = "";

        for x in (v_topas_perf_dict.get("log file sync(ms)")):
            v_text = v_text + gf_string_handle(gf_number_convert(x),' ',35,' ',1);

        v_text = gf_string_handle("log file sync(ms)",' ',45,' ',2) + v_text;
        print v_text;
        v_text = "";

        for x in (v_topas_perf_dict.get("log file parallel write(ms)")):
            v_text = v_text + gf_string_handle(gf_number_convert(x),' ',35,' ',1);

        v_text = gf_string_handle("log file parallel write(ms)",' ',45,' ',2) + v_text;
        print v_text;
        v_text = "";

        for x in (v_topas_perf_dict.get("direct path read(ms)")):
            v_text = v_text + gf_string_handle(gf_number_convert(x),' ',35,' ',1);

        v_text = gf_string_handle("direct path read(ms)",' ',45,' ',2) + v_text;
        print v_text;
        v_text = "";

        for x in (v_topas_perf_dict.get("direct path write(ms)")):
            v_text = v_text + gf_string_handle(gf_number_convert(x),' ',35,' ',1);

        v_text = gf_string_handle("direct path write(ms)",' ',45,' ',2) + v_text;
        print v_text;
        v_text = "";

        v_text = gf_string_handle('IOPS & IOBytes','-',45,'-') + v_db_info;
        print gf_string_color(v_text,"blue");
        v_text = "";

        for x in (v_topas_perf_dict.get("Datafile IOPS")):
            v_text = v_text + gf_string_handle(gf_number_convert(x),' ',35,' ',1);

        v_text = gf_string_handle("Datafile IOPS",' ',45,' ',2) + v_text;
        print v_text;
        v_text = "";

        for x in (v_topas_perf_dict.get("Datafile IOBytes")):
            v_text = v_text + gf_string_handle(gf_number_convert(x),' ',35,' ',1);

        v_text = gf_string_handle("Datafile IOBytes",' ',45,' ',2) + v_text;
        print v_text;
        v_text = "";

        for x in (v_topas_perf_dict.get("physical reads")):
            v_text = v_text + gf_string_handle(gf_number_convert(x),' ',35,' ',1);

        v_text = gf_string_handle("physical reads",' ',45,' ',2) + v_text;
        print v_text;
        v_text = "";

        for x in (v_topas_perf_dict.get("physical writes")):
            v_text = v_text + gf_string_handle(gf_number_convert(x),' ',35,' ',1);

        v_text = gf_string_handle("physical writes",' ',45,' ',2) + v_text;
        print v_text;
        v_text = "";

        v_text = gf_string_handle('RAC GC','-',45,'-') + v_db_info;
        print gf_string_color(v_text,"blue");
        v_text = "";

        for x in (v_topas_perf_dict.get("RAC GCBytes")):
            v_text = v_text + gf_string_handle(gf_number_convert(x),' ',35,' ',1);

        v_text = gf_string_handle("RAC GCBytes",' ',45,' ',2) + v_text;
        print v_text;
        v_text = "";

        for x in (v_topas_perf_dict.get("gc crblock recetime(ms)")):
            v_text = v_text + gf_string_handle(gf_number_convert(x),' ',35,' ',1);

        v_text = gf_string_handle("gc crblock recetime(ms)",' ',45,' ',2) + v_text;
        print v_text;
        v_text = "";

        for x in (v_topas_perf_dict.get("gc crblock senttime(ms)")):
            v_text = v_text + gf_string_handle(gf_number_convert(x),' ',35,' ',1);

        v_text = gf_string_handle("gc crblock senttime(ms)",' ',45,' ',2) + v_text;
        print v_text;
        v_text = "";

        v_text = gf_string_handle('Oracle Network','-',45,'-') + v_db_info;
        print gf_string_color(v_text,"blue");
        v_text = "";

        for x in (v_topas_perf_dict.get("bytes sent via SQL*Net to client")):
            v_text = v_text + gf_string_handle(gf_number_convert(x),' ',35,' ',1);

        v_text = gf_string_handle("bytes sent via SQL*Net to client",' ',45,' ',2) + v_text;
        print v_text;
        v_text = "";

        for x in (v_topas_perf_dict.get("bytes received via SQL*Net from client")):
            v_text = v_text + gf_string_handle(gf_number_convert(x),' ',35,' ',1);

        v_text = gf_string_handle("bytes received via SQL*Net from client",' ',45,' ',2) + v_text;
        print v_text;
        v_text = "";

        for x in (v_topas_perf_dict.get("SQL*Net roundtrips to/from client")):
            v_text = v_text + gf_string_handle(gf_number_convert(x),' ',35,' ',1);

        v_text = gf_string_handle("SQL*Net roundtrips to/from client",' ',45,' ',2) + v_text;
        print v_text;

        v_text = gf_string_handle("-","-",len(v_text),"-");
        print v_text;

#帮助，版本与授权信息
def gf_qostat_help():
    try:
        v_helptext = "***********************************************************************\n" + \
					 "****            WOQU Oracle Performance Sniffer Usage:             ****\n" + \
					 "****   python qostat.py -screen :Display statistics by bigscreen   ****\n" + \
					 "****   python qostat.py -line :Display statistics by linemodel     ****\n" + \
					 "****   python qostat.py -topvs :Display multi instance statistics  ****\n" + \
					 "***********************************************************************""";
        print v_helptext
        os._exit(0);
    except Exception, e:
        pass

class qostatthread(threading.Thread):
    def __init__(self,cfv_ip = None,cfv_port = None,cfv_sid = None,cfv_dbuser = None,cfv_dbpwd = None,cfv_interval = None,cfv_ssh_host = None):
        threading.Thread.__init__(self);
    	self.cgv_conn_db = None;
        self.cgv_db_cur = None;
        self.cgv_ssh_host = None;
    	self.cgv_ip = cfv_ip;
    	self.cgv_port = cfv_port;
    	self.cgv_sid = cfv_sid;
    	self.cgv_dbuser = cfv_dbuser;
    	self.cgv_dbpwd = cfv_dbpwd;
    	self.cgv_interval = cfv_interval;
        self.cgv_rac_traffic = 0;
        self.cgv_datafile_iops = 0;
        self.cgv_datafile_io_bytes = 0;
        #now用于记录当前采用值
        self.cgv_perf_stat_array_now = [];
        #last用于记录上次采用值，采用字典，是为了提高key值的get效率
        self.cgv_perf_stat_array_last = {};

        #线程阻塞控制标志
        self.cgv_pause_flag = threading.Event();
        self.cgv_is_running = threading.Event();
        self.cgv_finish_collect = False;

        #功能数组
        self.cgv_perf_stat_array = [];
        self.cgv_core_stat_array = [];
        self.cgv_iolatency_stat_array = [];
        self.cgv_iombps_stat_array = [];
        self.cgv_pgapro_stat_array = [];
        self.cgv_racgc_stat_array = [];
        self.cgv_network_stat_array = [];
        self.cgv_topevent_stat_array = [];

        #性能结果字典，便于Key查询
        self.cgv_perf_stat_result = {};

    def run(self):
        # 每轮采集，由子线程自己进行
        # 每轮结束，子线程将采集标志位设置为已完成，并自己进入等待状态
        # 外部主线程，开始结果打印，打印完毕，重置子线程的采集标志位为false，并唤醒子线程，开始下轮采集
        self.cgv_is_running.set();
        while (self.cgv_is_running.isSet()):
            #数据库性能信息采集
            self.cf_db_perf_stats_collect();
            #数据库性能信息计算分析
            self.cf_db_perf_stats_compute();
            #数组按照类型分组
            self.cf_metric_to_category();

            #表示该线程完成了自己的数据采集，即将进去阻塞等待状态
            self.cgv_finish_collect = True;
            #设置暂停状态，准备进入阻塞状态
            self.cf_pause();
            #进入阻塞状态，等待主线程完成打印输出后被唤醒
            self.cgv_pause_flag.wait();

        #结束循环时释放数据库连接
        self.cf_disconnect_db();

    # 暂停线程
    def cf_pause(self):
        self.cgv_pause_flag.clear();

    # 恢复线程
    def cf_resume(self):
        self.cgv_pause_flag.set();

    # 停止线程
    def cf_stop(self):
        self.cgv_is_running.clear();
        self.cf_resume();
        


    def cf_nowlist_to_lastdict(self):
        #初始化last字典
        self.cgv_perf_stat_array_last = {};
        for i in xrange(len(self.cgv_perf_stat_array_now)):
            #topevent名称可能与event部分重复，引发数据混乱，不需要进入到dict
            if (self.cgv_perf_stat_array_now[i][5] != 'topevent'):
                self.cgv_perf_stat_array_last[self.cgv_perf_stat_array_now[i][0]] = [self.cgv_perf_stat_array_now[i][1],
                self.cgv_perf_stat_array_now[i][2],
                self.cgv_perf_stat_array_now[i][3],
                self.cgv_perf_stat_array_now[i][4],
                self.cgv_perf_stat_array_now[i][5]];

    #数据库连接函数
    def cf_connect_db(self):
    	try:
    		cfv_dsn_tns = cx_Oracle.makedsn(self.cgv_ip,self.cgv_port,self.cgv_sid);
    		self.cgv_conn_db = cx_Oracle.Connection(self.cgv_dbuser,self.cgv_dbpwd,cfv_dsn_tns);
    		self.cgv_db_cur = self.cgv_conn_db.cursor();
    	except Exception, e:
    		print """db connect error:""" + self.cgv_ip + ":" + self.cgv_port + ":" + self.cgv_sid;
		print e;
    		os._exit(0);

    #数据库连接断开函数
    def cf_disconnect_db(self):
        try:
            if (self.cgv_db_cur):
                self.cgv_db_cur.close();
            if (self.cgv_conn_db):
                self.cgv_conn_db.close();
        except Exception, e:
            pass;

    #主机ssh连接函数
    def cf_connect_host(self):
        try:
            self.cgv_ssh_host = paramiko.SSHClient();
            self.cgv_ssh_host.set_missing_host_key_policy(paramiko.AutoAddPolicy());
            self.cgv_ssh_host.connect(self.cgv_ip,22,"root");
        except Exception,e:
            print """host connect error""" + self.cgv_ip;

    #主机ssh连接断开函数
    def cf_disconnect_host(self):
        try:
            self.cgv_ssh_host.close();
        except Exception,e:
            pass;

    #数组按照种类分解函数
    def cf_metric_to_category(self):
        #清空数组
        self.cgv_core_stat_array[:] = [];
        self.cgv_iolatency_stat_array[:] = [];
        self.cgv_iombps_stat_array[:] = [];
        self.cgv_pgapro_stat_array[:] = [];
        self.cgv_racgc_stat_array[:] = [];
        self.cgv_network_stat_array[:] = [];
        self.cgv_topevent_stat_array[:] = [];
        self.cgv_perf_stat_result = {};

        try:
            for i in xrange(len(self.cgv_perf_stat_array)):
                cfv_metric_name = self.cgv_perf_stat_array[i][0];
                cfv_metric_type = self.cgv_perf_stat_array[i][2];
                cfv_metric_value = self.cgv_perf_stat_array[i][1];
                #topevent，无需关注name，直接处理即可
                if (cfv_metric_type == 'topevent'):
                    #value即为排序值，不过要desc方式
                    self.cgv_topevent_stat_array.extend([[cfv_metric_name,cfv_metric_value,cfv_metric_value]]);
                else:
                    #Core Metric
                    if (cfv_metric_name == 'session logical reads'):
                        self.cgv_core_stat_array.extend([[cfv_metric_name,cfv_metric_value,1]]);
                    if (cfv_metric_name == 'execute count'):
                        self.cgv_core_stat_array.extend([[cfv_metric_name,cfv_metric_value,2]]);
                    if (cfv_metric_name == 'db block changes'):
                        self.cgv_core_stat_array.extend([[cfv_metric_name,cfv_metric_value,3]]);
                    if (cfv_metric_name == 'redo size'):
                        self.cgv_core_stat_array.extend([[cfv_metric_name,cfv_metric_value,4]]);
                    if (cfv_metric_name == 'parse count (total)'):
                        self.cgv_core_stat_array.extend([[cfv_metric_name,cfv_metric_value,5]]);
                    if (cfv_metric_name == 'parse count (hard)'):
                        self.cgv_core_stat_array.extend([[cfv_metric_name,cfv_metric_value,6]]);
                    if (cfv_metric_name == 'user commits'):
                        self.cgv_core_stat_array.extend([[cfv_metric_name,cfv_metric_value,7]]);
                    if (cfv_metric_name == 'redo writes'):
                        self.cgv_core_stat_array.extend([[cfv_metric_name,cfv_metric_value,8]]);
                    if (cfv_metric_name == 'ConCur Trans'):
                        self.cgv_core_stat_array.extend([[cfv_metric_name,cfv_metric_value,9]]);

                    #io latency
                    if (cfv_metric_name == 'db file sequential read(ms)'):
                        self.cgv_iolatency_stat_array.extend([[cfv_metric_name,cfv_metric_value,1]]);
                    if (cfv_metric_name == 'db file scattered read(ms)'):
                        self.cgv_iolatency_stat_array.extend([[cfv_metric_name,cfv_metric_value,2]]);
                    if (cfv_metric_name == 'log file sync(ms)'):
                        self.cgv_iolatency_stat_array.extend([[cfv_metric_name,cfv_metric_value,3]]);
                    if (cfv_metric_name == 'log file parallel write(ms)'):
                        self.cgv_iolatency_stat_array.extend([[cfv_metric_name,cfv_metric_value,4]]);
                    if (cfv_metric_name == 'direct path read(ms)'):
                        self.cgv_iolatency_stat_array.extend([[cfv_metric_name,cfv_metric_value,5]]);
                    if (cfv_metric_name == 'direct path write(ms)'):
                        self.cgv_iolatency_stat_array.extend([[cfv_metric_name,cfv_metric_value,6]]);
                    if (cfv_metric_name == 'direct path read temp(ms)'):
                        self.cgv_iolatency_stat_array.extend([[cfv_metric_name,cfv_metric_value,7]]);
                    if (cfv_metric_name == 'direct path write temp(ms)'):
                        self.cgv_iolatency_stat_array.extend([[cfv_metric_name,cfv_metric_value,8]]);

                    #iops & iobytes
                    if (cfv_metric_name == 'physical reads'):
                        self.cgv_iombps_stat_array.extend([[cfv_metric_name,cfv_metric_value,1]]);
                    if (cfv_metric_name == 'physical writes'):
                        self.cgv_iombps_stat_array.extend([[cfv_metric_name,cfv_metric_value,2]]);
                    if (cfv_metric_name == 'Datafile IOPS'):
                        self.cgv_iombps_stat_array.extend([[cfv_metric_name,cfv_metric_value,3]]);
                    if (cfv_metric_name == 'Datafile IOBytes'):
                        self.cgv_iombps_stat_array.extend([[cfv_metric_name,cfv_metric_value,4]]);
                    if (cfv_metric_name == 'db file sequential read'):
                        self.cgv_iombps_stat_array.extend([[cfv_metric_name,cfv_metric_value,5]]);
                    if (cfv_metric_name == 'db file scattered read'):
                        self.cgv_iombps_stat_array.extend([[cfv_metric_name,cfv_metric_value,6]]);
                    if (cfv_metric_name == 'log file sync'):
                        self.cgv_iombps_stat_array.extend([[cfv_metric_name,cfv_metric_value,7]]);
                    if (cfv_metric_name == 'log file parallel write'):
                        self.cgv_iombps_stat_array.extend([[cfv_metric_name,cfv_metric_value,8]]);
                    if (cfv_metric_name == 'direct path read'):
                        self.cgv_iombps_stat_array.extend([[cfv_metric_name,cfv_metric_value,9]]);
                    if (cfv_metric_name == 'direct path write'):
                        self.cgv_iombps_stat_array.extend([[cfv_metric_name,cfv_metric_value,10]]);
                    if (cfv_metric_name == 'direct path read temp'):
                        self.cgv_iombps_stat_array.extend([[cfv_metric_name,cfv_metric_value,11]]);
                    if (cfv_metric_name == 'direct path write temp'):
                        self.cgv_iombps_stat_array.extend([[cfv_metric_name,cfv_metric_value,12]]);

                    #session & pga & uptime
                    if (cfv_metric_name == 'Up time(Day)'):
                        self.cgv_pgapro_stat_array.extend([[cfv_metric_name,cfv_metric_value,1]]);
                    if (cfv_metric_name == 'DB time(s)'):
                        self.cgv_pgapro_stat_array.extend([[cfv_metric_name,cfv_metric_value,2]]);
                    if (cfv_metric_name == 'CPU time(s)'):
                        self.cgv_pgapro_stat_array.extend([[cfv_metric_name,cfv_metric_value,3]]);
                    if (cfv_metric_name == 'User Active Sessions'):
                        self.cgv_pgapro_stat_array.extend([[cfv_metric_name,cfv_metric_value,4]]);
                    if (cfv_metric_name == 'TOTAL PGA'):
                        self.cgv_pgapro_stat_array.extend([[cfv_metric_name,cfv_metric_value,5]]);
                    if (cfv_metric_name == 'AVG PGA'):
                        self.cgv_pgapro_stat_array.extend([[cfv_metric_name,cfv_metric_value,6]]);
                    if (cfv_metric_name == 'Processes'):
                        self.cgv_pgapro_stat_array.extend([[cfv_metric_name,cfv_metric_value,7]]);
                    if (cfv_metric_name == 'opened cursors'):
                        self.cgv_pgapro_stat_array.extend([[cfv_metric_name,cfv_metric_value,8]]);

                    #rac stat
                    if (cfv_metric_name == 'RAC GCBytes'):
                        self.cgv_racgc_stat_array.extend([[cfv_metric_name,cfv_metric_value,1]]);
                    if (cfv_metric_name == 'gc crblock recetime(ms)'):
                        self.cgv_racgc_stat_array.extend([[cfv_metric_name,cfv_metric_value,2]]);
                    if (cfv_metric_name == 'gc crblock senttime(ms)'):
                        self.cgv_racgc_stat_array.extend([[cfv_metric_name,cfv_metric_value,3]]);

                    #network
                    if (cfv_metric_name == 'bytes sent via SQL*Net to client'):
                        self.cgv_network_stat_array.extend([[cfv_metric_name,cfv_metric_value,1]]);
                    if (cfv_metric_name == 'bytes received via SQL*Net from client'):
                        self.cgv_network_stat_array.extend([[cfv_metric_name,cfv_metric_value,2]]);
                    if (cfv_metric_name == 'SQL*Net roundtrips to/from client'):
                        self.cgv_network_stat_array.extend([[cfv_metric_name,cfv_metric_value,3]]);

                    #结果转留一份到字典
                    self.cgv_perf_stat_result[cfv_metric_name] = cfv_metric_value;

            #排序操作
            self.cgv_core_stat_array.sort(key = lambda x:x[2]);
            self.cgv_iolatency_stat_array.sort(key = lambda x:x[2]);
            self.cgv_iombps_stat_array.sort(key = lambda x:x[2]);
            self.cgv_pgapro_stat_array.sort(key = lambda x:x[2]);
            self.cgv_racgc_stat_array.sort(key = lambda x:x[2]);
            self.cgv_network_stat_array.sort(key = lambda x:x[2]);
            #self.cgv_topevent_stat_array.sort(key = lambda x:x[2]);

        except Exception,e:
            pass;

    #数据库性能数据获取函数，函数完成后，last，now数据就绪
    def cf_db_perf_stats_collect(self):
        try:
            #交接班,将now交接给last
            self.cf_nowlist_to_lastdict();

            #大数组
            self.cgv_db_cur.execute(gv_big_sql);
            self.cgv_perf_stat_array_now = self.cgv_db_cur.fetchall();

            #process数组
            self.cgv_db_cur.execute(gv_process_sql);
            cfv_process_stat_array = self.cgv_db_cur.fetchall();

            #tempfile数组
            #self.cgv_db_cur.execute(gv_tempfile_sql);
            #cfv_tempfile_stat_array = self.cgv_db_cur.fetchall();

            #process信息补充到大数组中
            for rows in cfv_process_stat_array:
                t_array = [("Processes",rows[0],rows[1],0,"process","cur_val"),("AVG PGA",rows[0],rows[2],0,"process","cur_val"),("TOTAL PGA",rows[0],rows[3],0,"process","cur_val")];
                self.cgv_perf_stat_array_now.extend(t_array);

            #tempfile信息补充到大数组中
            #for rows in cfv_tempfile_stat_array:
            #    t_array = [["Tempfile IOPS",rows[0],rows[1],0,"tempstat","compute_val"],["Tempfile IOBytes",rows[0],rows[2],0,"tempstat","compute_val"]];
            #    self.cgv_perf_stat_array_now.extend(t_array);

        except Exception, e:
            print "DB stat collect get error : cf_db_perf_stats_collect!";
            print e;


    #性能数据计算
    def cf_db_perf_stats_compute(self):
        #恶心的gc响应时间指标统计
        cfv_gcblocks_receive = 0;
        cfv_gcblocks_served = 0;
        cfv_gcblocks_rectime = 0;
        cfv_gcblocks_senttime = 0;
        cfv_seconds_take = 1;

        # 清空cgv_perf_stat_array数组
        self.cgv_perf_stat_array[:] = [];

        #循环处理所有指标
        for i in xrange(len(self.cgv_perf_stat_array_now)):
            #获取now，last的指标名，指标值，时间差等信息
            cfv_name = self.cgv_perf_stat_array_now[i][0];
            cfv_cal_type = self.cgv_perf_stat_array_now[i][5];
            cfv_now_value = self.cgv_perf_stat_array_now[i][2];
            cfv_now_waittime = self.cgv_perf_stat_array_now[i][3];
            cfv_array = self.cgv_perf_stat_array_last.get(cfv_name);
            if (cfv_array):
                cfv_last_value = cfv_array[1];
            	cfv_last_waittime = cfv_array[2];
            	cfv_last_time = datetime.datetime.strptime(cfv_array[0],'%Y-%m-%d %H:%M:%S');
            else:
                cfv_last_value = 0;
                cfv_last_waittime = 0;
                cfv_last_time = datetime.datetime.now();

            cfv_now_time = datetime.datetime.strptime(self.cgv_perf_stat_array_now[i][1],'%Y-%m-%d %H:%M:%S');

            #计算时间间隔，并避免除零
            cfv_seconds_take = (cfv_now_time - cfv_last_time).seconds;
            if (cfv_seconds_take <= 0):
                cfv_seconds_take = 1;

            #等待事件，分两种记录，1:延迟latency 2:每秒事件次数
            if (cfv_cal_type == "event"):
                if ((cfv_now_value > cfv_last_value) and (cfv_now_waittime > cfv_last_waittime) and cfv_seconds_take > 0):
                    #延迟latency
                    self.cgv_perf_stat_array.extend([[cfv_name + "(ms)",round((cfv_now_waittime - cfv_last_waittime) / (cfv_now_value - cfv_last_value),2),'latency']]);

                    #每秒事件次数
                    self.cgv_perf_stat_array.extend([[cfv_name,round((cfv_now_value - cfv_last_value) / cfv_seconds_take,2),'event']]);
                else:
                    #延迟latency
                    self.cgv_perf_stat_array.extend([[cfv_name + "(ms)",0,'latency']]);

                    #每秒事件次数
                    self.cgv_perf_stat_array.extend([[cfv_name,0,'event']]);

            #RAC心跳流量，多个指标叠加
            elif (cfv_cal_type == "rac"):
                if ((cfv_now_value > cfv_last_value) and cfv_seconds_take > 0):
                    self.cgv_rac_traffic = self.cgv_rac_traffic + (cfv_now_value - cfv_last_value);
                    #gc时间统计
                    if (cfv_name == 'gc cr blocks received'):
                        cfv_gcblocks_receive = cfv_now_value - cfv_last_value;
                    if (cfv_name == 'gc cr blocks served'):
                        cfv_gcblocks_served = cfv_now_value - cfv_last_value;

            #IOPS，多个指标叠加
            elif (cfv_cal_type == "iops"):
                if ((cfv_now_value > cfv_last_value) and cfv_seconds_take > 0):
                    self.cgv_datafile_iops = self.cgv_datafile_iops + (cfv_now_value - cfv_last_value);
            #IO吞吐，多个指标叠加
            elif (cfv_cal_type == "mbps"):
                if ((cfv_now_value > cfv_last_value) and cfv_seconds_take > 0):
                    self.cgv_datafile_io_bytes = self.cgv_datafile_io_bytes + (cfv_now_value - cfv_last_value);
            #普通计算类型
            elif (cfv_cal_type == "compute_val"):
                if ((cfv_now_value > cfv_last_value) and cfv_seconds_take > 0):
		          #gc时间计算，仅记录数据，循环后再入打印数组
                    if (cfv_name == 'gc cr block receive time'):
                        cfv_gcblocks_rectime = cfv_now_value - cfv_last_value;
                    elif (cfv_name == 'gc cr block send time'):
                        cfv_gcblocks_senttime = cfv_now_value - cfv_last_value;
                    else:
                        #正常计算，入打印数组
                        self.cgv_perf_stat_array.extend([[cfv_name,round((cfv_now_value - cfv_last_value) / cfv_seconds_take,2),'compute_val']]);
                else:
                    self.cgv_perf_stat_array.extend([[cfv_name,0,'compute_val']]);
            #普通直接取值类型
            elif (cfv_cal_type == "cur_val"):
                self.cgv_perf_stat_array.extend([[cfv_name,cfv_now_value,'cur_val']]);
            #topevent类型，直接取值
            elif (cfv_cal_type == "topevent"):
                self.cgv_perf_stat_array.extend([[cfv_name,cfv_now_value,'topevent']]);
            else:
                pass

        #RAC叠加指标开始记录
        self.cgv_perf_stat_array.extend([['RAC GCBytes',round(self.cgv_rac_traffic / cfv_seconds_take,2),'rac']]);

        #IOPS叠加指标开始记录
        self.cgv_perf_stat_array.extend([['Datafile IOPS',round(self.cgv_datafile_iops / cfv_seconds_take,2),'iops']]);

        #MBPS叠加指标开始记录
        self.cgv_perf_stat_array.extend([['Datafile IOBytes',round(self.cgv_datafile_io_bytes / cfv_seconds_take,2),'mbps']]);

    	#gc_crblock receivetime
    	if (cfv_gcblocks_receive > 0):
    	    self.cgv_perf_stat_array.extend([['gc crblock recetime(ms)',round(cfv_gcblocks_rectime / cfv_gcblocks_receive,2),'rac']]);
    	else:
    	    self.cgv_perf_stat_array.extend([['gc crblock recetime(ms)',0,'rac']]);

        #gc_crblock senttime
        if (cfv_gcblocks_served > 0):
            self.cgv_perf_stat_array.extend([['gc crblock senttime(ms)',round(cfv_gcblocks_senttime / cfv_gcblocks_served,2),'rac']]);
        else:
            self.cgv_perf_stat_array.extend([['gc crblock senttime(ms)',0,'rac']]);

        #指标清零
        self.cgv_rac_traffic = 0;
        self.cgv_datafile_iops = 0;
        self.cgv_datafile_io_bytes = 0;

if __name__ == "__main__":
    #处理程序中断或Ctrl+C
    signal.signal(signal.SIGTERM,gf_exit_handler);
    signal.signal(signal.SIGQUIT,gf_exit_handler);
    signal.signal(signal.SIGINT,gf_exit_handler);

    #全局变量定义
    gv_continue_boolean = True;
    gv_row_count = 0;
    gv_count = 0;
    gv_argv = "";
    gv_interval_seconds = 1;
    gv_conf_list = [];

    #解析conf文件，获取连接信息
    try:
    	gv_conf_file.read("qostat.conf");
        gv_interval_seconds = gv_conf_file.get("Interval",'interval_seconds');
        for v_section in gv_conf_file.sections():
            if (v_section != "Interval"):
                gv_conf_list.extend([[gv_conf_file.get(v_section,'db_ip'), \
                                gv_conf_file.get(v_section,'db_port'), \
                                gv_conf_file.get(v_section,'db_sid'), \
                                gv_conf_file.get(v_section,'db_user'), \
                                gv_conf_file.get(v_section,'db_pwd'), \
                                gv_interval_seconds]]);
    except Exception,e:
        print "read qostat.conf get error，please check!!!";
        os._exit(0);

    #获取程序输入参数
    try:
        if (len(sys.argv) > 0):
            v_pname,gv_argv = sys.argv[1].split("-");
    except Exception,e:
        gv_argv = "line";

    # 非对比模式，line／screen模式，单进程处理
    if (gv_argv == "screen"):
        #初始化及连接数据库
        if (gv_conf_list):
            v_thread = qostatthread(gv_conf_list[0][0],gv_conf_list[0][1],gv_conf_list[0][2],gv_conf_list[0][3],gv_conf_list[0][4],gv_conf_list[0][5],None);
            v_thread.cf_connect_db();
            v_thread.start();

        #循环工作
        while(1 == 1):
            #大屏式打印
            gf_suck_pstat_print(v_thread.cgv_perf_stat_array,v_thread.cgv_core_stat_array,v_thread.cgv_pgapro_stat_array,v_thread.cgv_iolatency_stat_array,v_thread.cgv_iombps_stat_array,v_thread.cgv_racgc_stat_array,v_thread.cgv_network_stat_array,v_thread.cgv_topevent_stat_array);

            # 重置采集线程的cgv_finish_collect，并让线程开启下轮的数据采集
            v_thread.cgv_finish_collect = False;
            v_thread.cf_resume();

            #think time
            time.sleep(int(gv_interval_seconds));

            #ctrl＋c退出处理
            if (gv_continue_boolean == False):
                v_thread.cf_stop();
                print("qostat exit!!!");
                os._exit(0);

    # 多库对比模式
    if (gv_argv == "topvs"):
        v_threadlist = [];
        #启动多个采集子线程，进行多库采集
        for i in xrange(len(gv_conf_list)):
            v_thread = qostatthread(gv_conf_list[i][0],gv_conf_list[i][1],gv_conf_list[i][2],gv_conf_list[i][3],gv_conf_list[i][4],gv_conf_list[i][5],None);
            v_thread.cf_connect_db();
            v_thread.start();
            v_threadlist.append(v_thread);

        #多库对比打印
        while (1 == 1):
            v_is_continue = True;
            #是否存在未完成采集的子线程
            for v_thread in v_threadlist:
                if (v_thread.cgv_finish_collect == False):
                    v_is_continue = False;

            if (v_is_continue):
                # 打印对比信息,topvs
                gf_suck_topvs_print(v_threadlist);
                # 重置每个采集线程的cgv_finish_collect，并让每个线程开启下轮的数据采集
                for v_thread in v_threadlist:
                    v_thread.cgv_finish_collect = False;
                    v_thread.cf_resume();
            else:
                #如果存在子线程未完成采集的情况，放弃本轮打印
                pass;

            time.sleep(int(gv_interval_seconds));

            #ctrl＋c退出处理
            if (gv_continue_boolean == False):
                for v_thread in v_threadlist:
                    v_thread.cf_stop();   

                print("qostat exit!!!");
                os._exit(0);

    # line模式
    if (gv_argv == "line" or gv_argv is None):
        v_threadlist = [];
        #启动多个采集子线程，进行多库采集
        for i in xrange(len(gv_conf_list)):
            v_thread = qostatthread(gv_conf_list[i][0],gv_conf_list[i][1],gv_conf_list[i][2],gv_conf_list[i][3],gv_conf_list[i][4],gv_conf_list[i][5],None);
            v_thread.cf_connect_db();
            v_thread.start();
            v_threadlist.append(v_thread);

        #多库对比打印
        while (1 == 1):
            v_is_continue = True;
            #是否存在未完成采集的子线程
            for v_thread in v_threadlist:
                if (v_thread.cgv_finish_collect == False):
                    v_is_continue = False;

            if (v_is_continue):
                #行模式刷屏
                #获取当前屏幕最大rows数量
                v_screen_rows,v_screen_columns = os.popen('stty size', 'r').read().split();
                #超出屏幕或第一次打印，打印title，计数器清零
                if (gv_row_count >= int(v_screen_rows) - len(v_threadlist) or gv_count == 0):
                    gf_suck_vstat_print('none',1,v_threadlist);
                    gv_row_count = 0;
                    gv_row_count = gv_row_count + 1;
                #未超出屏幕，打印row即可
                elif (gv_row_count < int(v_screen_rows) - len(v_threadlist)):
                    gf_suck_vstat_print('none',0,v_threadlist);

                    gv_row_count = gv_row_count + len(v_threadlist);

                gv_count = gv_count + 1;

                # 重置每个采集线程的cgv_finish_collect，并让每个线程开启下轮的数据采集
                for v_thread in v_threadlist:
                    v_thread.cgv_finish_collect = False;
                    v_thread.cf_resume();
            else:
                #如果存在子线程未完成采集的情况，放弃本轮打印
                pass;

            time.sleep(int(gv_interval_seconds));

            #ctrl＋c退出处理
            if (gv_continue_boolean == False):
                for v_thread in v_threadlist:
                    v_thread.cf_stop();   

                print("qostat exit!!!");
                os._exit(0);

    # 帮助信息打印
    if (gv_argv == "help"):
        gf_qostat_help();



