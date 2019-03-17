#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
 ScriptName: q_sql_parseX.py
 Author: chun.luo@woqutech.com
 Create Date: 2019-3-10 22:40
 Modify Author: chun.luo@woqutech.com
 Modify Date: 2019-3-10 22:40
 Remark: sql解析，分析select，from，where
 MusicBand: Always remember us this way!!!
"""


import sqlparse
from sqlparse.sql import IdentifierList,Identifier,Where,Comparison
from sqlparse.tokens import Keyword,DML

def cgf_substr_find(v_substr, v_str, i = 1):
    count = 0;
    while i > 0:
        index = v_str.find(v_substr);
        if index == -1:
            return -1;
        else:
            v_str = v_str[index+1:];   #第一次出现该字符串后后面的字符
            i -= 1;
            count = count + index + 1;   #位置数总加起来
    return count;

def cgf_is_during_quotation(v_substr,v_str):
    """判断一个字符串是否是在一个字符串内部，并在里面带有引号"""
    v_count = v_str.upper().count(v_substr);
    if (v_count > 0):
        for i in xrange(v_count):
            v_left_pos = cgf_substr_find(v_substr,v_str.upper(),i + 1) - 1;
            v_right_pos = v_left_pos + len(v_substr) + 1;
            v_judge_x = v_str.upper()[0:v_left_pos].count("'");
            if ((v_judge_x % 2) == 0):
                return False;
            else:
                return True;
            v_judeg_x = v_str.upper()[0:v_left_pos].count('"');
            if ((v_judge_x % 2) == 0):
                return False;
            else:
                return True;
            v_judeg_x = v_str.upper()[v_right_pos - 1:].count("'");
            if ((v_judge_x % 2) == 0):
                return False;
            else:
                return True;
            v_judeg_x = v_str.upper()[v_right_pos - 1:].count('"');
            if ((v_judge_x % 2) == 0):
                return False;
            else:
                return True;
    else:
        # 不存在这个字符串，直接false
        return False;


def cgf_sql_simpler(v_sql_text = None):
    """去除换行，空格保留一个"""
    v_tmp_sql = v_sql_text.strip().replace("\r"," ").replace("\n"," ");
    v_tmp_sql = " ".join(v_tmp_sql.split()).replace("( + )","(+)").replace("(+ )","(+)").replace("( +)","(+)");
    return v_tmp_sql;

def cgf_sql_exist_leftlike(v_sql_text = None):
    """判断sql是否存在左模糊"""
    v_is_leftlike = False;

    v_count = v_sql_text.upper().count("LIKE '%");
    if (v_count > 0):
        for i in xrange(v_count):
            v_left_pos = cgf_substr_find("LIKE '%",v_sql_text.upper(),i + 1) - 1;
            v_right_pos = v_left_pos + len("LIKE '%") + 1;
            v_judge_x = v_sql_text.upper()[0:v_left_pos].count("'");
            if ((v_judge_x % 2) == 0):
                return True;
            v_judeg_x = v_sql_text.upper()[0:v_left_pos].count('"');
            if ((v_judge_x % 2) == 0):
                return True;
            v_judeg_x = v_sql_text.upper()[v_right_pos - 1:].count("'");
            if ((v_judge_x % 2) == 0):
                return True;
            v_judeg_x = v_sql_text.upper()[v_right_pos - 1:].count('"');
            if ((v_judge_x % 2) == 0):
                return True;

    v_count = v_sql_text.upper().count('LIKE "%');
    if (v_count > 0):
        for i in xrange(v_count):
            v_left_pos = cgf_substr_find('LIKE "%',v_sql_text.upper(),i + 1) - 1;
            v_right_pos = v_left_pos + len('LIKE "%') + 1;
            v_judge_x = v_sql_text.upper()[0:v_left_pos].count("'");
            if ((v_judge_x % 2) == 0):
                return True;
            v_judeg_x = v_sql_text.upper()[0:v_left_pos].count('"');
            if ((v_judge_x % 2) == 0):
                return True;
            v_judeg_x = v_sql_text.upper()[v_right_pos - 1:].count("'");
            if ((v_judge_x % 2) == 0):
                return True;
            v_judeg_x = v_sql_text.upper()[v_right_pos - 1:].count('"');
            if ((v_judge_x % 2) == 0):
                return True;

    return v_is_leftlike;

def cgf_sql_exist_outjoin(v_sql_text = None):
    """判断SQL是否存在外连接"""
    v_is_outjoin = False;
    v_count = v_sql_text.upper().count("LEFT JOIN");
    if (v_count > 0):
        for i in xrange(v_count):
            v_left_pos = cgf_substr_find("LEFT JOIN",v_sql_text.upper(),i + 1) - 1;
            v_right_pos = v_left_pos + len("LEFT JOIN") + 1;
            v_judge_x = v_sql_text.upper()[0:v_left_pos].count("'");
            if ((v_judge_x % 2) == 0):
                return True;
            v_judeg_x = v_sql_text.upper()[0:v_left_pos].count('"');
            if ((v_judge_x % 2) == 0):
                return True;
            v_judeg_x = v_sql_text.upper()[v_right_pos - 1:].count("'");
            if ((v_judge_x % 2) == 0):
                return True;
            v_judeg_x = v_sql_text.upper()[v_right_pos - 1:].count('"');
            if ((v_judge_x % 2) == 0):
                return True;

    v_count = v_sql_text.upper().count("RIGHT JOIN");
    if (v_count > 0):
        for i in xrange(v_count):
            v_left_pos = cgf_substr_find("RIGHT JOIN",v_sql_text.upper(),i + 1) - 1;
            v_right_pos = v_left_pos + len("RIGHT JOIN") + 1;
            v_judge_x = v_sql_text.upper()[0:v_left_pos].count("'");
            if ((v_judge_x % 2) == 0):
                return True;
            v_judeg_x = v_sql_text.upper()[0:v_left_pos].count('"');
            if ((v_judge_x % 2) == 0):
                return True;
            v_judeg_x = v_sql_text.upper()[v_right_pos - 1:].count("'");
            if ((v_judge_x % 2) == 0):
                return True;
            v_judeg_x = v_sql_text.upper()[v_right_pos - 1:].count('"');
            if ((v_judge_x % 2) == 0):
                return True;

    v_count = v_sql_text.upper().count("OUTER JOIN");
    if (v_count > 0):
        for i in xrange(v_count):
            v_left_pos = cgf_substr_find("OUTER JOIN",v_sql_text.upper(),i + 1) - 1;
            v_right_pos = v_left_pos + len("OUTER JOIN") + 1;
            v_judge_x = v_sql_text.upper()[0:v_left_pos].count("'");
            if ((v_judge_x % 2) == 0):
                return True;
            v_judeg_x = v_sql_text.upper()[0:v_left_pos].count('"');
            if ((v_judge_x % 2) == 0):
                return True;
            v_judeg_x = v_sql_text.upper()[v_right_pos - 1:].count("'");
            if ((v_judge_x % 2) == 0):
                return True;
            v_judeg_x = v_sql_text.upper()[v_right_pos - 1:].count('"');
            if ((v_judge_x % 2) == 0):
                return True;

    v_count = v_sql_text.upper().count("CROSS JOIN");
    if (v_count > 0):
        for i in xrange(v_count):
            v_left_pos = cgf_substr_find("CROSS JOIN",v_sql_text.upper(),i + 1) - 1;
            v_right_pos = v_left_pos + len("CROSS JOIN") + 1;
            v_judge_x = v_sql_text.upper()[0:v_left_pos].count("'");
            if ((v_judge_x % 2) == 0):
                return True;
            v_judeg_x = v_sql_text.upper()[0:v_left_pos].count('"');
            if ((v_judge_x % 2) == 0):
                return True;
            v_judeg_x = v_sql_text.upper()[v_right_pos - 1:].count("'");
            if ((v_judge_x % 2) == 0):
                return True;
            v_judeg_x = v_sql_text.upper()[v_right_pos - 1:].count('"');
            if ((v_judge_x % 2) == 0):
                return True;

    v_count = v_sql_text.upper().count("(+)");
    if (v_count > 0):
        for i in xrange(v_count):
            v_left_pos = cgf_substr_find("(+)",v_sql_text.upper(),i + 1) - 1;
            v_right_pos = v_left_pos + len("(+)") + 1;
            v_judge_x = v_sql_text.upper()[0:v_left_pos].count("'");
            if ((v_judge_x % 2) == 0):
                return True;
            v_judeg_x = v_sql_text.upper()[0:v_left_pos].count('"');
            if ((v_judge_x % 2) == 0):
                return True;
            v_judeg_x = v_sql_text.upper()[v_right_pos - 1:].count("'");
            if ((v_judge_x % 2) == 0):
                return True;
            v_judeg_x = v_sql_text.upper()[v_right_pos - 1:].count('"');
            if ((v_judge_x % 2) == 0):
                return True;

    return v_is_outjoin;



def cgf_is_subselect(v_token_list = None):
    """判断是否是子查询模块"""
    if (not v_token_list.is_group):
        # 具备tokens属性，可以嵌套
        return False
    for v_token in v_token_list.tokens:
        # 该token为select子查询
        if (v_token.ttype is DML and v_token.value.upper() == 'SELECT'):
            return True
    return False

def cgf_subsql_cutoff(v_sql_text = None):
    """sql剥离括号，传入的为子查询带括号的sql，例如(select xxx),或者(select xxx) a"""
    if (v_sql_text.count("(") != v_sql_text.count(")")):
        # 括号不对称，表示子查询传入的有问题
        return "nosub";

    if (len(v_sql_text.strip()) < 1):
        # 空字符串，直接返回
        return "nosub";

    if (v_sql_text.strip()[0] != "("):
        # 不以括号开头，就不是标准的子查询，直接返回
        return "nosub";

    # 剥离最外层的括号
    v_tmp_sql = v_sql_text.strip();
    v_start_pos = v_tmp_sql.find("(") + 1;
    v_end_pos = v_tmp_sql.rfind(")");
    v_return_sql = v_tmp_sql[v_start_pos:v_end_pos].strip();

    # 如果里层仍然是括号，递归剥下去
    if (len(v_return_sql) > 0 and v_return_sql[0] == "("):
        v_return_sql = cgf_subsql_cutoff(v_return_sql);

    # sql返回
    return v_return_sql;

def cgf_selsql_dissect(v_token_list = None,v_level = 0,v_table_list = None,v_compare_list = None,v_where_list = None):
    """获取SQL的table list，解析from与where之间的关键字进行获取"""
    v_seen_from = False;
    v_seen_where = False;
    v_seen_select = False;
    v_where_value = None;

    # 如果不是select语句，直接退出
    if (cgf_is_subselect(v_token_list) == False):
        return False;

    for i in xrange(len(v_token_list.tokens)):
        ############################################select模块处理############################################
        if (v_token_list.tokens[i].ttype is DML and v_token_list.tokens[i].value.upper() == "SELECT"):
            v_seen_select = True;
            continue;

        if (v_seen_select == True):
            pass;
        ############################################from模块处理############################################
        # 如果到达from语句，表示后面是table list模块，开启处理标志位
        if (v_token_list.tokens[i].ttype is sqlparse.tokens.Keyword and v_token_list.tokens[i].value.upper() == "FROM"):
            v_seen_select = False;
            v_seen_from = True;
            continue;

        # 在from和where之间的信息，IdentifierList和Identifier都是表名，这逻辑要对
        if (v_seen_from == True):
            # IdentifierList,主要应对from table_a,table_b之类的情况
            if (isinstance(v_token_list.tokens[i], sqlparse.sql.IdentifierList)):
                for v_identifier in v_token_list.tokens[i].get_identifiers():
                    v_table_list.append([v_level,v_identifier,v_identifier.value,v_identifier.get_real_name()]);

                    # 针对子查询，特殊处理
                    v_subsql_text = cgf_subsql_cutoff(v_identifier.value);      
                    if (v_subsql_text != "nosub"):
                        # 子查询，进行重新解析，如果确实是select sql，递归进行处理
                        v_sql_parser = sqlparse.parse(v_subsql_text);
                        if (cgf_is_subselect(v_token_list = v_sql_parser[0]) == True):
                            cgf_selsql_dissect(v_token_list = v_sql_parser[0],v_level = v_level + 1,v_table_list = v_table_list,
                                v_compare_list = v_compare_list,v_where_list = v_where_list);

            # Identifier，应对inner join的表名
            elif (isinstance(v_token_list.tokens[i], sqlparse.sql.Identifier)):
                v_table_list.append([v_level,v_token_list.tokens[i],v_token_list.tokens[i].value,v_token_list.tokens[i].get_real_name()]);

                # 针对子查询，特殊处理
                v_subsql_text = cgf_subsql_cutoff(v_token_list.tokens[i].value);
                if (v_subsql_text != "nosub"):
                    # 子查询，进行重新解析，如果确实是select sql，递归进行处理
                    v_sql_parser = sqlparse.parse(v_subsql_text);
                    if (cgf_is_subselect(v_token_list = v_sql_parser[0]) == True):
                        cgf_selsql_dissect(v_token_list = v_sql_parser[0],v_level = v_level + 1,v_table_list = v_table_list,
                            v_compare_list = v_compare_list,v_where_list = v_where_list);

            # Comparison,用于处理inner join模式的运算，例如a inner join b on a.id = b.id
            elif (isinstance(v_token_list.tokens[i], sqlparse.sql.Comparison)):
                # 1:表示表达式左边，2:表示表达式运算，例如=,!=,<>,>,<,>=,<=,3:表示表达式右边
                v_compare_bit = 1;
                v_left_item = None;
                v_right_item = None;
                v_compare_item = None;

                for v_token_sub in v_token_list.tokens[i].tokens:
                    if (str(v_token_sub.ttype) not in ("Token.Text.Whitespace","Token.Text.Whitespace.Newline")):
                        # 表达式左边逻辑处理
                        if (v_compare_bit == 1):
                            v_left_item = v_token_sub.value;
                            # 括号内部，可以递归解析
                            if (isinstance(v_token_sub, sqlparse.sql.Parenthesis)):
                                v_subsql_text = cgf_subsql_cutoff(v_token_sub.value);
                                if (v_subsql_text != "nosub"):
                                    # 子查询，进行重新解析，如果确实是select sql，递归进行处理
                                    v_sql_parser = sqlparse.parse(v_subsql_text);
                                    if (cgf_is_subselect(v_token_list = v_sql_parser[0]) == True):
                                        cgf_selsql_dissect(v_token_list = v_sql_parser[0],v_level = v_level + 1,
                                            v_table_list = v_table_list,v_compare_list = v_compare_list,v_where_list = v_where_list);

                        # 表达式符号处理
                        if (v_compare_bit == 2):
                            if (str(v_token_sub.ttype) == "Token.Operator.Comparison"):
                                v_compare_item = v_token_sub.value;

                        # 表达式右边逻辑处理
                        if (v_compare_bit == 3):
                            v_right_item = v_token_sub.value;
                            # 括号内部，可以递归解析
                            if (isinstance(v_token_sub, sqlparse.sql.Parenthesis)):
                                v_subsql_text = cgf_subsql_cutoff(v_token_sub.value);
                                if (v_subsql_text != "nosub"):
                                    # 子查询，进行重新解析，如果确实是select sql，递归进行处理
                                    v_sql_parser = sqlparse.parse(v_subsql_text);
                                    if (cgf_is_subselect(v_token_list = v_sql_parser[0]) == True):
                                        cgf_selsql_dissect(v_token_list = v_sql_parser[0],v_level = v_level + 1,
                                            v_table_list = v_table_list,v_compare_list = v_compare_list,v_where_list = v_where_list);

                        # 标志位递增
                        v_compare_bit = v_compare_bit + 1;

                        # 记录v_compare_list
                        if (v_compare_bit == 4):
                            # 入列表
                            v_compare_list.append([v_level,v_token_list.tokens[i],v_left_item,v_compare_item,v_right_item]);

                            # 标志位重置
                            v_compare_bit = 1;

                            # 数值重置
                            v_left_item = None;
                            v_right_item = None;
                            v_compare_item = None;
                

        ############################################where模块处理############################################
        if (isinstance(v_token_list.tokens[i],sqlparse.sql.Where)):
            v_seen_from = False;
            v_seen_where = True;
            v_where_value = v_token_list.tokens[i].value;

            for v_token in v_token_list.tokens[i].tokens:
                if isinstance(v_token, sqlparse.sql.Comparison):
                    # 1:表示表达式左边，2:表示表达式运算，例如=,!=,<>,>,<,>=,<=,3:表示表达式右边
                    v_compare_bit = 1;
                    v_left_item = None;
                    v_right_item = None;
                    v_compare_item = None;
                    for v_token_sub in v_token.tokens:
                        if (str(v_token_sub.ttype) not in ("Token.Text.Whitespace","Token.Text.Whitespace.Newline")):
                            # 表达式左边逻辑处理
                            if (v_compare_bit == 1):
                                v_left_item = v_token_sub.value;
                                # 括号内部，可以递归解析
                                if (isinstance(v_token_sub, sqlparse.sql.Parenthesis)):
                                    v_subsql_text = cgf_subsql_cutoff(v_token_sub.value);
                                    if (v_subsql_text != "nosub"):
                                        # 子查询，进行重新解析，如果确实是select sql，递归进行处理
                                        v_sql_parser = sqlparse.parse(v_subsql_text);
                                        if (cgf_is_subselect(v_token_list = v_sql_parser[0]) == True):
                                            cgf_selsql_dissect(v_token_list = v_sql_parser[0],v_level = v_level + 1,
                                                v_table_list = v_table_list,v_compare_list = v_compare_list,v_where_list = v_where_list);

                            # 表达式符号处理
                            if (v_compare_bit == 2):
                                if (str(v_token_sub.ttype) == "Token.Operator.Comparison"):
                                    v_compare_item = v_token_sub.value;

                            # 表达式右边逻辑处理
                            if (v_compare_bit == 3):
                                v_right_item = v_token_sub.value;
                                # 括号内部，可以递归解析
                                if (isinstance(v_token_sub, sqlparse.sql.Parenthesis)):
                                    v_subsql_text = cgf_subsql_cutoff(v_token_sub.value);
                                    if (v_subsql_text != "nosub"):
                                        # 子查询，进行重新解析，如果确实是select sql，递归进行处理
                                        v_sql_parser = sqlparse.parse(v_subsql_text);
                                        if (cgf_is_subselect(v_token_list = v_sql_parser[0]) == True):
                                            cgf_selsql_dissect(v_token_list = v_sql_parser[0],v_level = v_level + 1,
                                                v_table_list = v_table_list,v_compare_list = v_compare_list,v_where_list = v_where_list);

                            # 标志位递增
                            v_compare_bit = v_compare_bit + 1;

                            # 记录v_compare_list
                            if (v_compare_bit == 4):
                                # 入列表
                                v_compare_list.append([v_level,v_token,v_left_item,v_compare_item,v_right_item]);

                                # 标志位重置
                                v_compare_bit = 1;

                                # 数值重置
                                v_left_item = None;
                                v_right_item = None;
                                v_compare_item = None;

    # where list录入
    v_where_list.append([v_level,v_seen_where,v_where_value]);

    return v_table_list,v_compare_list,v_where_list;

v_sql = """select a.k,b.c 
from my_a as a,my_b as b 
inner join my_c as c on b.id = c.id(+) and b.kk = c.kk where a.id = 
(select id from my_b where id = a.id)"""


v_parser = sqlparse.parse(v_sql);
v_table_list = [];
v_compare_list = [];
v_where_list = [];
cgf_selsql_dissect(v_parser[0],0,v_table_list,v_compare_list,v_where_list);
v = sorted(v_table_list, key=lambda x: x[0]);
w = sorted(v_compare_list, key=lambda x: x[0]);
z = sorted(v_where_list, key=lambda x: x[0]);

for x in v:
    print (x[0],x[2],x[3]);

for x in w:
    print (x[0],x[2],x[3],x[4]);

for x in z:
    print (x[0],x[1],x[2]);

v_sql = "select '(+)' as b,'like '%' from a,b where a .id(+ ) = b.id and a.momo ";
xx = cgf_sql_exist_outjoin(cgf_sql_simpler(v_sql));
yy = cgf_sql_exist_leftlike(cgf_sql_simpler(v_sql));
print xx;
print yy;




        

        
