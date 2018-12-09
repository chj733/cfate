#!/usr/bin/env python
#-*- coding:utf8 -*-
"""
 PythonFile: 
 Writer: chun.luo@woqutech.com
 Function:经典排序算法
 CreateTime: 2018-11-20 23:00:41
"""

import os;
import sys;
import math;
import random;
import datetime;
import numpy as np;

#计数器
lv_loop_counter = 0;

# 交换数组中的元素,v1，v2代表下标
def gf_list_switch(v_list,v1,v2):
	if (v1 != v2 and v1 >= 0 and v1 < len(v_list) and v2 >= 0 and v2 < len(v_list)):
		v_tmp = v_list[v1];
		v_list[v1] = v_list[v2];
		v_list[v2] = v_tmp;

# 检查数组是否有序排列，正序为p，逆序为n
def gf_check_array_order(v_my_datalist = None,v_list_order_type = "p",v_list_len = None):
	if (len(v_my_datalist) <= 1):
		return True;

	if (len(v_my_datalist) <> v_list_len):
		return False;

	if (v_list_order_type == "p"):
		lv_last_value = v_my_datalist[0];
		for i in xrange(1,len(v_my_datalist)):
			if (lv_last_value <= v_my_datalist[i]):
				lv_last_value = v_my_datalist[i];
			else:
				return False;
				break;

		return True;

	if (v_list_order_type == "n"):
		lv_last_value = v_my_datalist[0];
		for i in xrange(1,len(v_my_datalist)):
			if (lv_last_value >= v_my_datalist[i]):
				lv_last_value = v_my_datalist[i];
			else:
				return False;
				break;

		return True;

def gf_help():
	v_helptext = "**********************************************************************************\n" + \
				 "****                           q_mine_sort usage                              ****\n" + \
				 "****   python q_mine_sort.py -bubble -100 -20: bubble sort 100 items in(1..20)****\n" + \
				 "****   python q_mine_sort.py -select -100 -20: select sort 100 items in(1..20)****\n" + \
				 "****   python q_mine_sort.py -insert -100 -20: insert sort 100 items in(1..20)****\n" + \
				 "****   python q_mine_sort.py -quick  -100 -20: quick sort 100 items in(1..20) ****\n" + \
				 "****   python q_mine_sort.py -bucket -100 -20: bucket sort 100 items in(1..20)****\n" + \
				 "****   python q_mine_sort.py -radix  -100 -10: radix sort 100 items in(1..20) ****\n" + \
				 "**********************************************************************************""";
	print v_helptext;
	os._exit(0);
			
# 插入排序
def gf_insert_sort(v_my_datalist,v_list_len):
	if (v_list_len > 1):
		global lv_loop_counter;
		#针对每个元素，做插排序操作
		for i in xrange(v_list_len):
			lv_now_value = v_my_datalist[i]
			#当前元素之前的数据，进行位置查找，确定合适的位置
			for j in xrange(i - 1,-1,-1):
				# 计数器记数
				lv_loop_counter = lv_loop_counter + 1;
				if (j >= 0 and j < v_list_len):
					# 如果j号元素 <= 当前值，表示已经到达正确位置，因为之前的元素已经是排序好的
					if (lv_now_value >= v_my_datalist[j]):
						break;
					else:
						# 交换元素
						gf_list_switch(v_my_datalist,j,j+1);

	return v_my_datalist;

# 冒泡排序
def gf_bubble_sort(v_my_datalist,v_list_len):
	if (v_list_len > 1):
		global lv_loop_counter;
		#原则上需要针对每个元素，做冒泡操作
		for i in xrange(v_list_len):
			v_is_continue = False;
			# 每完成一次冒泡，剩余元素减少，采用len － i进行范围缩小，但是注意边界
			for j in xrange(0,v_list_len - i + 1):
				if (j < v_list_len - 1):
					# 比大小，进行冒泡
					if (v_my_datalist[j] > v_my_datalist[j + 1]):
						gf_list_switch(v_my_datalist,j,j + 1);
						v_is_continue = True;

				#计数器记数
				lv_loop_counter = lv_loop_counter + 1;

			# 如果一次冒泡下来，没有任何元素交换，表示数组已经有序，无须再循环后面元素
			if (v_is_continue == False):
				break;		

	return v_my_datalist;

# 选择排序
def gf_select_sort(v_my_datalist,v_list_len):
	if (v_list_len > 1):
		global lv_loop_counter;
		# n个元素，进行n次选择排序
		for i in xrange(v_list_len):
			# 除去已经处理的元素，剩余元素，每次循环，确定一个当前最小值，直到所有元素都操作过
			for j in xrange(i + 1,v_list_len):
				if(j <= v_list_len - 1):
					if(v_my_datalist[j] < v_my_datalist[i]):
						gf_list_switch(v_my_datalist,i,j);

				#运算次数统计
				lv_loop_counter = lv_loop_counter + 1;

	return v_my_datalist;

# 快速排序
def gf_quick_sort(v_my_data_list,v_list_len):
	global lv_loop_counter;
	if (v_list_len > 1):
		# 栈，用于记录分区边界，头号边界即0-len - 1
		# 每次分区，都会将产生的新边界区间[new_start,new_end]加入栈
		# pop出栈，进行分区内的运算，直到栈尽为止
		# 栈实现了递归，又避免了递归迭代溢出的问题
		v_my_stack = [];
		v_my_stack.append([0,v_list_len - 1]);
		while (v_my_stack):
			# 出栈，取值
			v_start_end_positition = v_my_stack.pop();
			# 每次的分区选择中间元素
			v_lucky_position = ((v_start_end_positition[1] - v_start_end_positition[0]) / 2) + v_start_end_positition[0];
			v_lucky_value = v_my_data_list[v_lucky_position];
			# 起点坐标
			v_where_start = v_start_end_positition[0];
			# lucky value不参与分区内部的运算
			gf_list_switch(v_my_data_list,v_lucky_position,v_start_end_positition[1]);
			lv_loop_counter = lv_loop_counter + 1;

			v_switch_count = 0;
			v_little_count = 0;
			v_is_continue = True;

			while (v_is_continue):
				# 不包含最后元素，即luckvalue，每次循环，从v_where_start开始
				for i in xrange(v_where_start,v_start_end_positition[1]):
					lv_loop_counter = lv_loop_counter + 1;
					v_this_loop_continue = True;
					# v_where_start之前的元素，都是比luckvalue小的，换言之，都是下次循环不再需要处理的
					if (v_lucky_value >= v_my_data_list[i]):
						v_where_start = v_where_start + 1;
						# 小元素的记数
						v_little_count = v_little_count + 1;
					else:
						# 大于luckvalue的元素，依次堆到数组末尾，注意按照倒序逐个排列，v_switch_count很微妙
						gf_list_switch(v_my_data_list,i,v_start_end_positition[1] - 1 - v_switch_count);
						v_switch_count = v_switch_count + 1;
						# 由于不知道switch过来的值大小，只能退出循环，再从v_where_start开始，重新循环判断
						v_this_loop_continue = False;

					# 如果所有元素已经处理，要么大，要么小，就表示该分区数据运算已经完成，新的分区边界已经产生
					if (v_little_count + v_switch_count >= v_start_end_positition[1] - v_start_end_positition[0]):
						v_is_continue = False;
						break;
					
					# 退出当前的for循环
					if (v_this_loop_continue == False):
						break;

			# luckvalue回到自己的正确位置，即在这个分区里面，你按照大小排列，是第几位，v_where_start
			gf_list_switch(v_my_data_list,v_where_start,v_start_end_positition[1]);
			lv_loop_counter = lv_loop_counter + 1;
			# 小的分区，边界入栈，luckvalue已经不需要处理
			if (v_where_start - 1 > v_start_end_positition[0]):
				v_my_stack.append([v_start_end_positition[0],v_where_start - 1]);
			
			# 大的分区，边界入栈，luckvalue已经不需要处理
			if (v_where_start + 1 < v_start_end_positition[1]):
				v_my_stack.append([v_where_start + 1,v_start_end_positition[1]]);

	return v_my_data_list;
			
# 线性排序－桶记数排序法
def gf_bucket_sort(v_my_data_list,v_list_len,v_bucket_count):
	global lv_loop_counter;
	v_bucket_list = [];
	v_my_sorted_list = [];
	if (v_list_len > 1 and v_bucket_count > 1):
		# 每个桶，代表一个唯一值，排序数据所有的数值，都在v_bucket_list的范围内
		# 根据唯一值数量，构建一个空桶数组，注意，桶的下标，即该桶的数值value
		for i in xrange(v_bucket_count):
			v_bucket_list.append(0);
			lv_loop_counter = lv_loop_counter + 1;

		# 排序数组的每个值，都浏览一次，对桶值进行记数
		# 浏览完成，表示每个桶的记数完成，我们获取了每个唯一值的记数
		for j in xrange(v_list_len):
			try:
				v_bucket_list[v_my_data_list[j] - 1] = v_bucket_list[v_my_data_list[j] - 1] + 1;
				lv_loop_counter = lv_loop_counter + 1;
			except Exception,e:
				pass;

		# 对每个桶，计算他的排序数组的位置
		# 桶的位置实际就是前面所有桶的计数总和＋当前桶的计数
		v_now_bucket_value = v_bucket_list[0];
		v_bucket_list[0] = 0;
		for x in xrange(1,v_bucket_count):
			v_temp = v_bucket_list[x];
			v_bucket_list[x] = v_now_bucket_value;
			v_now_bucket_value = v_now_bucket_value + v_temp;
			lv_loop_counter = lv_loop_counter + 1;
		
		# 新建一个数组，用来存放排序后的结果，先全部为零，数组长度和排序数组一致
		for y in xrange(v_list_len):
			v_my_sorted_list.append(0);
			lv_loop_counter = lv_loop_counter + 1;

		# 扫描排序数组，每个值，寻找到对应的桶，获取他的position，然后将桶的计数减1，这部分很微妙
		for z in xrange(v_list_len):
			v_bucket_list_position = v_my_data_list[z] - 1;
			v_sorted_position = v_bucket_list[v_bucket_list_position];
			v_my_sorted_list[v_sorted_position] = v_my_data_list[z];
			v_bucket_list[v_bucket_list_position] = v_bucket_list[v_bucket_list_position] + 1;
			lv_loop_counter = lv_loop_counter + 1;
			
			
	return v_my_sorted_list;
		
def gf_radix_phone_sort(v_my_datalist,v_list_len,v_row_range = 10):
	global lv_loop_counter;
	if (v_list_len <= 1):
		return v_my_datalist;

	v_bucket_list = [];
	v_my_sorted_list = [];

	for i in xrange(v_row_range):
		v_bucket_list.append(0);
		lv_loop_counter = lv_loop_counter + 1;
	
	for y in xrange(v_list_len):
		v_my_sorted_list.append(0);
		lv_loop_counter = lv_loop_counter + 1;

	for i in xrange(11):
		for j in xrange(v_list_len):
			v_now_single_phonenum = int(str(v_my_datalist[j])[10 - i]);
			v_bucket_list[v_now_single_phonenum] = v_bucket_list[v_now_single_phonenum] + 1;
			lv_loop_counter = lv_loop_counter + 1;

		
		v_now_bucket_value = v_bucket_list[0];
		v_bucket_list[0] = 0;
		for k in xrange(1,v_row_range):
			v_temp = v_bucket_list[k];
			v_bucket_list[k] = v_now_bucket_value;
			v_now_bucket_value = v_now_bucket_value + v_temp;
			lv_loop_counter = lv_loop_counter + 1;


		for x in xrange(v_list_len):
			v_bucket_list_position = int(str(v_my_datalist[x])[10 - i]);
			v_sorted_position = v_bucket_list[v_bucket_list_position];
			v_my_sorted_list[v_sorted_position] = v_my_datalist[x];
			v_bucket_list[v_bucket_list_position] = v_bucket_list[v_bucket_list_position] + 1;
			lv_loop_counter = lv_loop_counter + 1;

		v_my_datalist[:] = v_my_sorted_list;
		
		for m in xrange(len(v_bucket_list)):
			v_bucket_list[m] = 0;
			lv_loop_counter = lv_loop_counter + 1;

	return v_my_datalist;
	

if __name__ == "__main__":
	v_sort_type = "quick";
	v_row_number = 100;
	v_row_range = 100;
	v_pname = "";
	v_my_sorted_list = None;
	v_my_list_len = 0;
	
	# 参数输入处理，-sort_type -row_num -row_range
	try:
		if (len(sys.argv) > 0):
			v_pname,v_sort_type,v_row_number,v_row_range = sys.argv[0],sys.argv[1].split("-")[1],\
			int(sys.argv[2].split("-")[1]),int(sys.argv[3].split("-")[1]);
			
	except Exception,e:
		gf_help();
		os._exit(0);


	#python的随机数组生成，这语法真优雅
	v_my_list = np.random.randint(1,v_row_range,v_row_number);
	v_my_list_len = len(v_my_list);
	#print v_my_list;

	# 计时	
	v_time_start = datetime.datetime.now();

	# 排序计算
	# 快速分区排序
	if (v_sort_type == "quick"):
		print v_my_list;
		v_my_sorted_list = gf_quick_sort(v_my_list,v_my_list_len);
	# 冒泡排序
	elif (v_sort_type == "bubble"):
		print v_my_list;
		v_my_sorted_list = gf_bubble_sort(v_my_list,v_my_list_len);
	# 插入排序
	elif (v_sort_type == "insert"):
		print v_my_list;
		v_my_sorted_list = gf_insert_sort(v_my_list,v_my_list_len);
	# 选择排序
	elif (v_sort_type == "select"):
		print v_my_list;
		v_my_sorted_list = gf_select_sort(v_my_list,v_my_list_len);
	# 桶计数排序
	elif (v_sort_type == "bucket"):
		print v_my_list;
		v_my_sorted_list = gf_bucket_sort(v_my_list,v_my_list_len,v_row_range);
	# 基数排序，用于手机号排大小
	elif (v_sort_type == "radix"):
		v_my_list = np.random.randint(10000000000,19999999999,v_row_number);
		print v_my_list;
		v_my_sorted_list = gf_radix_phone_sort(v_my_list,v_my_list_len,v_row_range);
	else:
		print v_my_list;
		v_my_sorted_list = gf_quick_sort(v_my_list,v_my_list_len);

	v_time_end = datetime.datetime.now();

	v_take_seconds = (v_time_end - v_time_start).seconds;

	print v_my_sorted_list;
	
	# 检查顺序是否正确
	v_right = gf_check_array_order(v_my_sorted_list,"p",v_my_list_len);

	# 统计结果输出
	print "Check result : " + str(v_right) + ", Count off(" + v_sort_type + ") : " + str(lv_loop_counter) + ", Seconds consume : " + str(v_take_seconds);

