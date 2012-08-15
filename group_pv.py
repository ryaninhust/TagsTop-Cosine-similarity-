#-*- coding:utf-8 -*-
from  dpark import DparkContext
import re
import os
import sys

CTX=DparkContext()
def init(pv_file_files):
    '''
    初始化RDD
    '''
    raw_rdd = CTX.textFile(pv_file_files[0], splitSize = 64<<20)
    for file_path in pv_file_files[1:]:
        raw_rdd = raw_rdd.union(CTX.textFile(file_path, splitSize = 64<<20))
    return raw_rdd


def get_topic_group_dict(topic_group_files):
    '''
    获取topic_group 映射rdd
    '''
    print "----in----"
    return init(topic_group_files).\
    map(lambda line: (int(line.strip().split(" ")[0]),
        int(line.strip().split(" ")[1])))
    

def filter_url(key_value):
    '''
    提取url文件
    '''
    return int(key_value[0])<300 and re.match('topic/(\d+)',
            key_value[1][21:]) is not None


def get_topic_id(key_value):
    '''
    获取topic_id
    '''
    result = re.match('topic/(\d+)', key_value[1][21:]).group().split('/')[1]
    return result


def format_rdd(raw_rdd):
    simple_rdd = raw_rdd.map(lambda line: (line.split('\t')[12], line.split('\t')[17]))    
    result_rdd = simple_rdd.filter(filter_url)\
    .map(get_topic_id).map(lambda x: (int(x), 1)).reduceByKey(lambda x, y : x + y)
    print "====ok!===="
    return result_rdd
    

def join_topic_group(formatted_rdd, topic_group_dict):
    return  formatted_rdd.join(topic_group_dict)


def group_count_map(data):
    if data[1][0] > data[1][1]:
        return (data[1][0], data[1][1])
    else:
        return (data[1][1], data[1][0])


def join_improve(formatted_rdd, _dict):
    '''
    改进后的join操作
    '''
    return formatted_rdd.union(_dict).groupByKey(numSplits = 20)\
            .filter(lambda x: len(x[1]) > 1)


def get_group_tags(group_tags_files):
    '''
    获取 group_tags rdd
    '''
    raw_rdd = init(group_tags_files)
    return raw_rdd\
    .map(lambda line: (int(line.split(':')[0]), line.split(':')[1]))


def get_group_count(pv_files, topic_group_files):
    '''
    获取group_count rdd
    '''
    return join_improve(format_rdd(init(pv_files)),
            get_topic_group_dict(topic_group_files))\
    .map(group_count_map).reduceByKey(lambda x, y: x + y)


def group_count_tags(group_count, group_tags):
    '''
    获取group_count_tags rdd
    '''
    return join_improve(group_count, group_tags)


def fix_tags_count(line):
    '''
    调整tags 和count 顺序
    '''
    try:
        int(line[1][1])
    except:
        return ('%s:%s %s') % (line[0], line[1][0], line[1][1])
    else:
        return ('%s:%s %s') % (line[0], line[1][1], line[1][0])


def get_dict_files(dict_path):
    '''
    获取文件集
    '''
    return [('%s/%s') % (dict_path, i) for i in os.listdir(dict_path)]


def load_relative_dict(dict_path):
    return init(get_dict_files(dict_path))\
    .map(lambda x: (x.strip().split(':')[0],
        x.strip().split(':')[1].split(',')))


def get_relative_words(relative_dict, keyword):
    list = []
    data = relative_dict.groupByKey().lookup(keyword)
    if data:
        list = data[0]
    list.append(keyword)
    return list
  

def compute_keyword_pv(group_count_tags, keyword):
    '''
    计算关键字pv
    '''
    pv = group_count_tags.filter(lambda line: line.split(':')[1].split(' ')[1].find(keyword) > -1)\
            .map(lambda line: line.split(':')[1].split(' ')[0])\
            .reduce(lambda x, y: int(x) + int(y))
    if pv:
        return pv
    else:
        return 0


def generate_duration_file(pv_dict_path, topic_group_dict, group_tags_dict, save_path):
    '''
    生成周期tags-count文件,具体的时间范围由文件集来定
    '''
    group_count = get_group_count(get_dict_files(pv_dict_path), get_dict_files(topic_group_dict))
    group_tags = get_group_tags(get_dict_files(group_tags_dict))
    join_improve(group_count, group_tags).map(fix_tags_count).saveAsTextFile(save_path)
    

def compute_keywords_pv(keyword, 
        relative_path = '/home/ybw_intern/tag_top/threshold_test',
        pv_files = '/home/ybw_intern/tag_top/final_rdd'):
    relative_dict = load_relative_dict(relative_path)
    pv_data = init(get_dict_files('/home/ybw_intern/tag_top/final_rdd'))
    relative_list = get_relative_words(relative_dict, keyword)
    pv_result = 0
    for word in relative_list:
        pv_result += int(compute_keyword_pv(pv_data,word))
    return pv_result

    
    


if __name__ == "__main__":
    print compute_keywords_pv(sys.argv[1])
