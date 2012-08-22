#-*- coding:utf-8 -*-
from group_pv import init, get_dict_files, CTX
import sys

def load_tags(dict_path):
    '''
    读取tags文件
    '''
    return init(get_dict_files(dict_path))


#level 0
def _simple_divide(line):
    '''
    [tag1,tag2]
    '''
    result = []
    for separator in [' ', '，', '。',
            '\\', ',', ';', '　', '、', '；','|']:
        tags = line.strip().split(':')[1].split(separator)
        if len(tags) > len(result):
            result = tags
    return result


def _divide_zip(line):
    '''
    (tag1,tag2)->[(tag1,1),(tag2,1)]
    '''
    result=[]
    for separator in [' ', '，', '。',
            '\\', ',', ';', '　', '、', '；']:
        tags = line.strip().split(':')[1].split(separator)
        if len(tags) > len(result):
            result = tags
    return zip(result, [1] * len(result))


#level 0
def _simple_divide2(line):
    return ((line.strip().split(':'))[0],
            int(line.strip().split(':')[1]))


def _format_tags(line):
    return (line.strip().split(':')[0],_simple_divide(line))


#level 0
def extract_tags(group_tags, extract_func):
    return group_tags.map(extract_func)

#字数统计所需
tags_data2 = extract_tags(load_tags('/home/ybw_intern/tag_top/_group_tags'),
        _divide_zip)

#基本统计所需
tags_data = extract_tags(load_tags('/home/ybw_intern/tag_top/_group_tags'),
        _simple_divide)

#格式化小组标签所需
group_tag = extract_tags(load_tags('/home/ybw_intern/tag_top/_group_tags'),
        _format_tags)

#字数统计结果
tags_count = extract_tags(load_tags('/home/ybw_intern/tag_top/_word_count'),
        _simple_divide2).filter(lambda line:int(line[1])>1)

def _string(line):
    tag_list_str = ''
    for tag in line[1]:
        tag_list_str = tag_list_str+'%s,' % tag
    return '%s:%s' % (line[0], tag_list_str[:-1])

def save_formated_tags(rdd = group_tag, savepath = 'test1'):
    rdd.map(_string).saveAsTextFile(savepath)
    
#level 0
def _merge(line1, line2):
    line1.extend(line2)
    return line1


#level0
def merge_tags_rdd(extracted_tags):
    return CTX.parallelize(extracted_tags.reduce(_merge), numSlices = 20)


def _union(rdd1, rdd2):
    return rdd1.union(rdd2)


def union_tem(line):
    return (tag, set(line)|set([(tag, 1)]) == set(line))


def count_tag(tag_tuple):
    global tag
    tag = tag_tuple[0]
    return (tag_tuple[0], tags_data.map(union_tem)\
            .reduceByKey(lambda x, y: x + y).collect()[0][1])



def handle_tag_count(extracted_tags):
   # returnt extracted_tags.map(lambda line: _test(line))
    return extracted_tags\
    .map(lambda line: [count_tag(tags_tuple) for tags_tuple in line])


#level 0
def reduce_merged_rdd(tags_rdd):
    #TODO need a persistence store
    return tags_rdd.reduceByKey(lambda x, y: x + y)


def _extract_real_count(line):
    list_set = line[1][0]
    list_set.extend(line[1][1])
    if list_set[0] >= list_set[-1]:
        real_count = list_set[0]
    else:
        real_count = list_set[-1]
    return (line[0], real_count)


def group_merged_with_reduced(merged_rdd, reduced_rdd):
    return merged_rdd.groupWith(reduced_rdd).map(_extract_real_count)


def _assign(line):
    line_rdd = CTX.parallelize(line)
    result = g_reduced_rdd.union(line_rdd).groupByKey()\
    .filter(lambda x:len(x[1])>1).map(lambda x :(x[0], max(x[1])))
    return result.collect()


def set_real_count(reduced_rdd, extrated_rdd = tags_data):
#TODO  persistence store    
    global g_reduced_rdd
    g_reduced_rdd = reduced_rdd
    return  extrated_rdd.map(_assign)


def _format_count(line):
    return '%s:%d' % line


def standard_word_count(extrated_data = tags_data2, save_path = '_word_count'):
    return reduce_merged_rdd(merge_tags_rdd(extrated_data))\
    .map(_format_count).saveAsTextFile(save_path)


def _cartesian_plus(line):
    return [((i, j), 1) for i in line for j in line if i is not j]


def _zip(line):
    return zip(line, [1] * len(line))


def _pick_tuple(_list):
    return [tup for tup in _list if isinstance(tup, tuple)]


def _pick_int(_list):
    try:
        int_list = [tup for tup in _list if isinstance(tup, int)][0]
    except IndexError:
        return 1
    else:
        return int_list



#level 0
def standard_tuple_count(extrated_data):
    return reduce_merged_rdd(merge_tags_rdd(extrated_data
        .map(_cartesian_plus))).filter(lambda line:line[1] > 2)


def _reposition(line):
    return zip([(line[0], _pick_int(line[1]))] * len(line[1]), _pick_tuple(line[1]))


def _compute_cos(line):
    key = line[0]
    key_count = _pick_int(line[1])    
    sim_word_list = [(word[0][0], float(word[1]) / (float(word[0][1])*float(key_count))) 
            for word in _pick_tuple(line[1])]
    return (key, sim_word_list)
    

def _format_cos(line):
    key = line[0]
    format_string = '%s:' % key
    for word in line[1]:
        format_string += '%s,' % word[0]
    return format_string[:-1]


#level 0
def Combine(tuple_count, word_count):
    tuple_count = tuple_count.map(lambda line: (line[0][0],
        (line[0][1], line[1])))
    rdd = tuple_count.union(word_count).groupByKey()\
    .filter(lambda line: len(line[1]) > 1)
    new_rdd = merge_tags_rdd(rdd.map(_reposition))
    return new_rdd.map(lambda line: (line[1][0], (line[0],
        line[1][1]))).union(word_count)\
        .groupByKey().filter(lambda line: len(line[1]) > 1).map(_compute_cos)
            

def _threshold(line):
    return (line[0], [_alter for _alter in line[1] if _alter[1] > threhold])


def estimate_threshold(combined_rd, t,save_path):
    global threhold
    threhold = t
    return  combined_rd.map(_threshold)\
    .filter(lambda x: x[1]).map(_format_cos).saveAsTextFile(save_path)
    

def output_relative_words(save_path, threshold = 0.005):
    standard_word_count(tags_data2)
    combined_rd = Combine(standard_tuple_count(tags_data) ,tags_count)
    estimate_threshold(combined_rd, threshold, save_path)

    
if __name__ == '__main__':
    output_relative_words(save_path = 'threshold_test')
    save_formated_tags()
    print ' ok!'
    

