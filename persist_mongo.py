from pymongo import Connection
from group_pv import init, get_dict_files
from word_similarity import _simple_divide, merge_tags_rdd

DB = Connection().tagtop


def save_documents(collection, formated_rdd, splitSize = 2):
    collection.remove()
    for i in range (splitSize):
        collection.insert(formated_rdd.collect()[i::splitSize])


def format_rdd(format_func, data_files):
    formated_rdd = init(get_dict_files(data_files))\
            .map(format_func)
    return formated_rdd


def _relative_format(line):
    line = line.replace('.', '_')
    return {line.strip().split(':')[0]:
            line.strip().split(':')[1]}


def _group_tag_format(line):
    key = line.strip().split(':')[0].replace('.', '_')
    tag_list = _simple_divide(line)
    return {key: tag_list} 


def _pv_format(line):
    key = line.strip().split(':')[0]
    try:
        tags = line.strip().split(':')[1].split(' ')[1]
        pv = line.strip().split(':')[1].split(' ')[0]
    except IndexError:
        return {'error': {'pv': 'error','tag': 'error'}}
    else:
        return {key: {'pv': pv, 'tags': tags}}



if __name__ == '__main__':
    save_documents(DB.relative, format_rdd(_relative_format,
        '/home/ybw_intern/tag_top/threshold_test'))

    save_documents(DB.group_tag, format_rdd(_pv_format,
        '/home/ybw_intern/tag_top/test1'))

    save_documents(DB.pv, format_rdd(_pv_format, 
        '/home/ybw_intern/tag_top/ryan'))


