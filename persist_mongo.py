from pymongo import Connection
from group_pv import init, get_dict_files
#from word_similarity import _simple_divide, merge_tags_rdd

DB = Connection().tagtop


def save_documents(collection, formated_rdd):
    collection.remove()
    collection.insert(formated_rdd.collect())


def format_rdd(format_func, data_files):
    formated_rdd = init(get_dict_files(data_files))\
            .map(format_func)
    return formated_rdd


def _relative_format(line):
    line = line.replace('.','_')
    return {line.strip().split(':')[0]:
            line.strip().split(':')[1]}


if __name__ == '__main__':
    save_documents(DB.test, format_rdd(_relative_format,
        '/home/ybw_intern/tag_top/threshold_test'))
    print DB.test.find().count()


