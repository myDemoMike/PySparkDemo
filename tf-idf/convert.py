#!/usr/bin/python

import os
import sys
import gzip

test_dir = sys.argv[1]


def get_file_handler(f):
    file_in = open(f, 'r')
    return file_in


index = 0
for fd in os.listdir(test_dir):

    txt_list = []

    file_fd = get_file_handler(test_dir + '/' + fd)
    for line in file_fd:
        txt_list.append(line.strip())

    print('\t'.join([str(index), ' '.join(txt_list)]))

    index += 1

# python convert.py input_tfidf_dir/ > 1.data
