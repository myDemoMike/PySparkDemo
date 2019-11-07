#!/usr/bin/python

import sys

for line in sys.stdin:
    ss = line.strip().split('\t', 1)
    doc_index = ss[0].strip()
    doc_context = ss[1].strip()

    word_list = doc_context.split(' ')

    word_set = set()
    for word in word_list:
        word_set.add(word)

    for word in word_set:
        print('\t'.join([word, "1"]))
