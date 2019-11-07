# coding=utf-8

import sys
import reload


reload(sys)
sys.setdefaultencoding('utf-8')
import jieba.analyse
import jieba

for line in sys.stdin:
    cols = line.strip().split('\t')
    id2 = cols[0]
    words_list = jieba.analyse.extract_tags(cols[1], topK=10, withWeight=True)
    for (word, score) in words_list:
        print('%s\t%s\t%s' % (id2, word, score))
        # 切完词之后，带权重。
        # print ','.join([str(x) + "_" + str(y) for (x, y) in words_list])


# select word,collect_list(concat_ws('_',id,score)) as id_score_list from(
# select tramsform(id,desc) using 'python jieba_score_udf.py' as (id,word,score)
# from musics sort by score desc
# ) t group by word limit 10 ;
