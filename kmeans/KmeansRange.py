#-*- coding:utf-8 -*-

import matplotlib.pyplot as plt
import numpy as np
from sklearn.cluster import KMeans
from scipy.spatial.distance import cdist

x = np.array([1, 2, 3, 1, 5, 6, 5, 5, 6, 7, 8, 9, 7, 9])
y = np.array([1, 3, 2, 2, 8, 6, 7, 6, 7, 1, 2, 1, 1, 3])
data = np.array(list(zip(x, y)))

# 肘部法则 求解最佳分类数
# K-Means参数的最优解也是以成本函数最小化为目标
# 成本函数是各个类畸变程度（distortions）之和。每个类的畸变程度等于该类重心与其内部成员位置距离的平方和
aa=[]
K = range(1, 10)
for k in range(1,10):
    kmeans=KMeans(n_clusters=k)
    kmeans.fit(data)
    aa.append(sum(np.min(cdist(data, kmeans.cluster_centers_, 'euclidean'),axis=1))/data.shape[0])
plt.figure()
plt.plot(np.array(K), aa, 'bx-')
plt.show()

'''
#绘制散点图及聚类结果中心点
plt.figure()
plt.axis([0, 10, 0, 10])
plt.grid(True)
plt.plot(x,y,'k.')
kmeans=KMeans(n_clusters=3)
kmeans.fit(data)
plt.plot(kmeans.cluster_centers_[:,0],kmeans.cluster_centers_[:,1],'r.')
plt.show()

'''''