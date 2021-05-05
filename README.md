# bigdata-movie-recommend
# 电影推荐分析系统

本次项目基于Python爬虫与Movielens数据集作为数据来源，获取CSV格式的数据，使用Hadoop HDFS作为数据的分布式存储平台，使用MongoDB作为数据结构化、规范化的处理并对运算结果进行存储，使用Spark暴露对外SQL接口，使用Spark进行数据处理运算，执行核心算法，使用SCALA语言编程，调用Spark MLlib等代码库进行机器学习算法执行，得到推荐结果。使用VUE前端框架与Flask后端框架进行结果可视化平台搭建。

## 过程简述

### 基于MovenLens 数据集

- 收集MovieLens数据集，包含16万个电影，2400万条评分，67万条评价标签，将csv文件上传到完全分布式HDFS文件系统
- scala、spark读取HDFS文件，整理导入MongoDB数据库
- MongoDB中加载数据，利用sparkRdd统计热门电影、高分电影，统计分年月、分类别的热门、高分电影数据
- 基于ALS协同过滤算法，得到用户电影推荐和相似电影推荐
- 通过 TF-IDF 算法对标签的权重进行调，计算电影的内容特征向量，实现基于内容的电影推荐
- 使用python、pymongo和matplotlib，读取MongoDB数据并进行可视化
- 使用python实现SVD奇异值分解进行电影推荐
- Python flask构建后端数据服务，vue构建前端页面，交互式展示数据。

### 基于豆瓣数据集

- 破解滑动验证块，爬虫爬取豆瓣电影短评数据
- 利用Jieba分词库对电影短评分词，利用snowNLP对文本情感分析
- 利用wordcloud制作词云

## 部分结果展示

- 日历图和热力图的绘制，体现电影在一年中的热度情况。以下选取了某部较为热门的电影，在1996年上映时候的每天的热度情况。

  ![rili](https://frozenwhale.oss-cn-beijing.aliyuncs.com/img/rili.png)

- 用户电影推荐

  ![yonghu](https://frozenwhale.oss-cn-beijing.aliyuncs.com/img/yonghu.png)



