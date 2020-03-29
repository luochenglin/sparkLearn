package com.alin.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}

object RDD {
    def main(args: Array[String]): Unit = {
        //        RDD数据来源是分布式的，RDD是不可变的，分区的，可并行执行的，在代码里是一个抽象类
        //        创建sparkConf对象,设置spark的运行环境为local模式
        val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
        //        创建spark上下文对象
        val sc = new SparkContext(conf)
        //        val arrayRDD = sc.makeRDD(Array(1,2,3,4,5),2)
        //        传递的分区数是最小分区数，但不一定是这个区分数，取决于Hadoop读取文件的分片规则
        //        (总字节数/设定的最小分区数=每个分片多少个字节)。
        //        txt中回车占两个字节，计算字节数从0开始。word.txt 共 13个字节。
        //        13/2 均分 6，6，1 共3个分区 0号分区存0-6共七个字节，
        //        1号分区存7-12共6个字节，2号分区，因数据读完不存数据。
        //        HadoopRDD: Input split: file:/F:/study/sparkLearn/in/word.txt:0+6
        //        HadoopRDD: Input split: file:/F:/study/sparkLearn/in/word.txt:6+6
        //        HadoopRDD: Input split: file:/F:/study/sparkLearn/in/word.txt:12+1
        //        word.txt:0+6（前面0表示位置0，后面6表示偏移，共七个字节）
        //        word.txt:6+6
        //        word.txt:12+1
        val text = sc.textFile("in", 2)
        text.saveAsTextFile("out")
        //        arrayRDD.saveAsTextFile("out")

    }

}
