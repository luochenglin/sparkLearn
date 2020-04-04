package com.alin.bigdata.spark

import com.alin.bigdata.util.SparkContextUtil
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD

object KeyValueRDD {
    private val sc: SparkContext = SparkContextUtil.createSparkContext()
    private val intRDD: RDD[Int] = sc.makeRDD(1 to 3)
    private val strRDD: RDD[String] = sc.makeRDD(List("a", "b", "c"))
    private val pairRDD: RDD[(String, Int)] = strRDD.zip(intRDD)

    def main(args: Array[String]): Unit = {

        //        pairRDD.reduceByKey(new HashPartitioner(2),_+_).saveAsTextFile("out")
        //        val i: Int = intRDD.reduce(_+_)
        //        print(i)

        //        val seqRDD: RDD[Int] = sc.makeRDD(1 to 10, 2)
        //        seqRDD.glom().collect().foreach(println(x=>x.mkString))
        //        val str: Int = seqRDD.aggregate(0)((_ + _), (-_ - _))
        //        println(str)
        val seqRDD: RDD[(String, Int)] = sc.makeRDD(List(("上半年",1),("上半年",4),("下半年",8)), 2).partitionBy(new MyPartitioner(2))
        seqRDD.glom().collect().foreach(x=>println(x.mkString))
        val str: RDD[(String, Int)] = seqRDD.aggregateByKey(0)((_+_), (_-_))
        println(str.collect().mkString)
        println(str.map(_._2).map(("a", _)).reduceByKey(_ - _).collect().mkString)


    }

}
