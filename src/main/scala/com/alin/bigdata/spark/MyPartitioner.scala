package com.alin.bigdata.spark

import org.apache.spark.Partitioner

class MyPartitioner(numPartition : Int) extends Partitioner{
    override def numPartitions: Int = numPartition

    override def getPartition(key: Any): Int = {
        key match {
            case "上半年" => 0
            case _ => 1
        }

    }
}
