package com.atguigu.bigdata.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File_Par {

    def main(args: Array[String]): Unit = {

        // TODO 准备环境
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)

        // TODO 创建RDD

        //val rdd = sc.textFile("datas/1.txt")
        // textFile可以将文件作为数据处理的数据源，可以默认也可以设定最少分区数目。
        // 默认分区数算法：math.min(defaultParallelism, 2)

        val rdd = sc.textFile("datas/1.txt", 2)
        // 如果不想使用默认的分区数量，可以通过第二个参数指定最少分区数
        // Spark读取文件，底层其实使用的就是Hadoop的读取方式
        // 分区数量的计算方式：
        //    totalSize = 7
        //    goalSize =  7 / 2 (minPartitions) = 3（byte）
        //    7 / 3 = 2...1 (Hadoop 解释分区的 1.1 概念 ： 剩余Size/partitionSize >10% ，产生新分区) + 1 = 3(分区)



        rdd.saveAsTextFile("output")


        // TODO 关闭环境
        sc.stop()
    }
}
