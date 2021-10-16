package com.atguigu.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_Req2_HotCategoryTop10SessionAnalysis {

    def main(args: Array[String]): Unit = {

        // TODO : Top10热门品类
        val sparConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
        val sc = new SparkContext(sparConf)

        val actionRDD = sc.textFile("datas/user_visit_action.txt")
        actionRDD.cache()
        val top10Ids: Array[String] = top10Category(actionRDD)

        // 1. 过滤原始数据,保留点击和前10品类ID
        val filterActionRDD = actionRDD.filter(
            action => {
                val datas = action.split("_")
                if ( datas(6) != "-1" ) {
                    top10Ids.contains(datas(6))
                } else {
                    false
                }
            }
        )

        // 2. 根据品类ID和sessionid进行点击量的统计
        val reduceRDD: RDD[((String, String), Int)] = filterActionRDD.map(
            action => {
                val datas = action.split("_")
                ((datas(6), datas(2)), 1)
            }
        ).reduceByKey(_ + _)
        //      System.out.println("reduceRDD values:")
        //      reduceRDD.foreach(println)
        /*
        ((17,4fbcd403-8f9a-4d7a-93b7-1c4becea9a1f),1)
        ((17,019a6a75-a775-4ac9-bd8f-4f64a0cffd7a),2)
        ((7,746f01c0-bd37-4a66-8e9d-4d96ccb74104),3)
        ((9,d96eb2b6-e268-42cf-80fb-b40978ee69d9),2)
        ((7,f1fcba86-9d67-4742-a92d-cff59d8ef037),1)
        */
        // 3. 将统计的结果进行结构的转换
        //  (（ 品类ID，sessionId ）,sum) => ( 品类ID，（sessionId, sum） )
        val mapRDD = reduceRDD.map{
            case ( (cid, sid), sum ) => {
                ( cid, (sid, sum) )
            }
        }

        // 4. 相同的品类进行分组
        val groupRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD.groupByKey()

        // 5. 将分组后的数据进行点击量的排序，取前10名
        val resultRDD = groupRDD.mapValues(
            iter => {
                iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(10)
            }
        )

        resultRDD.collect().foreach(println)


        sc.stop()
    }
    def top10Category(actionRDD:RDD[String]) = {
        val flatRDD: RDD[(String, (Int, Int, Int))] = actionRDD.flatMap(
            action => {
                val datas = action.split("_")
                if (datas(6) != "-1") {
                    // 点击的场合
                    List((datas(6), (1, 0, 0)))
                } else if (datas(8) != "null") {
                    // 下单的场合
                    val ids = datas(8).split(",")
                    ids.map(id => (id, (0, 1, 0)))
                } else if (datas(10) != "null") {
                    // 支付的场合
                    val ids = datas(10).split(",")
                    ids.map(id => (id, (0, 0, 1)))
                } else {
                    Nil
                }
            }
        )

        val analysisRDD = flatRDD.reduceByKey(
            (t1, t2) => {
                ( t1._1+t2._1, t1._2 + t2._2, t1._3 + t2._3 )
            }
        )

        analysisRDD.sortBy(_._2, false).take(10).map(_._1)
    }
}
