import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
//import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka._
//import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.util.IntParam
import org.apache.spark.SparkContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Row
//import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types._
import java.util.Calendar
import java.util.Date
import java.text.SimpleDateFormat


import scala.collection.mutable

//import org.apache.spark.sql.SparkSession

object annualEventRealTime {

  def main(args: Array[String]) {

    val batchInterval = Milliseconds(10000)

 // Create the context and set the batch size
    val sparkConf = new SparkConf().setAppName("Ignite 3 store inventory")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val hc = new HiveContext(sc)
    import sqlContext.implicits._
    hc.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    import hc._
   
    val ssc = new StreamingContext(sc, batchInterval)
    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092")
    val topics = Set("DCinTrans")
    //case class DcInTrans(item: String, dept: Int, upc: String, item_desc: String, retail_price: Double, str_nbr: Int, qty: Int, date: String, time_stamp: String)
    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,kafkaParams, topics)
    
    val data = stream.map(_._2)
      stream.foreachRDD{ rdd =>
            val lines = rdd.map(_._2)
     //}
   def row(line: List[String]): Row = {
     val timeStamp_1 = new SimpleDateFormat("yyyy-MM-ddTHH:mm:ss.000Z").format(new Date()).toString();
     Row(line(0), line(1).toInt, line(2), line(3), line(4).toDouble,line(5).toInt, line(6).toInt, line(7).toInt, line(8).toInt, line(9), timeStamp_1)
            }
   def dfSchema(columnNames: Seq[String]): StructType =
            //  val current_ts = Seq(Calendar.getInstance().getTime)
              StructType( Seq( StructField(name = "item", dataType = StringType, nullable = false),
                               StructField(name = "dept", dataType = IntegerType, nullable = false),   
                               StructField(name = "upc", dataType = StringType, nullable = false),
                               StructField(name = "item_desc", dataType = StringType, nullable = false),
                               StructField(name = "retail_price", dataType = DoubleType, nullable = false),   
                               StructField(name = "store", dataType = IntegerType, nullable = false),
                               StructField(name = "Inv_qty", dataType = IntegerType, nullable = false),
                               StructField(name = "Sales_qty", dataType = IntegerType, nullable = false),
                               StructField(name = "Rcv_qty", dataType = IntegerType, nullable = false),
                               StructField(name = "Sales_date", dataType = StringType, nullable = false),
                               StructField(name = "timestamp", dataType = StringType, nullable = false)
          )  
  )
          val data1 = lines.map(_.split(",").to[List]).map(row)
          val schema = dfSchema(Seq("item", "dept", "upc", "item_desc", "retail_price", "store", "Inv_qty", "Sales_qty", "Rcv_qty", "Sales_date", "timestamp"))
          val dataFrame = hc.createDataFrame(data1, schema)
          hc.sql("use default")
          //dataFrame.registerTempTable("dc_in_transit")
          dataFrame.write.insertInto("dc_in_transit")
          //dataFrame.write.mode("append").saveAsTable("dc_in_transit")    
    //val joinrdd = inputrdd.join(store_reloc_map)
     //joinrdd.foreach(println)
     //val rdd_with_date = joinrdd.map(d => (d._2._1 + "," + d._2._2.getString(2)+ "," + d._2._2.getString(3)))
     //rdd_with_date.foreach(println)
     //val filter_rdd = rdd_with_date.filter(a => ((a.substring(6,15) >= a.substring(73,82)) && (a.substring(6,15) <= a.substring(84,93))))    
    //filter_rdd.foreach(println)
    // val store_reloc_result = filter_rdd.map(b => (b._2._1))
    // val records = lines.map(y => (y.split("/n")))
    // records.foreach { x => 
    //   if (x.length > 0) {
    //   val store_nbr = x.map(z => z.substring(0,5))
    //   store_nbr.foreach(println)
    //   }
   }
     //val store_nbr = records.foreach.map(x => x.substring(0,5))
     
    ssc.start() 
    ssc.awaitTermination()

  }
}

  