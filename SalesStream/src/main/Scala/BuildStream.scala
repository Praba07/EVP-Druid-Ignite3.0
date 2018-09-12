package main.Scala

import java.text.SimpleDateFormat
import java.util.Calendar
import main.Scala.com.cloudurable.kafka.KafkaSalesStream
import scala.util.Random

object BuildStream {
  var rtlPrice: Double = 0
  val msgPart1 = "{" + "\"FO\"" + ":[{" + "\"OrderLine\":[{\"item\": \""
  val msgPart3 = "\",\"upc\": \""
  val msgPart4 = "\",\"item_desc\":\"Item"
  val msgPart5 = "\",\"retail_price\": \""
  val msgPart2 = "\",\"dept\": \""
  val msgPart6 = "\", \"store\": \""
  val msgPart7 = "\",\"Sales_qty\":\""
  val msgPart8 = "\",\"sales_date\":\""
  val msgPart9 = "\"}]}]}"

  // Build the item-dept-UPC-RetailPrice array
  def buildArr: Array[String] = {
    val i: Int = 11000
    val sb = new StringBuilder
    var strgen = ""
    for (i <- 11000 to 12000) {
      var j: Int = i % 100
      if (j <= 0) {
        j = 10
      }
      val k = 1000000000 + i + j
      val l: Int = j % 2
      if (l == 0) {
        rtlPrice = j - k / k + 1
      } else {
        rtlPrice = -j + 100.80
      }
      val p = (i.toDouble / (j.toDouble + 100000)) + rtlPrice.toDouble
      strgen = i + "-" + j + "-" + k + "-" + p.formatted("%.2f")
      sb.append(String.format(strgen + ","))
    }
    return Array(sb.toString())
  }

  def main(args: Array[String]) = {
    buildSales()
  }

  // Strem to generate Sales data
  def buildSales(): Unit = {
    val itemArr = buildArr(0).toString
    val itemArr1 = itemArr.split(',')
    val storelist = Array(100, 200, 300)
    val qty = Array(1, 2, 3, 4, 5, 6, 7, 8, 9)
    var count=0
    do {
      var rnd = new Random().nextInt(itemArr1.length)
      var itemdtl = itemArr1(rnd)
      var rndStr = new Random().nextInt(storelist.length)
      var str = storelist(rndStr)
      var rnq = new Random().nextInt(qty.length)
      var qt = qty(rnq)

      genSales(itemdtl, str, qt)
      Thread.sleep(10000)
      count += 1
      if ( count > 900) {
        buildRecv()
        count = 0
      }
    } while (true);
  }

  def genSales(itemdtl: String, str: Int, qty: Int) = {
    val Array(itm, d, u, r) = itemdtl.split("-")
    val now = Calendar.getInstance().getTime
    val formatter = new SimpleDateFormat("YYYY-MM-dd")
    val date = formatter.format(now)
    println(msgPart1 + itm + msgPart2 + d + msgPart3 + u + msgPart4
      + itm + msgPart5 + r + msgPart6 + str + msgPart7 + qty + msgPart8 + date + msgPart9)
    val kafkaMsg = msgPart1 + itm + msgPart2 + d + msgPart3 + u + msgPart4 + itm + msgPart5 + r + msgPart6 + str + msgPart7 + qty + msgPart8 + date + msgPart9
    KafkaSalesStream.createSalesProd(kafkaMsg, itm)
  }


  //Stream to generate Inv Data
  def buildRecv(): Unit = {
    val rcvitemArr = buildArr(0).toString
    val rcvitemArr1 = rcvitemArr.split(',')
    val rcvstorelist = Array(100, 200, 300)
    val rcvqty = Array(100, 50, 75, 125, 175, 200)
     for ( z <- 1 to 1000) {
       var rcvrnd = new Random().nextInt(rcvitemArr1.length)
       var rcvitemdtl = rcvitemArr1(rcvrnd)
       var rcvrndStr = new Random().nextInt(rcvstorelist.length)
       var rcvstr = rcvstorelist(rcvrndStr)
       var rcvrnq = new Random().nextInt(rcvqty.length)
       var rcvqt = rcvqty(rcvrnq)

       genRecv(rcvitemdtl, rcvstr, rcvqt)
     }
  }

  def genRecv(rcvitemdtl: String, rcvstr: Int, rcvqt: Int) = {
    val Array(itm, d, u, r) = rcvitemdtl.split("-")
    val now = Calendar.getInstance().getTime
    val formatter = new SimpleDateFormat("YYYY-MM-dd")
    val date = formatter.format(now)
    val csv = ","
    val salesqty = 0
    println(itm + csv + d + csv + u + csv + "item" + itm + csv + r + csv + rcvstr + csv + rcvqt + csv + salesqty + csv + rcvqt + csv + date)
    val kafkaMsgRcv = itm + csv + d + csv + u + csv + "item" + itm + csv + r + csv + rcvstr + csv + rcvqt + csv + salesqty + csv + rcvqt + csv + date
    KafkaSalesStream.createRecvProd(kafkaMsgRcv, itm)
  }

}
