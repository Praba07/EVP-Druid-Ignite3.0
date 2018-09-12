package main.Scala

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.KStreamBuilder
import org.json.simple.JSONArray
import org.json.simple.JSONObject
import org.json.simple.parser.JSONParser
import org.json.simple.parser.ParseException
import java.lang.Object
import java.text.SimpleDateFormat
import java.util
import java.util.Date
import java.util.Objects
import java.util.Properties


class SalesStream {

  @throws[ParseException]
  def main(args: Array[String]): Unit = {
    val streamProperties = new Properties
    streamProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093")
    streamProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "StreamApp1")
    streamProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
    streamProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
    val builder = new KStreamBuilder
    val stringSerde = Serdes.String
    val JsonStream = builder.stream(stringSerde, stringSerde, "Salesinput")
    val transformedJson = JsonStream.flatMapValues((value: String) => {
      def foo(value: String) = {
        var data = null
        var Message = ""
        val Message2 = ""
        try {
          val parser = new JSONParser
          val data = parser.parse(value.toString).asInstanceOf[JSONObject]
          val node = data.get("FO").asInstanceOf[JSONArray]
          var i = 0
          while ( {
            i < node.size
          }) {
            val `object` = node.get(i).asInstanceOf[JSONObject]
            val olarr = `object`.get("OrderLine").asInstanceOf[JSONArray]
            var j = 0
            while ( {
              j < olarr.size
            }) {
              val olobj = olarr.get(j).asInstanceOf[JSONObject]
              val dt = new SimpleDateFormat("yyyy-MM-dd").format(new Date)
              val tf = new SimpleDateFormat("HH:mm:ss").format(new Date)
              val ts = dt + "T" + tf + ".000Z"
              val dts = "" + ts + ""
              //JSONArray salesqty = (JSONArray) olobj.get("sales_qty");
              //String str1 = "";
              val str1 = olobj.get("sales_qty")
              val str2 = "-" + str1
              olobj.put("sales_qty2", str2)
              olobj.put("timestamp", dts)
              Message = olobj.toString + " " + Message

              {
                j += 1; j - 1
              }
            }

            {
              i += 1; i - 1
            }
          }
        } catch {
          case ex: ParseException =>
            System.out.println(ex)
        }
        util.Arrays.asList(Message)
      }

      foo(value)
    }).flatMapValues((value: String) => util.Arrays.asList(value.split(" ")))
    transformedJson.print()
    transformedJson.to("Salesinf")
    val kafkaStreams = new KafkaStreams(builder, streamProperties)
    kafkaStreams.start()
  }

}
