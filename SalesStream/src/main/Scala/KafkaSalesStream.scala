package main.Scala

package com.cloudurable.kafka

import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.{LongSerializer, Serdes, StringSerializer}
import java.util.Properties
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata

object KafkaSalesStream {
  private val TOPIC = "Salesinput"
  private val BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093"

  import org.apache.kafka.clients.producer.KafkaProducer
  import org.apache.kafka.clients.producer.Producer
  import org.apache.kafka.clients.producer.ProducerConfig

  def createSalesProd (kafkaMsg: String, key: String) = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "SalesProducer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    val time = System.currentTimeMillis
    val producer = new KafkaProducer[String, String] (props)
    try {
        val data = new ProducerRecord[String, String](TOPIC, key, kafkaMsg.toString)
        producer.send(data)
    } finally {
      producer.close()
    }
  }

  def createRecvProd (kafkaMsgRcv: String, key: String) = {
    val props = new Properties()
    val TOPIC = "DCinTrans"
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "RecvProducer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    val time = System.currentTimeMillis
    val producer = new KafkaProducer[String, String] (props)
    try {
      val data = new ProducerRecord[String, String](TOPIC, key, kafkaMsgRcv.toString)
      producer.send(data)
    } finally {
      producer.close()
    }
  }
}


