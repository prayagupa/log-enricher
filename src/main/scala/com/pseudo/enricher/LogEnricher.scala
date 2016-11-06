package com.pseudo.enricher

import java.util

import com.pseudo.enricher.api.Enricher
import org.apache.flume.Event
import org.apache.log4j.Logger
import org.json.{JSONObject, XML}
import scala.collection.JavaConverters._

/**
 * Created by prayagupd 
 * on 10/7/16.
 */

class LogEnricher extends Enricher {

  val logger = Logger.getLogger(classOf[LogEnricher])

  override def initialize(): Unit = {}

  override def close(): Unit = {}

  override def intercept(event: Event): Event = {
    val eventBody = new String(event.getBody)
    logger.info("received an event " + eventBody)
    val jsonObject = new JSONObject(eventBody)
    val requestEvent = jsonObject.getString("message")
    val metrics : List[(String, String)] =
      requestEvent.split(",").map(kv => (kv.split("=")(0), kv.split("=")(1))).toList

    metrics.map(kv => jsonObject.put(kv._1, getJson(kv._2)))
    jsonObject.remove("message")
    event.setBody(jsonObject.toString.getBytes)
    logger.info("enriched event " + eventBody)
    event
  }

  private def getJson(value: String) : Object = {
    if(value.startsWith("<")) {
      val messageDataJson = XML.toJSONObject(value)
      messageDataJson
    } else {
      value
    }
  }

  override def intercept(list: util.List[Event]): util.List[Event] = list.asScala.map(e => intercept(e)).asJava
}
