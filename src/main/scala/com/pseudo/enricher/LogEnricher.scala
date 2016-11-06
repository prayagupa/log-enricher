package com.pseudo.enricher

import java.util

import com.pseudo.enricher.api.Enricher
import org.apache.flume.Event
import org.apache.log4j.Logger
import org.json.{JSONObject, XML}

/**
 * Created by prayagupd 
 * on 10/7/16.
 */

class LogEnricher extends Enricher {

  val logger = Logger.getLogger(classOf[LogEnricher])

  override def initialize(): Unit = ???

  override def close(): Unit = ???

  override def intercept(event: Event): Event = {
    val eventBody = new String(event.getBody)
    logger.info("received an event " + eventBody)
    val jsonObject = new JSONObject(eventBody)
    val xml = jsonObject.getString("messageXml").replaceAll("[\\t\\n\\r]", " ")
    val messageDataJson = XML.toJSONObject(xml)
    jsonObject.put("messageXml", messageDataJson)
    event.setBody(jsonObject.toString.getBytes)
    logger.info("enriched event " + eventBody)
    event
  }

  override def intercept(list: util.List[Event]): util.List[Event] = ???
}
