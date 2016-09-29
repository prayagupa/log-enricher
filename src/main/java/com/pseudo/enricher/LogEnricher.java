package com.pseudo.enricher;

import com.pseudo.enricher.api.Enricher;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.json.XML;

import java.util.Iterator;
import java.util.List;

/**
 * Created by prayagupd
 * on 9/28/16
 */

public class LogEnricher extends Enricher {

    Logger logger = Logger.getLogger(LogEnricher.class);

    public void initialize() {

    }

    public Event intercept(Event event) {
        String eventBody = new String(event.getBody());
        logger.info("received an event " + eventBody);
        try {
            JSONObject jsonObject = new JSONObject(eventBody);
            String xml = jsonObject.getString("messageXml").replaceAll("[\\t\\n\\r]"," ");
            JSONObject messageDataJson = XML.toJSONObject(xml);
            jsonObject.put("messageXml", messageDataJson);
            event.setBody(jsonObject.toString().getBytes());
            logger.info("enriched event " + eventBody);
        } catch(Exception e) {
            logger.info("exception parsing " + e.getMessage());
        }
        return event;
    }

    public List<Event> intercept(List<Event> events) {
        for (Iterator<Event> iterator = events.iterator(); iterator.hasNext();) {
            Event next = intercept(iterator.next());
            if (next == null) {
                iterator.remove();
            }
        }

        return events;
    }

    public void close() {

    }

    public static class LogEnricherBuilder extends EnricherBuilder {

        public Interceptor build() {
            return new LogEnricher();
        }

        public void configure(Context context) {

        }
    }
}
