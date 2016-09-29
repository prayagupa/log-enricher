import api.Enricher;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.json.JSONObject;
import org.json.XML;

import java.util.Iterator;
import java.util.List;

/**
 * Created by prayagupd
 * on 9/28/16
 */

public class LogEnricher extends Enricher {

    public void initialize() {

    }

    public Event intercept(Event event) {
        String eventBody = new String(event.getBody());
        JSONObject jsonObject = new JSONObject(eventBody);
        JSONObject messageXml = XML.toJSONObject(jsonObject.getString("messageXml"));
        jsonObject.put("messageXml", messageXml);
        event.setBody(jsonObject.toString().getBytes());
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
