package api;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;

/**
 * Created by prayagupd
 * on 9/28/16
 */

public abstract class Enricher implements Interceptor {
    public abstract void initialize();
    public abstract Event intercept(Event event);

    public abstract List<Event> intercept(List<Event> list);

    public abstract void close();

    public abstract static class EnricherBuilder implements Interceptor.Builder {

        public abstract Interceptor build();

        public abstract void configure(Context context);
    }
}
