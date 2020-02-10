package io.jaegertracing.analytics.model;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;

public class Span implements Serializable {
    public String traceId;
    public String spanId;
    // TODO add references array
    public String parentId;
    public String serviceName;
    public String operationName;
    public long startTimeMicros;
    public long durationMicros;
    public Map<String, String> tags = new HashMap<>();
}
