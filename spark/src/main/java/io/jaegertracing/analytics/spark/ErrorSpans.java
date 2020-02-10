package io.jaegertracing.analytics.spark;

import com.alibaba.fastjson.JSON;
import com.clearspring.analytics.util.Lists;
import io.jaegertracing.analytics.ModelRunner;
import io.jaegertracing.analytics.gremlin.GraphCreator;
import io.jaegertracing.analytics.model.Span;
import io.jaegertracing.analytics.model.Trace;
import io.opentracing.tag.Tags;
import io.prometheus.client.Counter;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

public class ErrorSpans implements ModelRunner {

    private static final Counter counter = Counter.build()
        .name("trace_quality_error_spans")
        .help("The service emitted spans with error tags")
        .labelNames("service")
        .create()
        .register();

    @Override
    public void runWithMetrics(Graph graph) {
        Result ret = calculate(graph);
        if(ret != null && ret.errors != null && ret.errors.size() > 0){
            Map<String, Set<String>> group = ret.errors.stream().collect(
                    Collectors.groupingBy(ErrorInfo::getEsType,
                            Collectors.mapping(tmp -> JSON.toJSONString(tmp), Collectors.toSet())
                    ));
            group.entrySet().forEach( tmp -> SparkEsConfig.write(tmp.getKey(), Lists.newArrayList(tmp.getValue())));
        }
    }

    public static class Result {
        public List<ErrorInfo> errors = new ArrayList<>();
    }
    public static class ErrorInfo{
        private String spanId;
        private String traceId;
        private long startTimeMicros;

        public ErrorInfo(String traceId, String spanId, long startTimeMicros){
            this.traceId = traceId;
            this.spanId =  spanId;
            this.startTimeMicros = startTimeMicros;
        }

        public String getEsType(){
            TimeZone utcTZ= TimeZone.getTimeZone("UTC");
            Calendar utcCal= Calendar.getInstance(utcTZ);
            utcCal.setTimeInMillis(startTimeMicros/1000);

            SimpleDateFormat sdf= new SimpleDateFormat("yyyy-MM-dd");
            sdf.setTimeZone(utcTZ);
            Date utcDate= utcCal.getTime();
            return "jaeger-errorspan-" + sdf.format(utcDate) + "/errorspan";
        }

        public String getSpanId() {
            return spanId;
        }

        public String getTraceId() {
            return traceId;
        }

        public long getStartTimeMicros() {
            return startTimeMicros;
        }
    }

    public static Result calculate(Graph graph){
        Result result = new Result();
        Iterator<Vertex> vertices = graph.vertices();
        while (vertices.hasNext()) {
            Vertex vertex = vertices.next();
            Span span = GraphCreator.toSpan(vertex);
            String errVal = span.tags.get(Tags.ERROR.getKey());
            if(Boolean.valueOf(errVal)){
                counter.labels(span.serviceName)
                        .inc();
                result.errors.add(new ErrorInfo(span.traceId,span.spanId,span.startTimeMicros));
            }
        }
        return result;
    }

    public static void main(String[] args) {
        Span root =newTrace("root", "root");
        Span child = newChild("child", "child", root);
        child.tags.put(Tags.ERROR.getKey(),Boolean.TRUE.toString());

        Trace trace = new Trace();
        trace.spans = Arrays.asList(root, child);
        Graph graph = GraphCreator.create(trace);
        ErrorSpans analysis = new ErrorSpans();
        analysis.runWithMetrics(graph);
    }

    public static Span newTrace(String serviceName, String operationName) {
        Span span = new Span();
        span.serviceName = serviceName;
        span.operationName = operationName;
        span.spanId = UUID.randomUUID().toString();
        span.traceId = UUID.randomUUID().toString();
        span.startTimeMicros = new Date().getTime() * 1000;
        return span;
    }

    public static Span newChild(String serviceName, String operationName, Span parent) {
        Span span = new Span();
        span.serviceName = serviceName;
        span.operationName = operationName;
        span.spanId = UUID.randomUUID().toString();
        span.traceId = parent.traceId;
        span.parentId = parent.spanId;
        span.startTimeMicros = new Date().getTime() * 1000;
        return span;
    }
}
