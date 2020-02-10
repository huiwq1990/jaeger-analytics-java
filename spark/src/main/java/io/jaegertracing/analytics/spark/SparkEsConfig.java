package io.jaegertracing.analytics.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import java.util.List;

public class SparkEsConfig {

    static JavaSparkContext context;
    static {
        context = getConfig(getPropOrEnv("ES_SERVER","114.67.115.239"),getPropOrEnv("ES_PORT","9200"));
    }

    static String getPropOrEnv(String key, String defaultValue) {
        String value = System.getProperty(key, System.getenv(key));
        return value != null ? value : defaultValue;
    }

    public static void write(String type, List<String> jsonList){
        JavaEsSpark.saveJsonToEs(context.parallelize(jsonList), type);
    }

    public  static JavaSparkContext getConfig(String esServer, String esPort){
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("callchainAnalyzeTime");
        sparkConf.setMaster("local");
        sparkConf.set("es.nodes", esServer);
        sparkConf.set("es.port", esPort);
//        if (Strings.isNotEmpty(esClusterConfig.getUser()) && Strings.isNotEmpty(esClusterConfig.getPass())) {
//            sparkConf.set("es.net.http.auth.user", String.valueOf(esClusterConfig.getUser()));
//            sparkConf.set("es.net.http.auth.pass", String.valueOf(esClusterConfig.getPass()));
//        }
        sparkConf.set("es.scroll.size", "10000");
        sparkConf.set("es.index.auto.create", "true");//自动创建索引
        sparkConf.set("es.nodes.wan.only", "true");//只用域名访问
        sparkConf.set("es.index.read.missing.as.empty", "true");
        sparkConf.set("spark.broadcast.compress", "true");// 设置广播压缩
        sparkConf.set("spark.rdd.compress", "true");      // 设置RDD压缩
        sparkConf.set("spark.io.compression.codec", "org.apache.spark.io.SnappyCompressionCodec");
        sparkConf.set("spark.shuffle.file.buffer", "10m");
        sparkConf.set("spark.shuffle.io.maxRetries", "3");
        sparkConf.set("spark.io.compression.snappy.blockSize", "10m");

        return new JavaSparkContext(sparkConf);
    }
}
