package org.apache.spark.api.java;
import java.util.*;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;
public class JavaPairRDD<K, V> implements java.io.Serializable {
    public JavaPairRDD<K, V> reduceByKey(Function2<V, V, V> f) { return null; }
    public Map<K, V> collectAsMap() { return null; }
    public JavaPairRDD<K, V> filter(Function<Tuple2<K, V>, Boolean> f) { return null; }
    public <R> JavaRDD<R> map(Function<Tuple2<K, V>, R> f) { return null; }
    public <U> JavaPairRDD<K, U> mapValues(Function<V, U> f) { return null; }
    public JavaRDD<K> keys() { return null; }
    public JavaRDD<V> values() { return null; }
    public long count() { return 0; }
    public List<Tuple2<K, V>> collect() { return null; }
}
