package org.apache.spark.api.java;
import java.util.*;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;
/** Signatures copied from Spark 3.x JavaRDD / JavaRDDLike (bodies are stubs: this only type-checks converted code). */
public class JavaRDD<T> implements java.io.Serializable {
    public JavaRDD<T> filter(Function<T, Boolean> f) { return null; }
    public <R> JavaRDD<R> map(Function<T, R> f) { return null; }
    public <U> JavaRDD<U> flatMap(FlatMapFunction<T, U> f) { return null; }
    public <K2, V2> JavaPairRDD<K2, V2> mapToPair(PairFunction<T, K2, V2> f) { return null; }
    public JavaDoubleRDD mapToDouble(DoubleFunction<T> f) { return null; }
    public JavaRDD<T> distinct() { return null; }
    public <S> JavaRDD<T> sortBy(Function<T, S> f, boolean ascending, int numPartitions) { return null; }
    public JavaRDD<T> union(JavaRDD<T> other) { return null; }
    public JavaRDD<T> cache() { return null; }
    public List<T> collect() { return null; }
    public long count() { return 0; }
    public List<T> take(int n) { return null; }
    public T first() { return null; }
    public boolean isEmpty() { return false; }
    public T reduce(Function2<T, T, T> f) { return null; }
    public T fold(T zero, Function2<T, T, T> f) { return null; }
    public void foreach(VoidFunction<T> f) { }
    public T max(Comparator<T> c) { return null; }
    public T min(Comparator<T> c) { return null; }
    public JavaPairRDD<T, Long> zipWithIndex() { return null; }
    public <U> JavaPairRDD<U, Iterable<T>> groupBy(Function<T, U> f) { return null; }
    public Map<T, Long> countByValue() { return null; }
}
