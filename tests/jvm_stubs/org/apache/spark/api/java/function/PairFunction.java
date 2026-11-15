package org.apache.spark.api.java.function;
public interface PairFunction<T, K, V> extends java.io.Serializable { scala.Tuple2<K, V> call(T t) throws Exception; }
