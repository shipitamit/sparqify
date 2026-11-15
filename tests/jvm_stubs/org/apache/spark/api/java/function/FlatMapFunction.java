package org.apache.spark.api.java.function;
public interface FlatMapFunction<T, R> extends java.io.Serializable { java.util.Iterator<R> call(T t) throws Exception; }
