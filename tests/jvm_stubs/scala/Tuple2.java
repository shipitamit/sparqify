package scala;
public class Tuple2<A, B> implements java.io.Serializable {
    private final A a; private final B b;
    public Tuple2(A a, B b) { this.a = a; this.b = b; }
    public A _1() { return a; }
    public B _2() { return b; }
}
