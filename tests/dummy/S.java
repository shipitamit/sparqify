import java.util.*; import java.util.stream.*;
public class S { public static void main(String[] a){ List<Integer> xs = Arrays.asList(1,2,3,4);
 List<Integer> ev = xs.stream().filter(x -> x % 2 == 0).map(x -> x * x).collect(Collectors.toList()); } }
