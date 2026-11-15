import java.util.*;
import java.util.stream.*;
public class T {
  public static void main(String[] a) {
    List<Integer> xs = Arrays.asList(1, 2, 3, 4, 5);
    List<Integer> sq = xs.stream().filter(x -> x % 2 == 1).map(x -> x * x).collect(Collectors.toList());
    long c = xs.stream().distinct().count();
    int total = xs.stream().mapToInt(x -> x).sum();
    Map<Boolean, Long> g = xs.stream().collect(Collectors.groupingBy(x -> x > 2, Collectors.counting()));
    boolean any = xs.stream().anyMatch(x -> x > 4);
    List<Character> cs = names.stream().flatMap(n -> n.chars().mapToObj(ch -> (char) ch)).collect(Collectors.toList());
    xs.stream().skip(1).count();
  }
}
