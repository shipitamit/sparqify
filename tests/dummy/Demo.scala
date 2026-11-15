object Demo { def main(args: Array[String]): Unit = {
  val xs = List(1,2,3,4,5,6)
  val ev = xs.filter(_ % 2 == 0).map(x => x * x)
  println(ev.sum)
  val words = scala.io.Source.fromFile("in.txt").getLines().toList.flatMap(_.split(" "))
  val counts = words.groupBy(identity).mapValues(_.size)
}}
