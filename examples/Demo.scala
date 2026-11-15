object Demo {
  def main(args: Array[String]): Unit = {
    val nums = List(1, 2, 3, 4, 5, 6)
    val evens = nums.filter(_ % 2 == 0).map(x => x * x)
    val total = nums.map(_ * 2).sum
    val n = evens.size
    val words = Source.fromFile("book.txt").getLines().toList
    val counts = words.flatMap(_.split(" ")).groupBy(identity).mapValues(_.size)
    val top = words.map(_.length).sortBy(x => -x).take(3)
    val s = nums.foldLeft(0)(_ + _)
    val rev = nums.sorted.reverse
    println(evens)
  }
}
