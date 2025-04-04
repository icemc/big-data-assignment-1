import org.apache.spark.{SparkConf, SparkContext}

import  Utils._
import scala.util.Try

object WordNumberPairsTop {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Correct arguments: <input-directory> <output-directory>")
      System.exit(1)
    }

    val sc = new SparkContext(new SparkConf().setAppName("NumberWordPairsTop"))
    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    val topPairs = sc.textFile(args(0))
      .flatMap {
        case TuplePattern(pair@PairPattern(left, right), count)
          if (NUMBER_PATTERN.matcher(left).matches() && left.length >= 4 && left.length <= 16) &&
            WORD_PATTERN.matcher(right).matches() &&
            Try(count.trim.toInt).toOption.nonEmpty => Some((pair, count.trim.toInt))
        case _ => None
      }
      .top(100)(Ordering.by(_._2))

    sc.parallelize(topPairs)
      .coalesce(1)
      .saveAsTextFile(args(1))

    sc.stop()
  }
}
