import org.apache.spark.{SparkConf, SparkContext}
import Utils._

import scala.util.Try

object WordPairsFilter {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Correct arguments: <input-directory> <output-directory>")
      System.exit(1)
    }

    val sc = new SparkContext(new SparkConf().setAppName("WordPairsFilter"))
    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    sc.textFile(args(0))
      .flatMap {
        case TuplePattern(pair@PairPattern(left, right), "1000")
          if WORD_PATTERN.matcher(left).matches() &&
            WORD_PATTERN.matcher(right).matches()  => Some((pair.trim, 1000))
        case _ => None
      }
      .coalesce(1)
      .saveAsTextFile(args(1))

    sc.stop()
  }
}
