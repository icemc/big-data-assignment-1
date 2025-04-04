import org.apache.spark.{SparkConf, SparkContext}

import Utils._
import scala.util.Try

object WordCountTop {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Correct arguments: <input-directory> <output-directory>")
      System.exit(1)
    }

    val sc = new SparkContext(new SparkConf().setAppName("WordCountTop"))
    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    val topWords = sc.textFile(args(0))
      .flatMap {
        case TuplePattern(word, count)
          if WORD_PATTERN.matcher(word).matches() && Try(count.trim.toInt).toOption.nonEmpty =>
          Some((word, count.trim.toInt))
        case _ => None
      }
      .top(100)(Ordering.by(_._2))

    sc.parallelize(topWords)
      .coalesce(1)
      .saveAsTextFile(args(1))

    sc.stop()
  }
}
