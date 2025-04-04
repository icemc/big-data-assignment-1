import org.apache.spark.{SparkConf, SparkContext}
import Utils._

import java.util.regex.Pattern

object SparkWordCount {

  def main(args: Array[String]) {

    if (args.length < 2) {
      System.err.println("Correct arguments: <input-directory> <output-directory>")
      System.exit(1)
    }

    val sc = new SparkContext(new SparkConf().setAppName("SparkWordCount"))

    // old Hadoop trick: make sure we can recursively scan directories for text files!
    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    val counts = sc.textFile(args(0))
      .flatMap(_.toLowerCase.split("[^a-z0-9.,_\\-]+"))
      .filter {token =>
        WORD_PATTERN.matcher(token).matches() || (NUMBER_PATTERN.matcher(token).matches() && token.length >= 4 && token.length <= 16)
      }
      .map((_, 1))
      .reduceByKey(_ + _)
      .coalesce(1)

    counts.saveAsTextFile(args(1))

    sc.stop()
  }
}

