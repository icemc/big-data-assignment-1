import org.apache.spark.{SparkConf, SparkContext}
import Utils._

import scala.collection.immutable.Queue

object SparkWordPairs {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Correct arguments: <input-directory> <output-directory>")
      System.exit(1)
    }

    val sc = new SparkContext(new SparkConf().setAppName("SparkWordPairs"))

    // Enable recursive directory scanning
    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    val counts = sc.textFile(args(0))
      .flatMap { line =>
        val tokens = line.toLowerCase.split("[^a-z0-9.,_\\-]+")
          .filter { token =>
            WORD_PATTERN.matcher(token).matches() ||
              (NUMBER_PATTERN.matcher(token).matches() && token.length >= 4 && token.length <= 16)
          }

        // Process tokens with immutable Queue
        tokens.foldLeft((Queue.empty[String], List.empty[(String, Int)])) {
          case ((window, pairs), token) =>
            // Generate pairs with all words in window
            val newPairs = window.map(prevWord => (s"$prevWord:$token", 1))

            // Update window (maintain size 2)
            val newWindow = if (window.size == 2) window.dequeue._2.enqueue(token)
            else window.enqueue(token)

            (newWindow, pairs ++ newPairs)
        }._2 // Return just the pairs
      }
      .reduceByKey(_ + _)
      .coalesce(1)

    counts.saveAsTextFile(args(1))

    sc.stop()
  }
}

