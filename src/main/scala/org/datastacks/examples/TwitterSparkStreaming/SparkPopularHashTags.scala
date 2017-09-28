package org.datastacks.examples.TwitterSparkStreaming

import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hemabhatia on 9/26/17.
  */
object SparkPopularHashTags {

  val conf = new SparkConf().setMaster("local[4]").setAppName("Spark Streaming - popular hashtags")
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {

    sc.setLogLevel("WARN")
    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val filters = args.takeRight(args.length - 4)

    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val ssc = new StreamingContext(sc, Seconds(5))

    // create stream
    val stream = TwitterUtils.createStream(ssc, None, filters)

    // split stream on space and filter hastags by #
    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    // top 60 tweets in last 60 seconds
    val topCount60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
        .map { case (topic, count) => (count, topic)}
          .transform(_.sortByKey(false))

    // top 10 tweets in last 10 seconds
    val topCount10 = hashTags.map((_,1)).reduceByKeyAndWindow(_ + _, Seconds(10))
        .map {case (topic, count) => (count, topic)}
          .transform(_.sortByKey(false))

    // print tweets in the current DStream
    stream.print()

    //print popular hashtags
    topCount60.foreachRDD(rdd => {
      val topList = rdd.take(10);
      println("\nPopular topics in last 60 seconds (%s total)".format(rdd.count()))
      topList.foreach { case(count, tag) => println("%s (%s tweets)".format(tag, count))}
    })

    topCount10.foreachRDD(rdd => {
      val topList = rdd.take(10);
      println("\nPopular topics in last 10 seconds (%s total)".format(rdd.count()))
      topList.foreach { case(count, tag) => println("%s (%s tweets)".format(tag, count))}
    })

    ssc.start()
    ssc.awaitTermination();
  }

}
