package org.datastacks.examples.TwitterSparkStreaming


import SentimentUtils._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import java.util.Date
import java.text.SimpleDateFormat
import java.util.Locale

import scala.util.Try
/**
  * Created by hemabhatia on 9/27/17.
  */
object TwitterSentimentAnalysis {


  def main(args: Array[String]): Unit = {


    if (args.length < 4) {
      System.err.println("Usage: TwitterSentimentAnalysis <consumer key> <consumer secret> " +
        "<access token> <access token secret> [<filters>]")
      System.exit(1)
    }

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val filters = args.takeRight(args.length - 4)

    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val conf = new SparkConf().setAppName("TwitterSentimentAnalysis").setMaster("local[4]")

    // Create a DStream for every 5 seconds
    val ssc = new StreamingContext(conf, Seconds(5))

    // create stream
    val tweets = TwitterUtils.createStream(ssc, None, filters)

    tweets.print()

    tweets.foreachRDD((rdd, time) =>
      rdd.map(t => {
        Map(
          "user" -> t.getUser.getScreenName,
          "created_at" -> t.getCreatedAt.getTime.toString,
          "location" -> Option(t.getGeoLocation).map(geo => { s"${geo.getLatitude},${geo.getLongitude}"}),
          "text" -> t.getText,
          "hashTags" -> t.getHashtagEntities.map(_.getText),
          "retweet" -> t.getRetweetCount,
          "language" -> t.getLang.toString(),
          "sentiment" -> detectSentiment(t.getText).toString
        )
      }).saveToEs("twitter_092717/tweet")
    )

    ssc.start()
    ssc.awaitTermination();
  }

}
