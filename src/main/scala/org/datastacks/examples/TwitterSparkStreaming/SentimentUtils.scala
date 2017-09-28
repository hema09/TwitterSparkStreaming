package org.datastacks.examples.TwitterSparkStreaming

import java.util.Properties

import com.sun.org.apache.bcel.internal.generic.IF_ACMPEQ
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

/**
  * Created by hemabhatia on 9/27/17.
  */
object SentimentUtils {

  val nlpProperties = {
    val properties = new Properties()
    /* Annotators - Meaning http://corenlp.run/
       tokenize   - Tokenize the sentence.
       ssplit     - Split the text into sentence. Identify fullstop, exclamation etc and split sentences
       pos        - Reads text in some language and assigns parts of speech to each word (and other token), such as noun, verb, adjective, etc.
       lemma      - Group together the different inflected forms of a word so they can be analysed as a single item.
       parse      - Provide syntactic analysis http://nlp.stanford.edu:8080/parser/index.jsp
       sentiment  - Provide model for sentiment analysis
       * */
    properties.setProperty("annotators", "tokenize, ssplit, pos, lemma, parse, sentiment")
    properties
  }

  def detectSentiment(message: String) : String = {
    val pipeline = new StanfordCoreNLP(nlpProperties)

    val annotation = pipeline.process(message)
    val sentiments: ListBuffer[Double] = ListBuffer()
    val sizes: ListBuffer[Int] = ListBuffer()

    var longest = 0
    var mainSentiment = 0

    // An Annotation is a Map and you can get and use the various analyses individually.
    // For instance, this gets the parse tree of the first sentence in the text.
    // Iterate through tweet
    for(tweetMsg <- annotation.get(classOf[CoreAnnotations.SentencesAnnotation])) {
      val parseTree = tweetMsg.get(classOf[SentimentCoreAnnotations.AnnotatedTree])
      val tweetSentiment = RNNCoreAnnotations.getPredictedClass(parseTree)
      val partText = tweetMsg.toString

      if(partText.length() > longest) {
        mainSentiment = tweetSentiment
        longest = partText.length()
      }

      sentiments += tweetSentiment.toDouble
      sizes += partText.length
    }

    val weightedSentiments = (sentiments, sizes).zipped.map((sentiment, size) => sentiment * size)
    var weightedSentiment = weightedSentiments.sum / (sizes.fold(0)(_ + _))

    if(weightedSentiment <= 0.0)
      "NOT_UNDERSTOOD"
    else if (weightedSentiment < 1.6)
      "NEGATIVE"
    else if(weightedSentiment <= 2.0)
      "NEUTRAL"
    else if(weightedSentiment < 5.0)
      "POSITIVE"
    else "NOT_UNDERSTOOD"
  }

}
