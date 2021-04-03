import wikipedia.{WikipediaArticle, WikipediaRanking}
import wikipedia.WikipediaRanking.sc
import wikipedia.WikipediaRanking._

def initializeWikipediaRanking(): Boolean =
  try {
    WikipediaRanking
    true
  } catch {
    case ex: Throwable =>
      println(ex.getMessage)
      ex.printStackTrace()
      false
  }

initializeWikipediaRanking()

val langs = List("Scala", "Java", "Python", "Erlang")
val articles = List(
  WikipediaArticle("1","Groovy is pretty interesting, and so is Erlang"),
  WikipediaArticle("2","Scala and Java run on the JVM"),
  WikipediaArticle("3","Scala is not purely functional")
)
val rdd = sc.parallelize(articles)
rdd
  .flatMap(wikiArticle => langs.map(l => (l, wikiArticle.mentionsLanguage(l)))) // RDD[String, WikipediaArticle, Boolean]
  .filter(_._2)
  .map(wikiArticle => (wikiArticle._1, 1)) // RDD[(String, WikipediaArticle)]
  .reduceByKey((a,b) => a+b)
  .collect()