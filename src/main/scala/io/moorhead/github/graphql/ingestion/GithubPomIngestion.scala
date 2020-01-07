package io.moorhead.github.graphql.ingestion

import io.moorhead.github.graphql.ingestion.Query.QueryUtil
import org.apache.spark.sql.SparkSession
import sttp.client._
import sttp.model.HeaderNames

import scala.annotation.tailrec
import scala.util.Try

object GithubPomIngestion extends App {
  val authenticationToken = sys.env.get("AUTH_TOKEN").get
  val searchUri = sys.env.get("GITHUB_API_HOST").get

  implicit val backend = HttpURLConnectionBackend()

  val request = (token: String, uriStr: String, query: Query) =>
    basicRequest.header(HeaderNames.Authorization, s"Bearer $token")
    .post(uri"$uriStr")
    .body(query.toJson)
    .send()
    .body
    .right
    .get

  val query = GhGqlQueries.orgs
  val ghr = request(authenticationToken, searchUri, query)
  val json = ujson.read(ghr)
  val startCursor = GhGqlQueries.getOrgCursor(json)
  val startOrgs = GhGqlQueries.getOrgs(json)
  val orgs = orgsRec(startCursor, startOrgs)
  val spark = SparkSession.builder.appName(GithubPomIngestion.getClass.getName).getOrCreate()
  import spark.implicits._
  val writtenOrgs = Try { spark.read.parquet(Constants.rawGraphItemsPath).select($"org").distinct.as[String].collect }.getOrElse(Array.empty[String])
  orgs.diff(writtenOrgs).map(orgName => {
    Try {request(authenticationToken, searchUri, GhGqlQueries.repositories(orgName)) }
      .map(rawJson => {
        val repoJson = ujson.read(rawJson)
        val repoCursor = GhGqlQueries.getRepoCursor(repoJson)
        val repositoriesPoms = GhGqlQueries.getRepoPoms(repoJson)
        val rawGraphItems = repositoriesPoms.map{case(r, p) => RawGraphItem(orgName, r, p)}
        write(spark, Constants.rawGraphItemsPath, reposRec(orgName, repoCursor, rawGraphItems))
      })
  })

  @tailrec
  def orgsRec(startCursor: Option[String], orgs: Seq[String] = Seq.empty): Seq[String] = {
    startCursor match {
      case Some(cursor) => {
        val (newCursor, newOrgs) = Try(request(authenticationToken, searchUri, GhGqlQueries.orgsCursor(cursor)))
          .map(ujson.read(_))
          .map(v => GhGqlQueries.getOrgCursor(v) -> (orgs ++ GhGqlQueries.getOrgs(v))).toOption
          .getOrElse(None -> orgs)
        orgsRec(newCursor, newOrgs)
      }
      case None => orgs
    }
  }

  @tailrec
  def reposRec(orgName: String, startCursor: Option[String], graphItems: Seq[RawGraphItem] = Seq.empty): Seq[RawGraphItem] = {
    startCursor match {
      case Some(cursor) => {
        val (newCursor, newRawGraphItems) = Try(request(authenticationToken, searchUri, GhGqlQueries.repositoriesCursor(orgName, cursor)))
          .map(ujson.read(_))
          .map(v => {
            val repositoriesPoms = GhGqlQueries.getRepoPoms(v)
            val rawGraphItems = repositoriesPoms.map{case(r, p) => RawGraphItem(orgName, r, p)}
            GhGqlQueries.getRepoCursor(v) -> (graphItems ++ rawGraphItems)
          }).toOption
          .getOrElse(None -> graphItems)
        reposRec(orgName, newCursor, newRawGraphItems)
      }
      case None => graphItems
    }
  }

  def write(spark: SparkSession, path: String, rawGraphItems: Seq[RawGraphItem]): Unit = {
    rawGraphItems.toDS().write.mode("append").partitionBy("org").parquet(path)
  }
}

case class RawGraphItem(org: String, repo: String, pom: String)

