package io.moorhead.github.graphql.ingestion

import com.databricks.spark.xml.functions.from_xml
import com.databricks.spark.xml.schema_of_xml
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Try

object RawPomToGraphConversion extends App {

  val spark = SparkSession.builder.appName(this.getClass.getName).getOrCreate()

  import spark.implicits._
  val LOGGER = LoggerFactory.getLogger(this.getClass)

  val df = spark.read.parquet(Constants.rawGraphItemsPath)
  Await.result(Future.traverse(df.select($"org", $"repo").as[OrgRepo].collect.toSeq)(orgRepo => Future {
    val payloadSchema = schema_of_xml(df.filter($"org" === orgRepo.org && $"repo" === orgRepo.repo).select("pom").as[String])
    val parsedDf = df.filter($"org" === orgRepo.org && $"repo" === orgRepo.repo)
      .withColumn("parsed", from_xml($"pom", payloadSchema))
      .select($"org", $"repo", $"parsed")
      .withColumn("artifactId", $"parsed.artifactId")
    Try {
      parsedDf.filter($"parsed._corrupt_record".isNull)
    } recover {
      case  _ => parsedDf
    } flatMap {
      df => Try {
        df.withColumn("groupId", $"parsed.groupId")
      } recover {
        case _ => df
      }
    } flatMap {
      df => Try {
        df.withColumn("parent", $"parsed.parent")
      } recover {
        case _ => df
      }
    } flatMap  {
      df => Try { df.withColumn("modules", explode($"parsed.modules")) }
          .recover{ case _ => df }
    } flatMap {
      df => Try  {
        df.select(explode($"parsed.dependencyManagement.dependencies.dependency").as("dependency"))
      } recover {
        case _ => df.select(
          explode($"parsed.dependencies.dependency").as("dependency"))
      }
    } foreach {
      _.write.option("mode", "overwrite")
        .parquet(s"${Constants.graphItemsPath}/org=${orgRepo.org}/repo=${orgRepo.repo}")
    }
  } recover {
    case e => LOGGER.error(e.getMessage)
  }), Duration.Inf)

  case class OrgRepo(org: String, repo: String)

}

