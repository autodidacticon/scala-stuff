package io.moorhead.github.graphql.ingestion

import ujson.Value

object GhGqlQueries {
  def getRepoPoms(v: Value): Seq[(String, String)] = v("data")("organization")("repositories")("nodes").arr
    .filter(!_("pom").isNull)
    .map(v => v("name").str -> v("pom")("text").str)

  def getRepoCursor(v: Value) = Option.apply(v("data")("organization")("repositories")("pageInfo")("endCursor")).map(_.str)

  val getOrgs = (v: Value) => v("data")("organizations")("nodes").arr.map(_ ("name").str)

  val getOrgCursor = (v: Value) => Option.apply(v("data")("organizations")("pageInfo")("endCursor")).map(_.str)

  val orgs = Query(
    """{
      |organizations(first: 100) {
      |   pageInfo{
      |      endCursor
      |    }
      |    nodes {
      |      name
      |    }
      |  }
      |}
      |""".stripMargin
  )

  val orgsCursor = (cursor: String) => Query(
    """
      |query($org_cursor: String!) {
      |  organizations(first: 100, after: $org_cursor) {
      |    pageInfo{
      |      endCursor
      |    }
      |    nodes {
      |      name
      |    }
      |  }
      |}""".stripMargin,
    None,
    Map("org_cursor" -> s"$cursor")
  )

  val repositories = (orgName: String) => Query(
    """
      |query ($org_name: String!) {
      |  organization(login: $org_name) {
      |    orgName: name
      |    repositories(first: 100) {
      |      pageInfo {
      |        endCursor
      |      }
      |      nodes {
      |        name
      |        pom: object(expression: "master:pom.xml") {
      |          ... on Blob {
      |            text
      |          }
      |        }
      |      }
      |    }
      |  }
      |}
      |""".stripMargin,
    None,
    Map("org_name" -> s"$orgName")
  )

  val repositoriesCursor = (orgName: String, repoCursor: String) => Query(
    """query ($org_name: String!, $repo_cursor: String!) {
      |  organization(login: $org_name) {
      |    orgName: name
      |    repositories(first: 100, after: $repo_cursor) {
      |      pageInfo {
      |        endCursor
      |      }
      |      nodes {
      |        name
      |        pom: object(expression: "master:pom.xml") {
      |          ... on Blob {
      |            text
      |          }
      |        }
      |      }
      |    }
      |  }
      |}
      |""".stripMargin,
    None,
    Map(
      "org_name" -> s"$orgName",
      "repo_cursor" -> s"$repoCursor"
    )
  )
}

case class Query(query: String, operationName: Option[String] = None, variables: Map[String, String] = Map.empty)

object Query {
  implicit val ev = upickle.default.macroW[Query]

  implicit class QueryUtil(q: Query) {
    def toJson: String = upickle.default.write(q).replace("\\n", "")
  }

}


