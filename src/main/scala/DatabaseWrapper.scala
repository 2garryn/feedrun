import java.util.UUID

import com.datastax.driver.core.{ResultSet, Row}
import com.datastax.driver.core.querybuilder.{QueryBuilder, Select}
import org.joda.time.DateTime

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer


object DatabaseWrapper {
  val keyspace = "mybase2"
  val activityTable = "activities"
  val startYear = 2015

  DatabaseConnect.initialize(keyspace)
  createTables()


  def createTables() = {
    createActivityTable(activityTable)
  }


  def createActivityTable(activityTable: String) = {
    val fields = Map(
      "username" -> "text",
      "feedname" -> "text",
      "activity_id" -> "uuid",
      "published" -> "bigint",
      "cluster_key" -> "text",
      "year" -> "bigint",
      "actor" -> "text",
      "verb" -> "text",
      "object" -> "text",
      "target" -> "text",
      "foreign_id" -> "text")
    val partKey = Seq("username", "feedname", "year")
    val clusterKey = Seq("cluster_key")
    DatabaseConnect.createTable(activityTable, fields, partKey, clusterKey, "cluster_key", "desc")
  }


  def putActivity(username: String, feedname: String, activity: Activity) = {
    val year = activity.published.getYear()
    val published = activity.published.getMillis()
    val values = Map(
      "username" -> username,
      "feedname" -> feedname,
      "activity_id" -> activity.id,
      "published" -> published,
      "cluster_key" -> getClusterKey(activity),
      "year" -> year,
      "actor" -> activity.actor,
      "object" -> activity.obj,
      "verb" -> activity.verb,
      "target" -> activity.target,
      "foreign_id" -> activity.foreign_id
    )

    DatabaseConnect.insert(activityTable, values)
  }

  def getActivities(username: String, feedname: String, contid: ContinuationId, limit: Int = 10): ActivityResultSet = {
    contid match {
      case _: ActivityContIdStop  => ActivityResultSet(List[Activity](), ActivityContIdStop())
      case _: ActivityContIdStart => {
        val year = new DateTime().getYear()
        val rs = DatabaseConnect.query( getActivityQueryBuilder(username, feedname, year, limit) )
        val res = rawResultsToActivities(rs)
        doGetActivities(username, feedname, year - 1, limit, res.length, res)
      }
      case contid: ActivityContId => {
        val rs = DatabaseConnect.query( getActivityQueryBuilder(username, feedname, contid, limit) )
        val res = rawResultsToActivities(rs)
        doGetActivities(username, feedname, contid.published.getYear - 1, limit, res.length, res)
      }
    }

  }

  @tailrec
  private def doGetActivities( username: String, feedname: String, year: Int, limit: Int, limitDone: Int, result: List[Activity]): ActivityResultSet = {
    limitDone match {
      case _ if limit == limitDone => {
        prepareActivityContId(limit, result)
      }
      case _ if year < startYear => {
        prepareActivityContId(limit, result)
      }
      case _ => {
        val newYear = year - 1
        val rs = DatabaseConnect.query( getActivityQueryBuilder(username, feedname, year, limit - limitDone) )
        val res = rawResultsToActivities(rs)
        doGetActivities(username: String, feedname: String, newYear, limit, res.length + limitDone, result ++ res)

      }
    }
  }

  private def prepareActivityContId(limit: Int, result: List[Activity]): ActivityResultSet = {
    if( result.length < limit) {
      ActivityResultSet(result, ActivityContIdStop())
    } else {
      val last = result.last
      ActivityResultSet(result, ActivityContId(last.published, last.id))
    }
  }


  private def rawResultsToActivities(rs: ResultSet): List[Activity] = {
    var ls = ListBuffer[Activity]()
    rs.all().forEach({ r: Row =>
      ls += mapRowToActivity(r)
    })
    ls.toList
  }

  private def getActivityQueryBuilder(username: String, feedname: String, year: Int, limit: Int): Select = {
    QueryBuilder
      .select()
      .from(activityTable)
      .where(QueryBuilder.eq("username", username))
      .and(QueryBuilder.eq("feedname", feedname))
      .and(QueryBuilder.eq("year", year))
      .limit(limit)
  }

  private def getActivityQueryBuilder(username: String, feedname: String, contid: ActivityContId, limit: Int): Select = {
    QueryBuilder
      .select()
      .from(activityTable)
      .where(QueryBuilder.eq("username", username))
      .and(QueryBuilder.eq("feedname", feedname))
      .and(QueryBuilder.eq("year", contid.published.getYear))
      .and(QueryBuilder.lt("cluster_key", getClusterKey(contid.published, contid.activity_id)))
      .limit(limit)
  }

  private def mapRowToActivity(row: Row): Activity = {
    Activity(
      verb = row.getString("verb"),
      actor = row.getString("actor"),
      published = new DateTime(row.getLong("published")),
      obj = Option(row.getString("object")),
      target = Option(row.getString("target")),
      foreign_id = Option(row.getString("foreign_id")),
      id = row.getUUID("activity_id")
    )
  }

  private def getClusterKey(activity: Activity): String = {
    activity.published.getMillis.toString + ";" + activity.id.toString
  }

  private def getClusterKey(published: DateTime, activity_id: UUID): String = {
    published.getMillis.toString + ";" + activity_id.toString
  }
}
