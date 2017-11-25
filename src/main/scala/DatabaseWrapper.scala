import java.util.UUID

import com.datastax.driver.core.{ResultSet, Row}
import com.datastax.driver.core.querybuilder.QueryBuilder
import org.joda.time.DateTime

import scala.collection.mutable.ListBuffer

object DatabaseWrapper {
  val keyspace = "mybase1"
  val activityTable = "activities"
  //val activityByIdTable = "activity_by_id"


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
      "year" -> "bigint",
      "actor" -> "text",
      "verb" -> "text",
      "object" -> "text",
      "target" -> "text",
      "foreign_id" -> "text")
    val partKey = Seq("username", "feedname", "year")
    val clusterKey = Seq("published", "activity_id")
    DatabaseConnect.createTable(activityTable, fields, partKey, clusterKey, "published", "desc")
  }


  def putActivity(username: String, feedname: String, activity: Activity) = {
    //val UUID = activity.id.toString()
    val year = activity.published.getYear()
    val published = activity.published.getMillis()
    val values = Map(
      "username" -> username,
      "feedname" -> feedname,
      "activity_id" -> activity.id,
      "published" -> published,
      "year" -> year,
      "actor" -> activity.actor,
      "object" -> activity.obj,
      "verb" -> activity.verb,
      "target" -> activity.target,
      "foreign_id" -> activity.foreign_id
    )

    DatabaseConnect.insert(activityTable, values)
  }


  def getActivities(username: String, feedname: String, limit: Int = 10): List[Activity] = {
    val query = QueryBuilder
      .select()
      .from(activityTable)
      .where(QueryBuilder.eq("username", username))
      .and(QueryBuilder.eq("feedname", feedname))
      .and(QueryBuilder.eq("year", 2017)).limit(limit)
    val rs = DatabaseConnect.query(query)
    rawResultsToActivities(rs)
  }



  def rawResultsToActivities(rs: ResultSet): List[Activity] = {
    var ls = ListBuffer[Activity]()
    rs.all().forEach({ r: Row =>
      ls += mapRowToActivity(r)
    })
    ls.toList
  }

  def mapRowToActivity(row: Row): Activity = {
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
}
