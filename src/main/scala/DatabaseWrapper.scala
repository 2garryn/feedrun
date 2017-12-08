import java.util.UUID

import akka.Done
import akka.stream.scaladsl.Source
import com.datastax.driver.core.{PreparedStatement, ResultSet, Row}
import com.datastax.driver.core.querybuilder.{QueryBuilder, Select}
import org.joda.time.DateTime

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.util.Try


object DatabaseWrapper {
  val keyspace = "mybase2"
  val activityTable = "activities"
  val followTable = "follow"
  val startYear = 2015

  val insertName = "insertActivity"


  implicit val system = GlobalActorSystem.getActorSystem
  implicit val materializer = GlobalActorSystem.getMaterializer

  DatabaseConnect.initialize(keyspace)
  createTables()
  addPrepareStatements()

  def createTables() = {
    createActivityTable(activityTable)
    createFollowTable(followTable)
  }

  def addPrepareStatements() = {
    val insertActivityFields = List("actor","username","published","year","cluster_key","activity_id","feedname","verb")
    DatabaseConnect.addPreparedStatement(insertName, activityTable, insertActivityFields)
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

  def createFollowTable(followTable: String) = {
    val fields = Map(
      "username" -> "text",
      "follow_type" -> "text",
      "actor" -> "text"
    )
    val partKey = Seq("username", "follow_type")
    val clusterKey = Seq("actor")
    DatabaseConnect.createTable(followTable, fields, partKey, clusterKey, "actor", "asc")
  }

  def putFollow(following: String, follower: String) = {
    DatabaseConnect.insert(followTable, Map(
      "username" -> following,
      "follow_type" -> "follower",
      "actor" -> follower
    ))
    DatabaseConnect.insert(followTable, Map(
      "username" -> follower,
      "follow_type" -> "following",
      "actor" -> following
    ))
  }


  def putActivity(username: String, feedname: String, activity: Activity) = {
    val year = new DateTime(activity.published).getYear()
    val values = Map(
      "username" -> username,
      "feedname" -> feedname,
      "activity_id" -> activity.id,
      "published" -> activity.published,
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



  def putActivitiesAsync(containers: List[KafkaActivityContainer])(onComplete: Try[Done] => Unit) = {
    case class SContainer(follower: String, targetFeed: String, activity: Activity)
    implicit val ec =  scala.concurrent.ExecutionContext.Implicits.global
    val binder = (c: SContainer, statement: PreparedStatement) => {
        statement.bind(
          c.activity.actor,
          c.follower,
          Long.box(c.activity.published),
          Long.box(new DateTime(c.activity.published).getYear),
          getClusterKey(c.activity),
          c.activity.id,
          c.targetFeed,
          c.activity.verb
        )
    }
    val sink = DatabaseConnect.putActivitySinc[SContainer](insertName, binder)
    Source(containers).mapConcat[SContainer]({
      container: KafkaActivityContainer =>
        container.followers.toList.map(
          follower => SContainer(follower, container.targetFeed, container.activity
          )
        )
    }).runWith(sink).onComplete(onComplete)
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
        doGetActivities(username, feedname, new DateTime(contid.published).getYear - 1, limit, res.length, res)
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

  // TODO: add setFetchSize to requests (read doc)

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
      .and(QueryBuilder.eq("year", new DateTime(contid.published).getYear))
      .and(QueryBuilder.lt("cluster_key", getClusterKey(contid.published, contid.activity_id)))
      .limit(limit)
  }

  private def mapRowToActivity(row: Row): Activity = {
    Activity(
      verb = row.getString("verb"),
      actor = row.getString("actor"),
      published = row.getLong("published"),
      obj = Option(row.getString("object")),
      target = Option(row.getString("target")),
      foreign_id = Option(row.getString("foreign_id")),
      id = row.getUUID("activity_id")
    )
  }

   def getClusterKey(activity: Activity): String = {
    activity.published.toString + ";" + activity.id.toString
  }

   def getClusterKey(published: Long, activity_id: UUID): String = {
    published.toString + ";" + activity_id.toString
  }
}
