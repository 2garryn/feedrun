import java.util.UUID

import akka.Done
import akka.stream.alpakka.cassandra.scaladsl.{CassandraSink, CassandraSource}
import akka.stream.scaladsl.Sink
import com.datastax.driver.core._
import com.google.common.util.concurrent.{FutureCallback, Futures}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}
import scala.util.Try
object DatabaseConnect {
  val host = ConfigHandler.getString("cassandra-contact-point")
  val port = ConfigHandler.getInt("cassandra-port")

  var prepStatementsMap = Map[String, PreparedStatement]()

  implicit val system = GlobalActorSystem.getActorSystem
  implicit val materializer = GlobalActorSystem.getMaterializer

  val poolingOptions: PoolingOptions =
      new PoolingOptions()
        .setConnectionsPerHost(HostDistance.LOCAL,  4, 10)
        .setConnectionsPerHost(HostDistance.REMOTE, 2, 4)

  implicit val session = Cluster
      .builder()
      .addContactPoint(host)
      .withPort(port)
      .withPoolingOptions(poolingOptions)
      .build()
      .connect()

  def initialize(keyspace: String) = {
    createKeyspace(keyspace)
    useKeyspace(keyspace)
  }

  def addPreparedStatementInsert(name: String, table: String, fields: List[String]) = {
    val placeholders = List.fill(fields.length)("?").mkString(",")
    val st = "INSERT INTO " + table + " (" + fields.mkString(",") + ") VALUES (" + placeholders+ ") IF NOT EXISTS"
    addPreparedStatement(name: String, st: String)
  }

  def addPreparedStatementDelete(name: String, table: String, pkfields: List[String]) = {
    val placeholders = pkfields.map(_ + "=?").mkString(" and ")
    val st = "DELETE FROM " + table + " WHERE " + placeholders
    addPreparedStatement(name: String, st: String)
  }


  private def addPreparedStatement(name: String, st: String) = {
    prepStatementsMap get name match {
      case None => {
        val preparedStatement = session.prepare(st)
        prepStatementsMap = prepStatementsMap + (name -> preparedStatement)
      }
      case _ => {
        throw new Exception("Prepared statement name " + name + " has been already used")
      }
    }
  }

  def getPreparedStatement(name: String): PreparedStatement = prepStatementsMap(name)

  def createKeyspace(keyspace: String) = {
    query("CREATE KEYSPACE IF NOT EXISTS " + keyspace + " WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};")
  }

  def useKeyspace(keyspace: String) = query("USE " + keyspace)

  def query(str: String): ResultSet = session.execute(str)
  def query(stat: Statement): ResultSet = session.execute(stat)

  def queryAsync(stat: Statement): Future[ResultSet] = {
    val res = session.executeAsync(stat)
    val p = Promise[ResultSet]()
    Futures.addCallback(res,
      new FutureCallback[ResultSet] {
        def onSuccess(r: ResultSet) = p success r
        def onFailure(t: Throwable) = p failure t
      })
    p.future
  }


  def createTable(name: String, fields: Map[String, String], partKey: Seq[String], clusterKey: Seq[String], orderField: String, order: String = "DESC" ) = {
    val strFields = fields.foldLeft[String]("") {(acc: String,  kv: (String, String)) =>
        val(key, value) = kv
        acc + key + " " + value + ", "
    }
    val strPartKey = if(partKey.length > 1) "(" + partKey.mkString(",") + ")" else partKey(0)
    val strClusterKey = if(clusterKey.length > 1)  clusterKey.mkString(", ")  else clusterKey(0)

    val query = "CREATE TABLE IF NOT EXISTS " + name + " (" + strFields + " " + "PRIMARY KEY (" + strPartKey + ", " + strClusterKey +
      ")) WITH CLUSTERING ORDER BY (" + orderField + " " + order + ");"
    println(query)
    session.execute(query)

  }

  def insert(name: String, values: Map[String, Any], ifNotExist: Boolean = true, ttl: Option[Int] = None, ts: Option[Int] = None) = {
    val converted = toStringPairs(values)
    val fields = converted.keys.mkString(",")
    val cvals  = converted.values.mkString(",")

    val strIfNotExist = ifNotExist match {
      case true => " IF NOT EXISTS"
      case false => ""
    }

    val strTtl = ttl match {
      case None => ""
      case Some(t) => "USING TTL " + t.toString
    }

    val strTs = ts match {
      case None => ""
      case Some(t) => " AND TIMESTAMP " + t.toString
    }

    val query = "INSERT INTO " + name + " (" + fields + ") VALUES (" + cvals + ") " + strTtl + strTs + strIfNotExist + ";"

    session.execute(query)
  }

  def delete(name: String, values: Map[String, Any]) = {
    val req = toStringPairs(values).map({kv: (String, String) => kv._1 + "=" + kv._2}).mkString(" and ")
    session.execute("DELETE FROM " + name + " WHERE " + req + ";")
  }

  def putActivitySinc[T](prepName: String,  binder: (T, PreparedStatement) => BoundStatement): Sink[T, Future[Done]] = {
    val preparedStatement = prepStatementsMap(prepName)
    CassandraSink[T](parallelism = 2, preparedStatement, binder)
  }



  def getDataFlow(query: String, fetchSize: Int, group: Int, asyncN: Int) (f: (Seq[Row]) => Unit) (done: () => Unit) = {
    val stmt = new SimpleStatement(query).setFetchSize(fetchSize)
    val source = CassandraSource(stmt)
      .grouped(group)
      .mapAsyncUnordered(asyncN)({
        batch => Future {
          f(batch)
        }
      }).runWith(Sink.ignore).onComplete({_: Try[Done] => done()})
  }


  def toStringPairs(values: Map[String, Any]): Map[String, String] = {
    values.collect ({
      case (k, i: UUID) => (k, i.toString)
      case (k, s: String) => (k, "'" + s + "'")
      case (k, i: Int) => (k, i.toString)
      case (k, i: Long) => (k, i.toString)
    //  case (k, None) => (k, None)
      case (k, Some(s: String)) => (k, "'" + s + "'")
      case (k, Some(i: Int)) => (k, i.toString)
      case (k, Some(i: Long)) => (k, i.toString)
    })//.collect({ case (k, Some(v)) => k -> v })
  }

}
