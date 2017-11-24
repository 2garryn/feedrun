import java.util.UUID

import com.datastax.driver.core._

object DatabaseConnect {

  val host = "localhost"
  val port = 32769
  //val keyspace = "feedrun2"

  val poolingOptions: PoolingOptions =
      new PoolingOptions()
        .setConnectionsPerHost(HostDistance.LOCAL,  4, 10)
        .setConnectionsPerHost(HostDistance.REMOTE, 2, 4)

  val session = Cluster
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

  def createKeyspace(keyspace: String) = {
    query("CREATE KEYSPACE IF NOT EXISTS " + keyspace + " WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};")

  }

  def useKeyspace(keyspace: String) = {
    query("USE " + keyspace)
  }

  def query(str: String) = {
    session.execute(str)
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

    val converted = values.map ({
        case (k, i: UUID) => (k, i.toString)
        case (k, s: String) => (k, "'" + s + "'")
        case (k, i: Int) => (k, i.toString)
        case (k, i: Long) => (k, i.toString)
        case (k, None) => (k, None)
        case (k, Some(s: String)) => (k, "'" + s + "'")
        case (k, Some(i: Int)) => (k, i.toString)
        case (k, Some(i: Long)) => (k, i.toString)
    }).filter({ case (k, v)=> v != None })

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
    println(query)

    session.execute(query)
  }
}
