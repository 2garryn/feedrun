object DatabaseWrapper {
  val keyspace = "feedrun234"
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
      "published" -> "decimal",
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
/*
  def createActivityByIdTable(activityTable: String) = {
    val fields = Map(
      "activity_id" -> "uuid",
      "username" -> "text",
      "feedname" -> "text",
      "year" -> "bigint"
    )
    val partKey = Seq("username", "feedname", "year")
    val clusterKey = Seq("published", "activity_id")
    DatabaseConnect.createTable(activityTable, fields, partKey, clusterKey, "published", "desc")
  }
*/

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



}
