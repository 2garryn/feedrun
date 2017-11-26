import java.util.UUID

import org.joda.time.DateTime

object Feed extends App{
/*

  val contid = new ActivityContId(DateTime.now, UUID.randomUUID())
  val contid2 = new ActivityContId().fromStringId(contid.toStringId().getOrElse(""))
  val resultset = ActivityResultSet(contid)

  println(resultset)
  println(contid)
  println(contid.toStringId())
  println(contid2)
*/
/*
  ActivityManager.dispatch("artem", "myfeed1", Activity("2017", "dosome"))
  ActivityManager.dispatch("artem", "myfeed1", Activity("2017", "somet"))
  ActivityManager.dispatch("artem", "myfeed1", Activity("2017", "somet333asd"))

  val d1 = DateTime.now.minusYears(1)
  val d2 = DateTime.now.minusYears(2)

  ActivityManager.dispatch("artem", "myfeed1", Activity("2016", "dosome", published = d1))
  ActivityManager.dispatch("artem", "myfeed1", Activity("2016", "somet", published = d1))
  ActivityManager.dispatch("artem", "myfeed1", Activity("2016", "somet333asd", published = d1))

  ActivityManager.dispatch("artem", "myfeed1", Activity("2015", "dosome", published = d2))
  ActivityManager.dispatch("artem", "myfeed1", Activity("2015", "somet", published = d2))
  ActivityManager.dispatch("artem", "myfeed1", Activity("2015", "somet333asd", published = d2))
*/

  var LL = List[Activity]()
  val r = DatabaseWrapper.getActivities("artem", "myfeed1", ActivityContIdStart(), 20)
  for(s <- r.activities) {
    println(s.published, " ", s.id)
  }
  println(r.contid)



  println("----")
  var r2 = DatabaseWrapper.getActivities("artem", "myfeed1", ActivityContIdStart(), 5)
  for(s <- r2.activities) {
    println(s.published, " ", s.id)
  }

  LL = LL ++ r2.activities

  println(r2.contid)
  println("----")


  var r3 = DatabaseWrapper.getActivities("artem", "myfeed1", r2.contid, 5)
  for(s <- r3.activities) {
    println(s.published, " ", s.id)
  }
  LL = LL ++ r3.activities

  println(r3.contid)
  println("----")
  var r4 = DatabaseWrapper.getActivities("artem", "myfeed1", r3.contid, 5)

  for(s <- r4.activities) {
    println(s.published, " ", s.id)
  }
  LL = LL ++ r4.activities
  println(r4.contid.toStringId)
  println(r4.contid)


  println("----")
  var r5 = DatabaseWrapper.getActivities("artem", "myfeed1", r4.contid, 5)

  for(s <- r5.activities) {
    println(s.published, " ", s.id)
  }
  LL = LL ++ r5.activities
  println(r5.contid)



  println(r.activities == LL)



}
