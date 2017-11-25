import java.util.UUID

import org.joda.time.DateTime

object Feed extends App{


  val contid = new ActivityContId(DateTime.now, UUID.randomUUID())
  val contid2 = new ActivityContId().fromStringId(contid.toStringId().getOrElse(""))
  println(contid)
  println(contid.toStringId())
  println(contid2)

/*
  ActivityManager.dispatch("artem", "myfeed", Activity("myactor", "dosome"))
  ActivityManager.dispatch("artem", "myfeed", Activity("myactor1", "somet"))
  ActivityManager.dispatch("artem", "myfeed", Activity("blah", "somet333asd"))
*/
  val r = DatabaseWrapper.getActivities("artem", "myfeed")
  println(r)



}
