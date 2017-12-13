/**
  * Created by artemgolovinskij on 13/12/2017.
  */
object FollowHandler {
  def follow(follower: String, following: String) = {
    DatabaseWrapper.putFollow(follower, following)
  }

}
