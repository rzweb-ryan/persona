package us.rzweb.tags

/**
  * Created by RZ on 4/6/17.
  * make tags for user access logs
  */
trait Tags {
  def makeTags(args:Any*):Map[String, Int]
}
