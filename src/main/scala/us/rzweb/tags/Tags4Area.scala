package us.rzweb.tags

import us.rzweb.beans.Log

/**
  * Created by RZ on 4/6/17.
  * make tags via views' location
  */
object Tags4Area extends Tags{

  /**
    * ZP:province, ZC: city
    * @param args
    * @return Map[String, Int]
    */
  override def makeTags(args: Any*): Map[String, Int] = {
    var map = Map[String, Int]()
    if (args.length == 1) {
      val log = args(0).asInstanceOf[Log]
      if(log.cityname.nonEmpty) {
        map += ("ZP" + log.cityname -> 1)
      }
      if(log.provincename.nonEmpty) {
        map += ("ZC" + log.provincename -> 1)
      }
    }
    map
  }
}
