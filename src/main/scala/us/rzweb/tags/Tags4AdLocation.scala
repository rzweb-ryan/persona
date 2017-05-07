package us.rzweb.tags

import org.apache.commons.lang.StringUtils
import us.rzweb.beans.Log

/**
  * Created by RZ on 5/6/17.
  * make tags by ads' platform
  */
object Tags4AdLocation extends Tags{

  /**
    * AdlocationId => LC0(<10), "LC"(>=10), AdlocationType => "LN"
    * @param args log
    * @return Map
    */
  override def makeTags(args: Any*): Map[String, Int] = {
    var adLocalTagMap = Map[String, Int]()
    if (args.length == 1) {
      val log = args(0).asInstanceOf[Log]

      log.adspacetype match {
        case x if x < 10 => adLocalTagMap += ("LC0".concat(x.toString) -> 1)
        case x if x >= 10 => adLocalTagMap += ("LC".concat(x.toString) -> 1)
      }

      if(StringUtils.isNotEmpty(log.adspacetypename)) {
        adLocalTagMap += ("LN".concat(log.adspacetypename) -> 1)
      }
    }
    adLocalTagMap
  }
}
