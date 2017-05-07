package us.rzweb.tags

import org.apache.commons.lang.StringUtils
import us.rzweb.beans.Log

/**
  * Created by RZ on 4/6/17.
  * make tags by ads' channel
  */
object Tags4Channel extends Tags{

  /**
    * channel:CN
    * @param args(0)->Log
    * @return Map
    */
  override def makeTags(args: Any*): Map[String, Int] = {
    var channelMap = Map[String, Int]()

    if (args.length > 0) {
      val log = args(0).asInstanceOf[Log]

      if (StringUtils.isNotEmpty(log.adplatformproviderid.toString)) {
        channelMap += ("CN"+log.adplatformproviderid -> 1)
      }
    }

    channelMap
  }
}
