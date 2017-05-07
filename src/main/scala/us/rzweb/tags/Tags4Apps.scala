package us.rzweb.tags

import org.apache.commons.lang.StringUtils
import us.rzweb.beans.Log

/**
  * Created by RZ on 4/6/17.
  * make tags by viewers' client app
  */
object Tags4Apps extends Tags{

  /**
    * APP
    * @param args(0)->log, args(1)->appDictionary
    * @return
    */
  override def makeTags(args: Any*): Map[String, Int] = {
    var map = Map[String, Int]()
    if(args.length == 2) {
      val log = args(0).asInstanceOf[Log]
      val appDic = args(1).asInstanceOf[Map[String, String]]
      val appName = appDic.getOrElse(log.appid, log.appname)
      if(StringUtils.isNotEmpty(appName)) map += ("APP" + appName -> 1)
    }
    map
  }
}
