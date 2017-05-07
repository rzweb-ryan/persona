package us.rzweb.tags

import org.apache.commons.lang.StringUtils
import us.rzweb.beans.Log

/**
  * Created by RZ on 4/6/17.
  * make tags by viewers' keywords
  */
object Tags4Keyword extends Tags{
  /**
    * keywords: K
    * @param args -> LOG
    * @return Map
    */
  override def makeTags(args: Any*): Map[String, Int] = {
    var keywordsMap = Map[String, Int]()
    if (args.length > 0) {
      val log = args(0).asInstanceOf[Log]

      if (StringUtils.isNotEmpty(log.keywords)) {
        val keys = log.keywords.split("\\|")
        keys.filter(e=> e.length >=3 && e.length <=8).map(t=>keywordsMap+=("K"+t -> 1))
      }
    }
    keywordsMap
  }
}
