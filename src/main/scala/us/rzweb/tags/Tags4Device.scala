package us.rzweb.tags

import us.rzweb.beans.Log

/**
  * Created by RZ on 4/6/17.
  * make tags by viewers' client device
  */
object Tags4Device extends Tags{

  /**
    *
    * @param args(0)-> log, args(1) -> deviceDictionary
    * @return
    */
  override def makeTags(args: Any*): Map[String, Int] = {
    var deviceMap = Map[String, Int]()

    if (args.length > 1) {
      val log = args(0).asInstanceOf[Log]

      val deviceDict = args(1).asInstanceOf[Map[String, String]]

      val osOption: Option[String] = deviceDict.get(log.client.toString)
      if (osOption.nonEmpty) deviceMap += (osOption.get -> 1)
      else {
        deviceMap += (deviceMap.get("4").get.toString -> 1)
      }

      // 联网方式
      val network = deviceDict.getOrElse(log.networkmannername, deviceDict.get("NETWORKOTHER").get)
      deviceMap += (network -> 1)


      // 运营商
      val isp = deviceDict.getOrElse(log.ispname, deviceDict.get("OPERATOROTHER").get)

      deviceMap += (isp -> 1)
    }

    deviceMap
  }
}
