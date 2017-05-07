package us.rzweb.utils

import us.rzweb.beans.Log

/**
  * Created by RZ on 3/9/17.
  */
object ReportUtils {
  /**
    * mode == request(code == 1),processnode == valid(1), validRequest(2),validForResponse(3)
    *
    * @param log
    * @return List(valid, validRequest, validForResponse)
    */
  def calculateRequestTimes(log: Log): List[Double] = {
    if (log.requestmode == 1) {
      log.processnode match {
        case 1 => List(1, 0, 0)
        case 2 => List(1, 1, 0)
        case 3 => List(1, 1, 1)
        case _ => List(0, 0, 0)
      }
    } else List(0, 0, 0)
  }

  /**
    * valid response times calculate
    *
    * @param log
    * @return List(bid, win)
    */
  def calculateResponseTimes(log: Log): List[Double] = {
    if (log.adplatformproviderid >= 100000 && log.iseffective == 1 && log.isbilling == 1) {
      if (log.isbid == 1) return List(1, 0)
      else if (log.iswin == 1) return List(0, 1)
    }
    List(0, 0)
  }


  /**
    * calculate Ads been validly pushed, shown or clicked by audience
    *
    * @param log
    * @return List(shown, clicked)
    */
  def calculatePushTimes(log: Log): List[Double] = {
    if (log.iseffective == 1) {
      if (log.requestmode == 2) {
        //shown
        return List(1, 0)
      } else if (log.requestmode == 3) {
        return List(0, 1)
      }
    }
    List(0, 0)
  }

  /**
    * calculate ad payment & costs
    * @param log
    * @return List(cost, payment)
    */
  def calculateCosts(log: Log): List[Double] = {
    if (log.adplatformproviderid >= 10000
      && log.iseffective == 1
      && log.isbilling ==1
      && log.iswin == 1
      && log.adorderid > 200000
      && log.adcreativeid > 200000) List(log.winprice/1000.0, log.adpayment/1000.0)
    else List(0.0, 0.0)
  }
}
