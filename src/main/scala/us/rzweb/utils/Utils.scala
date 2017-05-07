package us.rzweb.utils

import java.util.regex.Pattern

import org.apache.commons.lang.StringUtils

/**
  * Created by RZ on 5/5/17.
  */
object Utils {
  def parseInt(s: String): Int = {
    try {
      s.isEmpty match {
        case false => 0
        case true => s.toInt
      }
    } catch {
      case  _: Exception => 0
    }
  }

  def parseDouble(s: String): Double = {
    try {
      s.isEmpty match {
        case false => 0
        case true => s.toDouble
      }
    } catch {
      case _: Exception => 0
    }
  }

  /**
    * return date string format in "yyyymmdd"
    * @param s
    * @return
    */
  def fmtDate(s: String): Option[String] = {
    try {
      if (StringUtils.isNotEmpty(s)) {
        val split = s.split(" ")
        if (split.length > 1) {
          Some(split(0).replace("-",""))
        } else None
      } else None
    } catch {
      case _:Exception => None
    }
  }

  /**
    * return hour string format int "HH"
    * @param s
    * @return
    */
  def fmtHour(s: String): Option[String] = {
    try {
      if (StringUtils.isNotEmpty(s)) {
        val split = s.split(" ")
        if (split.length > 1) {
          Some(split(2).substring(0, 2))
        } else None
      } else None
    } catch {
      case _:Exception => None
    }
  }


  /**
    * format imei into standard imei string
    * @param imei
    * @return
    */
  def formatIMEID(imei: String): String = {
    var imeiId = ""
    val imeiNum = if (isAllNumber(imei) && (imei.length == 15 || imei.length == 14)) imei else ""
    if (imeiNum.length == 14) {
      imeiId = imeiNum + getMeid15(imei)
    } else if (imeiNum.length == 15) {
      imeiId = imeiNum
    }
    val IMEILet = if (Utils.containsLetter(imei) && (imei.length == 15 || imei.length == 14)) imei else ""
    if (IMEILet.length == 14) {
      imeiId = formatMeid(IMEILet).toUpperCase
    } else if (IMEILet.length == 15) {
      imeiId = IMEILet
    }
    imeiId
  }

  private def isAllNumber(str: String) = Pattern.compile("[0-9]*").matcher(str).matches()

  private def isContainNumber(str: String) = Pattern.compile("[0-9]*").matcher(str).find()

  private def isLetterOrDigit(str: String) = Pattern.compile("[a-zA-Z0-9]*").matcher(str).matches()

  private def isLetter(str: String) = Pattern.compile("[a-zA-Z]+").matcher(str).matches()

  private def containsLetter(str: String) = Pattern.compile("[a-zA-Z]+").matcher(str).find()


  private def isHexadecimal(str: String) = Pattern.compile("[a-fA-F0-9]*").matcher(str).matches()

  /**
    * format MEID into standard
    * @param meid
    * @return
    */
  private def formatMeid(meid: String): String = {
    val dxml: Int = meid.length
    if (dxml != 14 && dxml != 16) {
      return meid
    }
    var meidRes: String = ""
    if (dxml == 14 && isHexadecimal(meid)) {
      meidRes = meid + getMeid15(meid)
    } else {
      meidRes = meid
    }
    if (dxml == 16) {
      meidRes = meid.substring(2) + getMeid15(meid.substring(2))
    }
    meidRes
  }

  /**
    * get 15th digit of meid
    * MEID校验码算法：
    * (1).将偶数位数字分别乘以2，分别计算个位数和十位数之和，注意是16进制数
    * (2).将奇数位数字相加，再加上上一步算得的值
    * (3).如果得出的数个位是0则校验位为0，否则为10(这里的10是16进制)减去个位数
    * 如：AF 01 23 45 0A BC DE 偶数位乘以2得到F*2=1E 1*2=02 3*2=06 5*2=0A A*2=14 C*2=18 E*2=1C,
    * 计算奇数位数字之和和偶数位个位十位之和，得到 A+(1+E)+0+2+2+6+4+A+0+(1+4)+B+(1+8)+D+(1+C)=64
    * 校验位 10-4 = C
    *
    * @param meid
    * @return
    */
  private def getMeid15(meid: String): String = {
    if (meid.length == 14) {
      val myStr = Array("a", "b", "c", "d", "e", "f")
      var sum = 0
      for (i <- 0 to meid.length - 1) {
        var param = meid.substring(i, i + 1)

        for (j <- myStr.indices) {
          if (param.equalsIgnoreCase(myStr(j))) {
            param = "1" + String.valueOf(j)
          }
        }

        if (i % 2 == 0) {
          sum = sum + param.toInt
        }
        else {
          sum = sum + 2 * param.toInt % 16
          sum = sum + 2 * param.toInt / 16
        }
      }

      if (sum % 16 == 0) {
        "0"
      }
      else {
        var result = 16 - sum % 16
        if (result > 9) {
          result += 65 - 10
        }
        result + ""
      }
    }
    else {
      ""
    }
  }


}
