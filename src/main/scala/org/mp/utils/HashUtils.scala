package org.mp.utils

import java.security.MessageDigest

/**
  * Created by MP on 16/1/31.
  */
object HashUtils extends Serializable {
  def hash2MD5(text: String): String = {
    val digest = MessageDigest.getInstance("MD5")
    digest.digest(text.getBytes).map("%02x".format(_)).mkString
  }
}
