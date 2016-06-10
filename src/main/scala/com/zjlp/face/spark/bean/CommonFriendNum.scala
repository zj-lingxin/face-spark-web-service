package com.zjlp.face.spark.bean

import scala.beans.BeanProperty

/**
 * @param username 用户（商户）username
 * @param commFriendNum 共同好友数
 */
class CommonFriendNum(@BeanProperty var username: String, @BeanProperty var commFriendNum: Int) extends scala.Serializable {
  def this() {
    this("",0)
  }
  override def toString: String = s"($username,$commFriendNum)"
}

