package com.zjlp.face.spark.bean

import scala.beans.BeanProperty

/**
 * @param userId 用户（商户）id
 * @param `type` 关系类型 1：一度好友 2：二度好友  3：陌生人
 */
class PersonRelation(@BeanProperty var userId: String, @BeanProperty var `type`: Int) {
  def this() {
    this("",0)
  }
  override def toString: String = s"($userId,${`type`})"
}
