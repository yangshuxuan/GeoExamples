package com.zzjz.deepinsight.core.traceanalysis.model.userstate

abstract sealed class UserState {

}
case class Normal(cause:String) extends UserState{
  override def toString: String =  s"Normal:${cause}"
}
case class Abnormal(cause:String) extends UserState{
  override def toString: String = s"Abnormal:${cause}"
}
case class Uncertain(cause:String) extends UserState{
  override def toString: String = s"Uncertain:${cause}"
}
