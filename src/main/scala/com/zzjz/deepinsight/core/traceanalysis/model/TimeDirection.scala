package com.zzjz.deepinsight.core.traceanalysis.model

object TimeDirection  extends Enumeration {
  type TimeDirection = Value



  val Ago = Value("ago")
  val Current   = Value("current")
  val Future   = Value("future")

}