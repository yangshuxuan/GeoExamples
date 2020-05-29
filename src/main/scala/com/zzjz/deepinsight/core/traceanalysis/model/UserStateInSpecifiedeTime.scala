package com.zzjz.deepinsight.core.traceanalysis.model

import com.zzjz.deepinsight.core.traceanalysis.model.TimeDirection.TimeDirection
import com.zzjz.deepinsight.core.traceanalysis.model.userstate.{Normal, Uncertain, UserState}
import org.joda.time.Interval

case class UserStateInSpecifiedeTime(userId:String,
                                     interval:Interval,
                                     timeDirection:TimeDirection = TimeDirection.Ago,
                                     howManyCompares:Int = 7,
                                     userState:UserState = Uncertain("not start to find abnormal behavior")){
  def extendInterval():List[Interval]={
    val end = interval.getEndMillis
    val start = interval.getStartMillis
    val duration = interval.toDurationMillis

    val t = if(timeDirection == TimeDirection.Ago){
      -howManyCompares to -1
    }else if(timeDirection == TimeDirection.Current){
      val mid  = howManyCompares / 2 + 1
      (1 until mid) ++ (-mid to -1)

    } else {
      1 to howManyCompares
    }

    t.map(v => interval.withStartMillis(start + v * duration).withEndMillis(end + v * duration)).toList
  }
}
