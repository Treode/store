package com.treode.store.disk2

import com.treode.pickle.Picklers

sealed abstract class LogHeader

object LogHeader {

  case object End extends LogHeader
  case class Continue (seg: Int) extends LogHeader
  case class Entry (time: Long, id: TypeId) extends LogHeader

  val pickle = {
    import Picklers._
    val typeId = TypeId.pickle
    tagged [LogHeader] (
        0x1 -> const (End),
        0x2 -> wrap (int) .build (Continue.apply _) .inspect (_.seg),
        0x3 -> wrap (fixedLong, typeId)
            .build ((Entry.apply _).tupled)
            .inspect (v => (v.time, v.id)))
  }}
