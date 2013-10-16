package com.treode.cluster.messenger

import com.esotericsoftware.kryo.io.Output
import com.treode.cluster.MailboxId
import com.treode.pickle.{Pickler, pickle}

private trait PickledMessage {

  def mbx: MailboxId
  def write (out: Output)
}

private object PickledMessage {

  def apply [A] (p: Pickler [A], _mbx: MailboxId, msg: A): PickledMessage =
    new PickledMessage {
      def mbx = _mbx
      def write (out: Output) = pickle (p, msg, out)
      override def toString = "PickledMessage" + (mbx, msg)
    }}
