package com.treode.cluster.messenger

import com.treode.cluster.MailboxId
import com.treode.pickle.{Pickler, pickle}
import io.netty.buffer.ByteBuf

private trait PickledMessage {

  def mbx: MailboxId
  def write (b: ByteBuf)
}

private object PickledMessage {

  def apply [A] (p: Pickler [A], _mbx: MailboxId, msg: A): PickledMessage =
    new PickledMessage {
      def mbx = _mbx
      def write (buf: ByteBuf) = pickle (p, msg, buf)
      override def toString = "PickledMessage" + (mbx, msg)
    }}
