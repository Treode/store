package com.treode.cluster.messenger

import com.treode.buffer.PagedBuffer
import com.treode.cluster.MailboxId
import com.treode.pickle.{Pickler, pickle}

private trait PickledMessage {

  def mbx: MailboxId
  def write (buffer: PagedBuffer)
}

private object PickledMessage {

  def apply [A] (p: Pickler [A], _mbx: MailboxId, msg: A): PickledMessage =
    new PickledMessage {
      def mbx = _mbx
      def write (buf: PagedBuffer) = pickle (p, msg, buf)
      override def toString = "PickledMessage" + (mbx, msg)
    }}
