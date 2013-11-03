package com.treode.cluster.messenger

import com.treode.cluster.MailboxId
import com.treode.pickle.{Buffer, Pickler, pickle}

private trait PickledMessage {

  def mbx: MailboxId
  def write (buffer: Buffer)
}

private object PickledMessage {

  def apply [A] (p: Pickler [A], _mbx: MailboxId, msg: A): PickledMessage =
    new PickledMessage {
      def mbx = _mbx
      def write (buf: Buffer) = pickle (p, msg, buf)
      override def toString = "PickledMessage" + (mbx, msg)
    }}
