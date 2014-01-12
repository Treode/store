package com.treode.cluster

import scala.language.implicitConversions

import com.treode.pickle.Picklers

class MailboxId (val id: Long) extends AnyVal {
  import MailboxId.fixed

  def isFixed = (id & fixed) == fixed

  override def toString = f"Mailbox:$id%016X"
}

object MailboxId {

  private val fixed = 0xFF00000000000000L

  implicit def apply (id: Long): MailboxId =
    new MailboxId (id)

  val pickle = {
    import Picklers._
    wrap (fixedLong) build (apply _) inspect (_.id)
  }}
