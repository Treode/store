package com.treode.cluster

import scala.language.implicitConversions
import scala.util.Random
import com.treode.pickle.Picklers

class MailboxId (val id: Long) extends AnyVal {

  def isFixed = MailboxId.isFixed (id)
  override def toString = f"Mailbox:$id%016X"
}

object MailboxId {

  private val fixed = 0xFF00000000000000L

  implicit def apply (id: Long): MailboxId =
    new MailboxId (id)

  def isFixed (id: MailboxId): Boolean =
    (id.id & fixed) == fixed

  def isEphemeral (id: MailboxId): Boolean =
    (id.id & fixed) != fixed

  def newEphemeral(): MailboxId = {
    var id = Random.nextLong()
    while ((id & fixed) == fixed)
      id = Random.nextLong
    new MailboxId (id)
  }

  val pickle = {
    import Picklers._
    wrap (fixedLong) build (apply _) inspect (_.id)
  }}
