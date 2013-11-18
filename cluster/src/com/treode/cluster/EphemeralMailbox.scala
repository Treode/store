package com.treode.cluster

trait EphemeralMailbox [M] {

  def id: MailboxId
  def close()
  def receive (receiver: (M, Peer) => Any)
  def whilst (condition: => Boolean) (receiver: (M, Peer) => Any)
}
