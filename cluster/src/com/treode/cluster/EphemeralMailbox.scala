package com.treode.cluster

trait EphemeralMailbox [M] {

  def id: MailboxId
  def close()
}
