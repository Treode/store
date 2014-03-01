package com.treode.store

import com.treode.cluster.MailboxId

package object catalog {

  private [catalog] type Ping = Seq [(MailboxId, Int)]
  private [catalog] type Patches = Seq [Bytes]
  private [catalog] type Value = (Int, Bytes, Patches)
  private [catalog] type Update = Either [Value, (Int, Patches)]
  private [catalog] type Sync = Seq [(MailboxId, Update)]

  private [catalog] val catalogChunkSize = 16
  private [catalog] val catalogHistoryLimit = 16

  def isEmpty (update: Update): Boolean =
    update match {
      case Left (_) => false
      case Right ((_, diffs)) => diffs.isEmpty
    }}
