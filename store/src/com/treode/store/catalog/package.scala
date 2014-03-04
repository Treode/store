package com.treode.store

import com.treode.cluster.MailboxId

package object catalog {

  private [catalog] type Ping = Seq [(MailboxId, Int)]
  private [catalog] type Sync = Seq [(MailboxId, Update)]

  private [catalog] val catalogChunkSize = 16
  private [catalog] val catalogHistoryLimit = 16
}
