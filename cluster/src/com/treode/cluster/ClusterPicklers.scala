package com.treode.cluster

import com.treode.pickle.Picklers

private trait ClusterPicklers extends Picklers {

  def hostId = HostId.pickler
  def mbxId = MailboxId.pickler
}

private object ClusterPicklers extends ClusterPicklers
