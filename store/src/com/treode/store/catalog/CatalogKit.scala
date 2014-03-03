package com.treode.store.catalog

import com.treode.async.Scheduler
import com.treode.cluster.Cluster
import com.treode.disk.Disks
import com.treode.store.Catalogs

private [store] object CatalogKit {

  def recover () (implicit
      scheduler: Scheduler,
      cluster: Cluster,
      recovery: Disks.Recovery): Catalogs.Recovery =
    new RecoveryKit
}
