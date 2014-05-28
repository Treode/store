package com.treode.store

import com.treode.async.misc.EpochReleaser

private class Library {

  val releaser = new EpochReleaser

  var atlas: Atlas = Atlas.empty

  var residents: Residents = Residents.all
}
