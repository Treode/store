package com.treode.cluster

trait EphemeralPort [M] {

  def id: PortId
  def close()
}
