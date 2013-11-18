package com.treode.store.cluster.atomic

import com.treode.store.Bytes

trait AtomicTestTools {

  val Zero = Bytes (0)
  val One = Bytes (1)
  val Two = Bytes (2)

  implicit class TestableDeputy (s: Deputy) {

    def isRestoring = classOf [Deputy#Restoring] .isInstance (s.state)
    def isOpen = classOf [Deputy#Open] .isInstance (s.state)
    def isPrepared = classOf [Deputy#Prepared] .isInstance (s.state)
    def isCommitted = classOf [Deputy#Committed] .isInstance (s.state)
    def isAborted = classOf [Deputy#Aborted] .isInstance (s.state)
    def isShutdown = classOf [Deputy#Shutdown] .isInstance (s.state)
  }}
