package com.twitter.algebird

import org.scalatest.{ PropSpec, Matchers }
import org.scalatest.prop.PropertyChecks
import org.scalatest.prop.Checkers.check
import org.scalacheck.{ Gen, Arbitrary }
import Arbitrary.arbitrary

// Shell for RingBuf laws.
class RingBufLaws extends PropSpec with PropertyChecks with Matchers {
  import BaseProperties._
}
