package com.twitter.algebird

import org.scalatest.{ PropSpec, Matchers }
import org.scalatest.prop.PropertyChecks
import org.scalatest.prop.Checkers.check
import org.scalacheck.{ Gen, Arbitrary }
import Arbitrary.arbitrary

class ExpHistLaws extends PropSpec with PropertyChecks with Matchers {
  import BaseProperties._

  def pos(i: Long): Long = if (i == Long.MinValue) Long.MaxValue else Math.abs(i)

  // The example in the paper is actually busted, based on his
  // algorithm. He says to assume that k/2 == 2, but then he promotes
  // as if k/2 == 1, ie k == 2.
  property("example from paper") {
    val e = ExpHist.empty(2, 100)
    val plus76 = e.add(76, 0)

    val inc = plus76.inc(0)
    val incinc = inc.add(2, 0)

    plus76.windows shouldBe Vector(32, 16, 8, 8, 4, 4, 2, 1, 1)
    inc.windows shouldBe Vector(32, 16, 8, 8, 4, 4, 2, 2, 1)
    incinc.windows shouldBe Vector(32, 16, 16, 8, 4, 2, 1)
  }

  property("add and inc should generate the same results") {
    forAll { (x: Short) =>
      // Currently SUPER SLOW!
      val i = x & 0xfff
      val timestamp = 0L
      val e = ExpHist.empty(20, 100)

      val incs = (0L until i).foldLeft(e) {
        case (acc, _) => acc.inc(timestamp)
      }

      val adds = e.add(i, timestamp)

      incs.slowTotal shouldEqual adds.slowTotal
      incs.lowerBoundSum shouldEqual adds.lowerBoundSum
      incs.upperBoundSum shouldEqual adds.upperBoundSum
    }
  }

  property("l-canonical representation round-trips") {
    forAll { (x: Long, kIn: Short) =>
      // i and k must be positive, > 0.
      val i = (x & 0xfffffff) + 1
      val k = (kIn & 0xfffff) + 1
      ExpHist.expand(ExpHist.lNormalize(i, k)) shouldEqual i
    }
  }

  property("all i except last have either k/2, k/2 + 1 buckets") {
    forAll { (x: Long, kIn: Short) =>
      // i and k must be positive, > 0.
      val i = (x & 0xfffffff) + 1
      val k = (kIn & 0xfffff) + 1

      val lower = ExpHist.minBuckets(k)
      val upper = ExpHist.maxBuckets(k)

      ExpHist.lNormalize(i, k).init.forall { numBuckets =>
        lower <= numBuckets && numBuckets <= upper
      } shouldBe true
    }
  }
}
