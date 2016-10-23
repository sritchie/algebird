package com.twitter.algebird

import org.scalatest.{ PropSpec, Matchers }
import org.scalatest.prop.PropertyChecks
import org.scalatest.prop.Checkers.check
import org.scalacheck.{ Gen, Arbitrary }
import Arbitrary.arbitrary

class RingBufLaws extends PropSpec with PropertyChecks with Matchers {
  import BaseProperties._

  case class RingBufParams(size: Int)

  implicit def rbMonoid[A: Monoid](implicit params: RingBufParams): Monoid[RingBuf[A]] =
    RingBuf.monoid(params.size)

  implicit def arbitraryRingBuf[A: Arbitrary](implicit params: RingBufParams): Arbitrary[RingBuf[A]] =
    Arbitrary(for {
      vect <- Gen.buildableOfN[Vector[A], A](params.size, arbitrary[A])
      i <- arbitrary[Short].map(i => (i & 0xffff) % params.size)
    } yield RingBuf(vect, RingBuf.Idx(i)))

  def runProperties[A: Arbitrary: Equiv: Monoid](implicit p: RingBufParams): Unit =
    check(monoidLawsEquiv[RingBuf[A]])

  property("test Monoid[RingBuf[Int]] #1") {
    implicit val p = RingBufParams(100)
    runProperties[Int]
  }

  property("test Monoid[RingBuf[BigInt]] #2") {
    implicit val p = RingBufParams(123)
    runProperties[BigInt]
  }
}
