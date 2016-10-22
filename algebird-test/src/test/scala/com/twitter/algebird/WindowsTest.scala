package com.twitter.algebird

import org.scalatest.{ PropSpec, Matchers }
import org.scalatest.prop.PropertyChecks
import org.scalatest.prop.Checkers.check
import org.scalacheck.{ Gen, Arbitrary }
import Arbitrary.arbitrary

class WindowsLaws extends PropSpec with PropertyChecks with Matchers {

  import BaseProperties._

  case class WindowsParams(step: Int, numSteps: Int) {
    def totalSize: Int = numSteps + 1
  }

  implicit def windowsMonoid[A: Monoid](implicit w: WindowsParams): Monoid[Windows[A]] =
    Windows.monoid(w.step, w.numSteps)

  implicit def arbitraryWindows[A: Arbitrary](implicit w: WindowsParams): Arbitrary[Windows[A]] =
    Arbitrary(for {
      vect <- Gen.buildableOfN[Vector[A], A](w.totalSize, arbitrary[A])
      n <- arbitrary[Short].map(_ & 0xffff)
      i <- arbitrary[Short].map(i => (i & 0xffff) % w.totalSize)
    } yield Windows(RingBuf(vect, i), w.step, (n * w.totalSize + i) * w.step))

  def runProperties[A: Arbitrary: Equiv: Monoid](implicit w: WindowsParams): Unit =
    check(monoidLawsEquiv[Windows[A]])

  // property("failing case") {
  //   implicit val w = WindowsParams(100, 10)
  //   val m = windowsMonoid[Int]
  //
  //   //val arg0 = Windows(Vector(1032478800, -1, 1, -1830505412, 598145662, -2147483648, -1525890587, -1764787492, 863764197, 1490172142, 427417516),0,100,0)
  //
  //   //val arg0 = Windows(Vector(2147483647, 1404594962, -1181511465, 1902416464, 1, 1, -886569095, 1, 0, -809395231, -763138612),1,100,59973200)
  //
  //   val arg0 = Windows(Vector(-731477864, -271003139, 1, 227212362, 1735155773, -2147483648, 1496978553, -95735355, 2147483647, -1957555367, -1460422666),5,100,425100)
  //
  //   val sum = (m.zero + arg0)
  //   println((arg0.step, arg0.time, arg0.iterator.toList))
  //   println((sum.step, sum.time, sum.iterator.toList))
  //   Equiv[Windows[Int]].equiv(sum, arg0) shouldBe true
  // }

  property("test Monoid[Windows[Int]] #1") {
    implicit val w = WindowsParams(100, 10)
    runProperties[Int]
  }

  property("test Monoid[Windows[BigInt]] #2") {
    implicit val w = WindowsParams(13, 13)
    runProperties[BigInt]
  }

  property("Windows of different sizes should add") {
    val l = Windows.empty[Int](100, 10).add(13, 95).add(12, 103).add(3, 105)
    val l2 = Windows.empty[Int](100, 5).add(13, 95).add(12, 103).add(3, 105)
    val r = Windows.empty[Int](100, 5).add(2, 95)
    Equiv[Windows[Int]].equiv(l + r, l2 + r) shouldBe true
  }

  // [info]       arg0 = (-238125844828599004,0), // 2 shrinks
  // [info]       arg1 = Vector((-1327071966132228755,0)) // 3 shrinks
  // property("works correctly") {
  //   forAll { (e0: (Long, Int), events0: Vector[(Long, Int)]) =>
  //     val events = (e0 +: events0).map { case (t0, x) => (t0 & 0x7fffffffffffL, x) }.sorted
  //     val t0 = events.head._1
  //     val lastT = events.last._1
  //     val span = lastT - t0

  //     val StepSize = ((span / 100) max 1).toInt
  //     val NumSteps = 20
  //     val WindowSize = NumSteps * StepSize

  //     val empty: Windows[Int] = Windows.empty[Int](StepSize, NumSteps)
  //     val windows = events.foldLeft(empty) { case (w, (t, x)) => w.add(x, t) }
  //     val realSum = events.iterator.collect { case (t, x) if t > lastT - WindowSize => x }.sum

  //     windows.lowerBoundSum should be <= realSum
  //     windows.upperBoundSum should be >= realSum
  //   }
  // }

  def testCase(e0: (Long, Int), events0: Vector[(Long, Int)]): Unit = {
    val events = (e0 +: events0).map { case (t0, x) => (t0 & 0x7fffffffffffL, x) }.sorted
    val t0 = events.head._1
    val lastT = events.last._1
    val span = lastT - t0

    val StepSize = (((span / 100) max 1) min 1000000).toInt
    val NumSteps = 20
    val WindowSize = NumSteps * StepSize

    val empty: Windows[Int] = Windows.empty[Int](StepSize, NumSteps)
    val windows = events.foldLeft(empty) { case (w, (t, x)) => w.add(x, t) }
    val realSum = events.iterator.collect { case (t, x) if t > lastT - WindowSize => x }.sum

    windows.lowerBoundSum should be <= realSum
    windows.upperBoundSum should be >= realSum
  }

  testCase((0L, 0), Vector((9223372036854775807L, 0)))
}
