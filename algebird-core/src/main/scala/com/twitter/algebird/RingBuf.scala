package com.twitter.algebird

/**
 * Value class to hold references to indices internal to RingBuf.
 */
case class Idx(value: Int) extends AnyVal {
  def +(i: Int): Idx = Idx(value + i)
  def -(i: Int): Idx = Idx(value - i)

  // Look/ itself up in the supplied vector.
  def of[A](v: Vector[A]): A = v(value)

  // updates the supplied vector with the supplied value.
  def update[A](v: Vector[A], a: A): Vector[A] = v.updated(value, a)
}

object Idx {
  def bounded(i: Long, bound: Int): Idx = Idx(((i + bound) % bound).toInt)
}

case class RingBuf[A](slots: Vector[A], index: Idx) { lhs =>
  import RingBuf._
  /**
   * Slots are 0-indexed from the current `index`, looking backwards.
   *
   * `toIdx` converts an index into a vector position in `slots` for
   * internal use.
   *
   * i=1, index=5, toIdx(i)=Idx(4)
   * i=(slots.size - 1), index=5, toIdx(i)==Idx(6) (ie last item)
   */
  private[this] def toIdx(i: Long): Idx = rawIdx(i + index.value)

  /**
   * Use if you don't need the `index.value` offset.
   */
  private[this] def rawIdx(i: Long): Idx = Idx.bounded(i, slots.size)

  private[this] def assertBounds(i: Long): Unit =
    assert(i >= 0 && i < slots.size, s"$i is out of allowed bounds: [0 $slots.size)")

  /**
   * Returns a copy of the RingBuf with `a` added in to the proper
   * externally-indexed slot.
   */
  def add(a: A, i: Long)(implicit ev: Monoid[A]): RingBuf[A] = {
    assertBounds(i)
    val idx = toIdx(i)
    copy(slots = idx.update(slots, ev.plus(idx.of(slots), a)))
  }

  /**
   * Look up the item in the vector by external index.
   */
  def apply(i: Long): A = {
    assertBounds(i)
    toIdx(i).of(slots)
  }

  /**
   * Steps the RingBuf forward `i` steps, zero-ing out all new
   * entries.
   */
  def step(i: Long)(implicit ev: Monoid[A]): RingBuf[A] = {
    if (i <= 0) this
    else {
      val v = (1L to (i min slots.size)).foldLeft(slots) {
        (acc, j) => toIdx(j).update(acc, ev.zero)
      }
      RingBuf(v, toIdx(i))
    }
  }

  def +(rhs: RingBuf[A])(implicit ev: Monoid[A]): RingBuf[A] = {
    val merge = (lhs.reverseIterator zip rhs.reverseIterator).map {
      case (l, r) => ev.plus(l, r)
    }
    val newSize = lhs.slots.size min rhs.slots.size
    RingBuf(merge.toVector.reverse, rawIdx(newSize - 1))
  }

  // oldest-to-newest iterator across partial sums
  def iterator: Iterator[A] =
    (1 to slots.size).iterator.map(i => toIdx(i).of(slots))

  // newest-to-oldest iterator across partial sums
  def reverseIterator: Iterator[A] =
    (0 until slots.size).iterator.map(i => rawIdx(index.value - i).of(slots))

  def size: Int = slots.size
}

object RingBuf {
  def empty[A](size: Int)(implicit ev: Monoid[A]): RingBuf[A] =
    RingBuf(Vector.fill(size)(ev.zero), Idx(0))

  def monoid[A](size: Int)(implicit ev: Monoid[A]): Monoid[RingBuf[A]] =
    Monoid.from[RingBuf[A]](RingBuf.empty(size))(_ + _)

  implicit def equiv[A: Equiv](implicit ev: Equiv[A]): Equiv[RingBuf[A]] =
    new Equiv[RingBuf[A]] {
      def equiv(x: RingBuf[A], y: RingBuf[A]): Boolean =
        (x.iterator zip y.iterator).forall { case (m, n) => ev.equiv(m, n) }
    }
}
