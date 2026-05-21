package zio.internal

import zio._
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.test._

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger

object FiberMailboxSpec extends ZIOBaseSpec {

  def spec = suite("FiberMailboxSpec")(
    test("poll returns null when empty") {
      val mailbox = new FiberMailbox()

      assert(mailbox.poll())(isNull) &&
      assert(mailbox.isEmpty)(isTrue)
    },
    test("preserves FIFO order for a single producer") {
      val mailbox = new FiberMailbox()
      val first   = FiberMessage.resumeUnit
      val second  = FiberMessage.Stateful(_ => ())
      val third   = FiberMessage.Resume(ZIO.unit)

      mailbox.add(first)
      mailbox.add(second)
      mailbox.add(third)

      assert(mailbox.poll())(equalTo(first)) &&
      assert(mailbox.poll())(equalTo(second)) &&
      assert(mailbox.poll())(equalTo(third)) &&
      assert(mailbox.poll())(isNull) &&
      assert(mailbox.isEmpty)(isTrue)
    },
    test("does not lose messages from multiple producers") {
      val mailbox       = new FiberMailbox()
      val producerCount = 8
      val perProducer   = 1000
      val start         = new CountDownLatch(1)
      val done          = new CountDownLatch(producerCount)
      val consumed      = new AtomicInteger(0)

      val threads =
        (0 until producerCount).map { _ =>
          new Thread(() => {
            start.await()
            var i = 0
            while (i < perProducer) {
              mailbox.add(FiberMessage.resumeUnit)
              i += 1
            }
            done.countDown()
          })
        }

      ZIO.attempt {
        threads.foreach(_.start())
        start.countDown()

        while (done.getCount > 0 || !mailbox.isEmpty) {
          val message = mailbox.poll()
          if (message ne null) consumed.incrementAndGet()
        }

        threads.foreach(_.join())
      }.as {
        assert(consumed.get())(equalTo(producerCount * perProducer)) &&
        assert(mailbox.poll())(isNull) &&
        assert(mailbox.isEmpty)(isTrue)
      }
    } @@ nonFlaky(100)
  )
}
