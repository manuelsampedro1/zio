package zio.internal

import org.openjdk.jmh.annotations._

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.TimeUnit

@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 10, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(3)
@State(Scope.Thread)
private[this] class FiberMailboxBenchmark {
  private[this] val message = FiberMessage.resumeUnit

  private[this] var mailbox: FiberMailbox                 = _
  private[this] var linked: ConcurrentLinkedQueue[AnyRef] = _

  @Setup(Level.Invocation)
  def setup(): Unit = {
    mailbox = new FiberMailbox()
    linked = new ConcurrentLinkedQueue[AnyRef]()
  }

  @Benchmark
  def fiberMailboxOfferPoll(): FiberMessage = {
    mailbox.add(message)
    mailbox.poll()
  }

  @Benchmark
  def concurrentLinkedQueueOfferPoll(): AnyRef = {
    linked.add(message)
    linked.poll()
  }
}
