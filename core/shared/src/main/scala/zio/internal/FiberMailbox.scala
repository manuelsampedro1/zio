/*
 * Copyright 2022-2024 John A. De Goes and the ZIO Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package zio.internal

import zio.stacktracer.TracingImplicits.disableAutoTrace

import java.util.concurrent.atomic.AtomicReference

/**
 * A specialized single-reader, multiple-writer mailbox for fiber messages.
 *
 * This is a minimal MPSC linked queue tailored to FiberRuntime's inbox access
 * pattern. Producers atomically publish a new tail and then link it from the
 * previous tail. The single consumer owns the head pointer and only spins in
 * the rare window where a producer has published the tail but has not linked
 * the predecessor yet.
 */
private[zio] final class FiberMailbox {
  import FiberMailbox.Node

  private[this] val stub     = new Node(null)
  private[this] val producer = new AtomicReference[Node](stub)
  private[this] var consumer = stub

  def add(message: FiberMessage): Unit = {
    val node = new Node(message)
    val prev = producer.getAndSet(node)
    prev.next = node
  }

  def poll(): FiberMessage = {
    var next = consumer.next

    if (next eq null) {
      if (consumer eq producer.get()) return null

      while (next eq null) {
        next = consumer.next
      }
    }

    consumer = next

    val message = next.message
    next.message = null
    message
  }

  def isEmpty: Boolean =
    (consumer eq producer.get()) && (consumer.next eq null)
}

private[zio] object FiberMailbox {
  private final class Node(var message: FiberMessage) {
    @volatile var next: Node = _
  }
}
