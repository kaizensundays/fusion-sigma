package com.kaizensundays.fusion.memmap

import org.junit.jupiter.api.Test
import reactor.core.publisher.Flux
import reactor.test.StepVerifier
import java.time.Duration
import kotlin.test.assertTrue

/**
 * Created: Saturday 1/20/2024, 6:52 PM Eastern Time
 *
 * @author Sergey Chuykov
 */
class DefaultMemMapQueueTest {

    private val baseDir = "target/.mmq"

    private val timeout = Duration.ofSeconds(10)

    private fun enclosingMethod(obj: Any) = obj.javaClass.enclosingMethod.name

    private fun queueName(obj: Any) = javaClass.simpleName + '.' + enclosingMethod(obj)

    @Test
    fun offerReturnsFalseIfQueueIsFull() {

        val queue = DefaultMemMapQueue(baseDir, queueName(object {}), 1)

        val m = queue.offer("abc".toByteArray(), timeout)

        val done = StepVerifier.create(m)
            .expectNext(false)
            .verifyComplete()

        assertTrue(done < timeout)
    }

    @Test
    fun pollReturnsNothingIfQueueIsEmpty() {

        val queue = DefaultMemMapQueue(baseDir, queueName(object {}), 1)

        val m = queue.poll(timeout)

        val done = StepVerifier.create(m)
            .verifyComplete()

        assertTrue(done < timeout)
    }

    @Test
    fun offerAndPollOneMessage() {

        val queue = DefaultMemMapQueue(baseDir, queueName(object {}), 8)

        val offer = queue.offer("Ok".toByteArray(), timeout)

        var done = StepVerifier.create(offer)
            .expectNext(true)
            .verifyComplete()

        assertTrue(done < timeout)

        val poll = queue.poll(timeout)

        done = StepVerifier.create(poll)
            .expectNextMatches { data -> "Ok" == String(data) }
            .verifyComplete()

        assertTrue(done < timeout)
    }

    @Test
    fun offerAndPollMessages() {

        val queue = DefaultMemMapQueue(baseDir, queueName(object {}), 1000)

        val num = 4

        val messages = (0 until num)
            .map { n -> "{ ${javaClass.simpleName}:${n} }" }

        val data = messages.map { s -> s.toByteArray() }

        val offers = Flux.fromIterable(data)
            .flatMap { msg -> queue.offer(msg, timeout) }

        var done = StepVerifier.create(offers)
            .expectNext(*Array(num) { true })
            .verifyComplete()

        assertTrue(done < timeout)

        val polls = Flux.range(0, num)
            .flatMap { _ -> queue.poll(timeout) }
            .map { msg -> String(msg) }

        done = StepVerifier.create(polls)
            .expectNext(*messages.toTypedArray())
            .verifyComplete()

        assertTrue(done < timeout)
    }

}