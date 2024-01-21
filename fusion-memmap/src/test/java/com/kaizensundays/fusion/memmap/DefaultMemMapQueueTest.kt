package com.kaizensundays.fusion.memmap

import org.junit.jupiter.api.Test
import reactor.core.publisher.Flux
import reactor.test.StepVerifier
import java.time.Duration
import java.util.*
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

    @Test
    fun offerReturnsFalseIfQueueIsFull() {

        val queue = DefaultMemMapQueue(baseDir, javaClass.simpleName + '.' + enclosingMethod(object {}), 1)

        val m = queue.offer("abc".toByteArray(), timeout)

        val done = StepVerifier.create(m)
            .expectNext(false)
            .verifyComplete()

        assertTrue(done < timeout)
    }

    @Test
    fun pollReturnsNothingIfQueueIsEmpty() {

        val queue = DefaultMemMapQueue(baseDir, javaClass.simpleName + '.' + enclosingMethod(object {}), 1)

        val m = queue.poll(timeout)

        val done = StepVerifier.create(m)
            .verifyComplete()

        assertTrue(done < timeout)
    }

    @Test
    fun offerAndPollOneMessage() {

        val queue = DefaultMemMapQueue(baseDir, javaClass.simpleName + '.' + enclosingMethod(object {}), 8)

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

        val queue = DefaultMemMapQueue(baseDir, javaClass.simpleName + '.' + enclosingMethod(object {}), 1000)

        val messages = (0..3)
            .map { _ -> "{ ${javaClass.simpleName}:${Date()} }".toByteArray() }

        val offers = Flux.fromIterable(messages)
            .flatMap { msg -> queue.offer(msg, timeout) }

        val done = StepVerifier.create(offers)
            .expectNext(*Array(4) { true })
            .verifyComplete()

        assertTrue(done < timeout)
    }

}