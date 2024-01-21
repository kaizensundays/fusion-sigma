package com.kaizensundays.fusion.memmap

import org.junit.jupiter.api.Test
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

    private var queue = DefaultMemMapQueue(baseDir, javaClass.simpleName, 1000)

    private fun enclosingMethod(obj: Any) = obj.javaClass.enclosingMethod.name

    @Test
    fun offerReturnsFalseIfQueueIsFull() {

        queue = DefaultMemMapQueue(baseDir, javaClass.simpleName + '.' + enclosingMethod(object {}), 1)

        val m = queue.offer("abc".toByteArray(), Duration.ofSeconds(10))

        val done = StepVerifier.create(m)
            .expectNext(false)
            .verifyComplete()

        assertTrue(done < Duration.ofSeconds(10))
    }

    @Test
    fun pollReturnsNothingIfQueueIsEmpty() {

        queue = DefaultMemMapQueue(baseDir, javaClass.simpleName + '.' + enclosingMethod(object {}), 1)

        val m = queue.poll(Duration.ofSeconds(10))

        val done = StepVerifier.create(m)
            .verifyComplete()

        assertTrue(done < Duration.ofSeconds(10))
    }

}