package com.kaizensundays.fusion.messaging

import org.junit.Test
import java.net.URI
import kotlin.test.assertEquals

/**
 * Created: Friday 10/6/2023, 7:10 PM Eastern Time
 *
 * @author Sergey Chuykov
 */
class TopicTest {

    @Test
    fun parseTopics() {

        var uri = URI("get:/ping?timeoutSec=10")

        assertEquals("get", uri.scheme)
        assertEquals("/ping", uri.path)

        uri = URI("post:/submit?timeoutSec=10&retries=3")

        assertEquals("post", uri.scheme)
        assertEquals("/submit", uri.path)

        val params = uri.query?.split("&")?.associate {
            val (key, value) = it.split("=")
            key to value
        } ?: emptyMap()

        assertEquals(2, params.size)

        uri = URI("topic:/kappa")

        assertEquals("topic", uri.scheme)
        assertEquals("/kappa", uri.path)
    }

}