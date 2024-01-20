package com.kaizensundays.fusion.webflux

import com.kaizensundays.fusion.messaging.LoadBalancer
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import java.net.URI
import kotlin.test.assertEquals

/**
 * Created: Saturday 9/30/2023, 7:11 PM Eastern Time
 *
 * @author Sergey Chuykov
 */
class WebFluxProducerTest : TestSupport() {

    private var loadBalancer: LoadBalancer = mock()

    private val producer = WebFluxProducer(loadBalancer)

    @Test
    fun optionsMap() {

        assertEquals(emptyMap(), producer.optionsMap(URI("get:/ping")))
        assertEquals(mapOf("maxAttempts" to "3"), producer.optionsMap(URI("get:/ping?maxAttempts=3")))
        assertEquals(mapOf("maxAttempts" to "7", "timeoutSec" to "11"), producer.optionsMap(URI("get:/ping?maxAttempts=7&timeoutSec=11")))
        assertEquals(mapOf("maxAttempts" to "3", "timeoutSec" to "17"), producer.optionsMap(URI("get:/ping?timeoutSec=17&maxAttempts=3")))
    }

    @Test
    fun topicOptions() {

        assertEquals(TopicOptions(), producer.topicOptions(URI("get:/ping")))
        assertEquals(TopicOptions(maxAttempts = 3), producer.topicOptions(URI("get:/ping?maxAttempts=3")))
        assertEquals(TopicOptions(maxAttempts = 7, timeoutSec = 11), producer.topicOptions(URI("get:/ping?maxAttempts=7&timeoutSec=11")))
    }

}