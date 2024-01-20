package com.kaizensundays.fusion.webflux

import com.kaizensundays.fusion.messaging.DefaultLoadBalancer
import com.kaizensundays.fusion.messaging.Instance
import com.kaizensundays.fusion.messaging.LoadBalancer
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import org.springframework.boot.test.web.server.LocalServerPort
import org.springframework.test.context.ContextConfiguration
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.socket.client.WebSocketClient
import reactor.core.publisher.Flux
import reactor.test.StepVerifier
import java.net.URI
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Created: Saturday 9/30/2023, 7:18 PM Eastern Time
 *
 * @author Sergey Chuykov
 */
@ContextConfiguration(locations = ["/WebFluxProducerIntegrationTest.xml"])
class WebFluxProducerIntegrationTest : IntegrationTestSupport() {

    private lateinit var loadBalancer: LoadBalancer

    private lateinit var producer: WebFluxProducer

    @LocalServerPort
    var port = 0

    @BeforeEach
    fun before() {
        loadBalancer = DefaultLoadBalancer(
            listOf(
                Instance("localhost", port + 2),
                Instance("localhost", port + 1),
                Instance("localhost", port),
            )
        )
        producer = WebFluxProducer(loadBalancer)
    }


    @Test
    fun get() {

        val response = producer.request(URI("get:/ping?maxAttempts=3"))
            .blockLast(100)

        assertEquals("Ok", response.asText())
    }

    @Test
    fun post() {

        val msg = "{ ${javaClass.simpleName} }".toByteArray()

        val response = producer.request(URI("post:/submit?maxAttempts=3"), msg)
            .blockLast(10)

        assertEquals("Ok", response.asText())
    }

    @Test
    fun throwsExceptionIfSchemeIsNotSupported() {

        assertThrows<IllegalArgumentException> {
            producer.request(URI("unsupported:/find")).subscribe()
        }
    }

    @Test
    fun returnsFluxErrorIfWebClientThrowsException() {

        val client: WebClient = mock()

        producer.setWebClient(client)

        whenever(client.get()).thenThrow(IllegalStateException())

        val f = producer.request(URI("get:/find"))

        StepVerifier.create(f)
            .expectError(IllegalStateException::class.java)
            .verify()

    }

    @Test
    fun stream() {

        val messages = (0..3)
            .map { _ -> "{ ${javaClass.simpleName}:${System.currentTimeMillis()} }".toByteArray() }

        val topic = URI("ws:/default/ws?maxAttempts=3")

        val result = producer.request(topic, Flux.fromIterable(messages))
            .take(messages.size.toLong())

        val done = StepVerifier.create(result)
            .expectNextCount(messages.size.toLong())
            .verifyComplete()

        assertTrue(done < Duration.ofSeconds(10))
    }

    @Test
    fun returnsFluxErrorIfWebSocketClientThrowsException() {

        val client: WebSocketClient = mock()

        producer.setWebSocketClient(client)

        whenever(client.execute(any(), any())).thenThrow(IllegalStateException())

        val f = producer.request(URI("ws:/default"))

        StepVerifier.create(f)
            .expectError(IllegalStateException::class.java)
            .verify()

    }

    @Test
    fun send() {

        val msg = "{ ${javaClass.simpleName} }".toByteArray()

        val topic = URI("ws:/default/ws?maxAttempts=3")

        val m = producer.send(topic, msg)

        val done = StepVerifier.create(m)
            .verifyComplete()

        assertTrue(done < Duration.ofSeconds(10))
    }

}