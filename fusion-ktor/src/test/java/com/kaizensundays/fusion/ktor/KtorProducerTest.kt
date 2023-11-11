package com.kaizensundays.fusion.ktor

import com.kaizensundays.fusion.messsaging.Instance
import com.kaizensundays.fusion.messsaging.LoadBalancer
import io.ktor.server.application.call
import io.ktor.server.response.respond
import io.ktor.server.routing.get
import io.ktor.server.testing.testApplication
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import java.net.URI
import kotlin.test.assertEquals

/**
 * Created: Saturday 10/7/2023, 5:54 PM Eastern Time
 *
 * @author Sergey Chuykov
 */
class KtorProducerTest : KtorTestSupport() {

    private val loadBalancer: LoadBalancer = mock()

    private var producer = KtorProducer(loadBalancer)

    @BeforeEach
    fun before() {
        whenever(loadBalancer.get()).thenReturn(Instance("localhost", 51001))
    }

    @Test
    fun getUrl() {

        assertEquals("http://node0:51001/ping", producer.getUrl(Instance("node0", 51001), URI("get:/ping")))

        assertEquals("http://node1:51003/findAll", producer.getUrl(Instance("node1", 51003), URI("get:/findAll")))

        assertEquals("http://node2:51007/submit", producer.getUrl(Instance("node2", 51007), URI("post:/submit")))
    }

    @Test
    fun unexpectedSchema() {

        assertThrows<IllegalArgumentException> {
            producer.request(URI("unexpected:/path"))
                .blockLast(10)
        }
    }

    @Test
    fun returnsOk() {

        testApplication {
            routing {
                get("/ping") {
                    call.respond("Ok".toByteArray())
                }
            }

            producer.httpClient = createClient { }
            producer.getUrl = { _, _ -> "" }

            producer.request(URI("get:/ping"))
                .blockLast(10)
        }
    }

}