package com.kaizensundays.fusion.ktor

import com.kaizensundays.fusion.messsaging.DefaultLoadBalancer
import com.kaizensundays.fusion.messsaging.Instance
import com.kaizensundays.fusion.messsaging.LoadBalancer
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.boot.test.web.server.LocalServerPort
import org.springframework.test.context.ContextConfiguration
import java.net.URI
import kotlin.test.assertEquals

/**
 * Created: Saturday 10/7/2023, 1:31 PM Eastern Time
 *
 * @author Sergey Chuykov
 */
@ContextConfiguration(locations = ["/KtorProducerIntegrationTest.xml"])
class KtorProducerIntegrationTest : KtorIntegrationTestSupport() {

    private lateinit var loadBalancer: LoadBalancer

    private lateinit var producer: KtorProducer

    @LocalServerPort
    var port = -1

    @BeforeEach
    fun before() {
        loadBalancer = DefaultLoadBalancer(
            listOf(
                Instance("localhost", port),
            )
        )
        producer = KtorProducer(loadBalancer)
    }

    @Test
    fun ping() {

        val response = producer.request(URI("get:/ping"))
            .blockLast(10)

        assertEquals("Ok", response.asText())
    }

    @Test
    fun submit() {

        val msg = "{ ${javaClass.simpleName} }".toByteArray()

        val response = producer.request(URI("post:/submit"), msg)
            .blockLast(10)

        assertEquals("Ok", response.asText())
    }

}