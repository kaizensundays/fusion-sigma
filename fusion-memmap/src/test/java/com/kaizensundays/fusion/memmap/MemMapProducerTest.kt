package com.kaizensundays.fusion.memmap

import org.junit.jupiter.api.Test
import java.net.URI
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

/**
 * Created: Saturday 1/20/2024, 6:27 PM Eastern Time
 *
 * @author Sergey Chuykov
 */
class MemMapProducerTest {

    private val baseDir = "target/.mmq"

    private fun producerName(obj: Any) = javaClass.simpleName + '.' + obj.javaClass.enclosingMethod.name

    private val timeout = Duration.ofSeconds(10)

    @Test
    fun sendAndReceive() {

        val producer = MemMapProducer(baseDir, producerName(object {}), 1000)

        producer.send(URI("memmap:/default"), "Ok".toByteArray())
            .block(timeout)

        val msg = producer.pub.poll(timeout).block(timeout)

        assertNotNull(msg)
        assertEquals("Ok", String(msg))
    }

}