package com.kaizensundays.fusion.memmap

import com.kaizensundays.fusion.messaging.Producer
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.net.URI
import java.time.Duration

/**
 * Created: Saturday 1/20/2024, 6:23 PM Eastern Time
 *
 * @author Sergey Chuykov
 */
@SuppressWarnings(
    "kotlin:S6508", // Mono<Void>
)
class MemMapProducer(baseDir: String, producerName: String, maxQueueSize: Int) : Producer {

    private val timeout = Duration.ofSeconds(10)

    val pub = DefaultMemMapQueue(baseDir, "$producerName.pub", maxQueueSize)
    val sub = DefaultMemMapQueue(baseDir, "$producerName.sub", maxQueueSize)

    override fun request(topic: URI, messages: Flux<ByteArray>): Flux<ByteArray> {
        return Flux.empty()
    }

    override fun request(topic: URI, msg: ByteArray): Flux<ByteArray> {
        return Flux.empty()
    }

    override fun request(topic: URI): Flux<ByteArray> {
        return Flux.empty()
    }

    override fun send(topic: URI, msg: ByteArray): Mono<Void> {

        return pub.offer(msg, timeout).then()
    }

}