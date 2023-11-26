package com.kaizensundays.fusion.messaging

import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.net.URI

/**
 * Created: Friday 9/29/2023, 8:56 PM Eastern Time
 *
 * @author Sergey Chuykov
 */
interface Producer {

    fun request(topic: URI, msg: ByteArray): Flux<ByteArray>

    fun request(topic: URI): Flux<ByteArray>

    fun send(topic: URI, msg: ByteArray): Mono<Void>

}