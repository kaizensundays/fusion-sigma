package com.kaizensundays.fusion.messaging

import reactor.core.publisher.Flux

/**
 * Created: Saturday 1/20/2024, 6:31 PM Eastern Time
 *
 * @author Sergey Chuykov
 */
fun interface Consumer {

    fun handle(messages: Flux<ByteArray>): Flux<ByteArray>

}