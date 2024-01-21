package com.kaizensundays.fusion.memmap

import reactor.core.publisher.Mono
import java.nio.ByteBuffer
import java.time.Duration

/**
 * Created: Saturday 1/20/2024, 6:45 PM Eastern Time
 *
 * @author Sergey Chuykov
 */
class DefaultMemMapQueue : ReactiveQueue<ByteBuffer> {

    override fun offer(obj: ByteBuffer, timeout: Duration): Mono<Boolean> {
        return Mono.just(false)
    }

    override fun poll(timeout: Duration): Mono<ByteBuffer> {
        return Mono.empty()
    }

}