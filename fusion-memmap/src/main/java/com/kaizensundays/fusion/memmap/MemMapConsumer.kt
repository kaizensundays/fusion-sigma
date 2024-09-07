package com.kaizensundays.fusion.memmap

import com.kaizensundays.fusion.messaging.Consumer
import reactor.core.publisher.Flux

/**
 * Created: Saturday 1/20/2024, 6:37 PM Eastern Time
 *
 * @author Sergey Chuykov
 */
class MemMapConsumer : Consumer {

    override fun handle(messages: Flux<ByteArray>): Flux<ByteArray> {
        return Flux.empty()
    }

}