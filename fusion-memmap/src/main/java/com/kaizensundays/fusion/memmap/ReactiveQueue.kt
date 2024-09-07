package com.kaizensundays.fusion.memmap

import reactor.core.publisher.Mono
import java.time.Duration

/**
 * Created: Saturday 1/20/2024, 6:38 PM Eastern Time
 *
 * @author Sergey Chuykov
 */
interface ReactiveQueue<T> {

    fun offer(data: T, timeout: Duration): Mono<Boolean>

    fun poll(timeout: Duration): Mono<T>

}