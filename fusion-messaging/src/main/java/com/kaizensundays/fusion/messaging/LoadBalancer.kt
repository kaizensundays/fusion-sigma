package com.kaizensundays.fusion.messaging

import reactor.core.publisher.Mono

/**
 * Created: Sunday 10/8/2023, 1:24 PM Eastern Time
 *
 * @author Sergey Chuykov
 */
fun interface LoadBalancer {

    fun get(): Mono<Instance>

}