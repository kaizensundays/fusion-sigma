package com.kaizensundays.fusion.messaging

import reactor.core.publisher.Mono
import java.util.concurrent.atomic.AtomicLong

/**
 * Created: Saturday 10/14/2023, 7:33 PM Eastern Time
 *
 * @author Sergey Chuykov
 */
class DefaultLoadBalancer(private val instances: List<Instance>) : LoadBalancer {

    private val index = AtomicLong()

    override fun get(): Mono<Instance> {
        return if (instances.isNotEmpty()) {
            val idx = index.getAndIncrement()
            Mono.just(instances[(idx % instances.size).toInt()])
        } else {
            Mono.error(IllegalStateException())
        }
    }

}