package com.kaizensundays.fusion.ktor

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import java.time.Duration

/**
 * Created: Sunday 10/8/2023, 7:06 PM Eastern Time
 *
 * @author Sergey Chuykov
 */
abstract class KtorTestSupport {

    val logger: Logger = LoggerFactory.getLogger(javaClass)

    fun ByteArray?.asText() = if (this != null) String(this) else ""

    fun <T> Flux<T>.blockLast(sec: Long): T? = blockLast(Duration.ofSeconds(sec))

}