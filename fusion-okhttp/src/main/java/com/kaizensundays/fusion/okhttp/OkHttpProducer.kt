package com.kaizensundays.fusion.okhttp

import com.kaizensundays.fusion.messaging.LoadBalancer
import com.kaizensundays.fusion.messaging.Producer
import okhttp3.OkHttpClient
import okhttp3.Request
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.util.retry.Retry
import java.io.IOException
import java.net.URI
import java.time.Duration

/**
 * Created: Sunday 10/1/2023, 1:36 PM Eastern Time
 *
 * @author Sergey Chuykov
 */
class OkHttpProducer(private val loadBalancer: LoadBalancer) : Producer {

    private val logger: Logger = LoggerFactory.getLogger(javaClass)

    private fun Retry.RetrySignal.printAttempts() = "attempt=" + (this.totalRetries() + 1)

    private fun parse(s: String?, default: Long) = s?.toLong() ?: default

    fun optionsMap(topic: URI): Map<String, String> {

        return if (topic.query != null) {
            topic.query.split("&")
                .map { opt -> opt.split("=") }
                .associate { opt -> opt[0] to opt[1] }
        } else {
            emptyMap()
        }
    }

    fun topicOptions(topic: URI): TopicOptions {
        val opts = TopicOptions()
        val map = optionsMap(topic)
        opts.maxAttempts = parse(map["maxAttempts"], opts.maxAttempts)
        opts.timeoutSec = parse(map["timeoutSec"], opts.timeoutSec)
        return opts
    }

    override fun request(topic: URI, messages: Flux<ByteArray>): Flux<ByteArray> {
        return Flux.empty()
    }

    override fun request(topic: URI, msg: ByteArray): Flux<ByteArray> {

        val client = OkHttpClient.Builder().build()

        val request = Request.Builder().url("http://localhost:7701/ping").build()

        val response = client.newCall(request).execute()

        println(response)

        return Flux.empty()
    }

    override fun request(topic: URI): Flux<ByteArray> {
        return request(topic, byteArrayOf())
    }

    private fun send(topic: URI, msg: ByteArray, s: String): Mono<Void> {

        return Mono.error(IOException())
    }


    override fun send(topic: URI, msg: ByteArray): Mono<Void> {

        require("ws" == topic.scheme)

        val opts = topicOptions(topic)

        return Mono.defer { send(topic, msg, "") }
            .retryWhen(
                Retry.backoff(opts.maxAttempts, Duration.ofSeconds(opts.minBackoffSec))
                    .maxBackoff(Duration.ofSeconds(opts.maxBackoffSec))
                    .doAfterRetry { signal -> logger.trace(signal.printAttempts()) }
            )
    }
}