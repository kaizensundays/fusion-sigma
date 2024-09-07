package com.kaizensundays.fusion.webflux

import com.kaizensundays.fusion.messaging.LoadBalancer
import com.kaizensundays.fusion.messaging.Producer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.web.reactive.function.BodyInserters
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient
import org.springframework.web.reactive.socket.client.WebSocketClient
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import reactor.core.scheduler.Schedulers
import reactor.util.retry.Retry
import java.net.URI
import java.time.Duration
import java.util.concurrent.atomic.AtomicReference
import java.util.function.Supplier

/**
 * Created: Saturday 9/30/2023, 7:12 PM Eastern Time
 *
 * @author Sergey Chuykov
 */
@SuppressWarnings(
    "kotlin:S6508", // Mono<Void>
)
class WebFluxProducer(private val loadBalancer: LoadBalancer) : Producer {

    private val logger: Logger = LoggerFactory.getLogger(javaClass)

    private val supportedSchemes = listOf("get", "post", "ws")

    private var webClient: AtomicReference<WebClient> = AtomicReference()

    private var webSocketClient: AtomicReference<WebSocketClient> = AtomicReference()

    fun setWebClient(webClient: WebClient) {
        this.webClient.set(webClient)
    }

    fun setWebSocketClient(webSocketClient: WebSocketClient) {
        this.webSocketClient.set(webSocketClient)
    }

    fun <T> AtomicReference<T>.get(factory: Supplier<T>): T {
        if (this.get() == null) {
            synchronized(this) {
                if (this.get() == null) {
                    this.set(factory.get())
                }
            }
        }
        return this.get()
    }

    private fun webClient() = webClient.get { WebClient.create() }

    private fun webSocketClient() = webSocketClient.get { ReactorNettyWebSocketClient() }

    private fun Retry.RetrySignal.printAttempts() = "attempt=" + (this.totalRetries() + 1)

    fun optionsMap(topic: URI): Map<String, String> {

        return if (topic.query != null) {
            topic.query.split("&")
                .map { opt -> opt.split("=") }
                .associate { opt -> opt[0] to opt[1] }
        } else {
            emptyMap()
        }
    }

    private fun parse(s: String?, default: Long) = s?.toLong() ?: default

    fun topicOptions(topic: URI): TopicOptions {
        val opts = TopicOptions()
        val map = optionsMap(topic)
        opts.maxAttempts = parse(map["maxAttempts"], opts.maxAttempts)
        opts.timeoutSec = parse(map["timeoutSec"], opts.timeoutSec)
        return opts
    }

    private fun nextUri(topic: URI): URI {
        val instance = loadBalancer.get().block(Duration.ofSeconds(100))
        requireNotNull(instance)
        val uri = URI("${instance.protocol}://${instance.host}:${instance.port}" + topic.path)
        logger.info("nextUri=$uri")
        return uri
    }

    private fun get(topic: URI, client: WebClient): Flux<ByteArray> {

        return client.get()
            .uri { _ -> nextUri(topic) }
            .retrieve()
            .bodyToFlux(ByteArray::class.java)
            .doOnError { logger.trace("doOnError") }
    }

    private fun get(topic: URI): Flux<ByteArray> {

        val client = webClient()

        val opts = topicOptions(topic)

        return Flux.defer { get(topic, client) }
            .retryWhen(Retry.backoff(opts.maxAttempts, Duration.ofSeconds(opts.minBackoffSec))
                .maxBackoff(Duration.ofSeconds(opts.maxBackoffSec))
                .doAfterRetry { signal -> logger.trace(signal.printAttempts()) }
            )
    }

    private fun post(topic: URI, msg: ByteArray, client: WebClient): Flux<ByteArray> {

        return client.post()
            .uri { _ -> nextUri(topic) }
            .body(BodyInserters.fromValue(msg))
            .retrieve()
            .bodyToFlux(ByteArray::class.java)
            .doOnError { logger.trace("doOnError") }
    }

    private fun post(topic: URI, msg: ByteArray): Flux<ByteArray> {

        val client = webClient()

        val opts = topicOptions(topic)

        return Flux.defer { post(topic, msg, client) }
            .retryWhen(Retry.backoff(opts.maxAttempts, Duration.ofSeconds(opts.minBackoffSec))
                .maxBackoff(Duration.ofSeconds(opts.maxBackoffSec))
                .doAfterRetry { signal -> logger.trace(signal.printAttempts()) }
            )
    }

    private fun streamZipped(topic: URI, messages: Flux<ByteArray>, client: WebSocketClient): Flux<ByteArray> {

        val uri = nextUri(topic)

        val sub = Sinks.many().multicast().directBestEffort<ByteArray>()

        val outbound = messages
            .delayElements(Duration.ofMillis(1000))
            .doOnNext { msg ->
                logger.info("> {}", String(msg))
            }

        val ws = client.execute(uri) { session ->
            session.send(outbound.map { msg -> session.binaryMessage { factory -> factory.wrap(msg) } })
                .zipWith(session.receive()
                    .doOnSubscribe { _ -> logger.info("< doOnSubscribe") }
                    .map { wsm -> wsm.payloadAsText }
                    .doOnNext { msg ->
                        logger.info("< {}", msg)
                        sub.tryEmitNext(msg.toByteArray())
                    }.then()
                )
                .then()
        }.doOnError { e ->
            logger.error("", e)
            sub.tryEmitError(e)
        }

        return sub.asFlux()
            .publishOn(Schedulers.boundedElastic())
            .doOnSubscribe { _ -> ws.subscribe() }
    }

    @Suppress("CallingSubscribeInNonBlockingScope")
    private fun stream(topic: URI, messages: Flux<ByteArray>, client: WebSocketClient): Flux<ByteArray> {

        val uri = nextUri(topic)

        val sub = Sinks.many().multicast().directBestEffort<ByteArray>()

        val ws = client.execute(uri) { session ->

            val inbound = session.receive()
                .doOnSubscribe { _ -> logger.info("< doOnSubscribe") }
                .map { wsm -> wsm.payloadAsText }
                .publishOn(Schedulers.boundedElastic())
                .doOnNext { msg ->
                    logger.info("< {}", msg)
                    sub.tryEmitNext(msg.toByteArray())
                }
                .doOnError { e ->
                    sub.tryEmitError(e)
                }
                .then()

            val outbound = session.send(
                messages
                    .publishOn(Schedulers.boundedElastic())
                    .doOnNext { msg ->
                        logger.info("> {}", String(msg))
                    }
                    .doOnError { e ->
                        sub.tryEmitError(e)
                    }
                    .map { msg ->
                        session.binaryMessage { factory -> factory.wrap(msg) }
                    }
            )

            outbound.subscribe()
            inbound.subscribe()

            Mono.never()
        }.doOnError { e ->
            logger.error("", e)
            sub.tryEmitError(e)
        }

        return sub.asFlux()
            .publishOn(Schedulers.boundedElastic())
            .doOnSubscribe { _ -> ws.subscribe() }
    }

    override fun request(topic: URI, messages: Flux<ByteArray>): Flux<ByteArray> {

        require("ws" == topic.scheme)

        val opts = topicOptions(topic)

        val client = webSocketClient()

        return Flux.defer { stream(topic, messages, client) }
            .retryWhen(Retry.backoff(opts.maxAttempts, Duration.ofSeconds(opts.minBackoffSec))
                .maxBackoff(Duration.ofSeconds(opts.maxBackoffSec))
                .doAfterRetry { signal -> logger.trace(signal.printAttempts()) }
            )
    }

    override fun request(topic: URI, msg: ByteArray): Flux<ByteArray> {

        require(supportedSchemes.contains(topic.scheme))

        return try {
            when (topic.scheme) {
                "ws" -> request(topic, Flux.just(msg))
                "post" -> post(topic, msg)
                else -> get(topic)
            }
        } catch (e: Exception) {
            Flux.error(IllegalStateException())
        }
    }

    override fun request(topic: URI): Flux<ByteArray> {
        return request(topic, byteArrayOf())
    }

    private fun send(topic: URI, msg: ByteArray, client: WebSocketClient): Mono<Void> {

        val uri = nextUri(topic)

        val pub = Flux.just(msg)

        return client.execute(uri) { session ->
            session.send(pub.map { msg -> session.binaryMessage { factory -> factory.wrap(msg) } })
        }
    }

    override fun send(topic: URI, msg: ByteArray): Mono<Void> {

        require("ws" == topic.scheme)

        val opts = topicOptions(topic)

        val client = webSocketClient()

        return Mono.defer { send(topic, msg, client) }
            .retryWhen(Retry.backoff(opts.maxAttempts, Duration.ofSeconds(opts.minBackoffSec))
                .maxBackoff(Duration.ofSeconds(opts.maxBackoffSec))
                .doAfterRetry { signal -> logger.trace(signal.printAttempts()) }
            )
    }
}
