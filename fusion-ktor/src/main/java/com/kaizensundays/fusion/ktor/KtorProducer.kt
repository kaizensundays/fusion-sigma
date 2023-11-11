package com.kaizensundays.fusion.ktor

import com.kaizensundays.fusion.messsaging.Instance
import com.kaizensundays.fusion.messsaging.LoadBalancer
import com.kaizensundays.fusion.messsaging.Producer
import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import io.ktor.client.plugins.HttpRequestRetry
import io.ktor.client.plugins.retry
import io.ktor.client.request.get
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.readBytes
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.content.ByteArrayContent
import io.ktor.http.contentType
import io.ktor.http.takeFrom
import kotlinx.coroutines.runBlocking
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.net.URI

/**
 * Created: Saturday 10/07/2023, 1:15 PM Eastern Time
 *
 * @author Sergey Chuykov
 */
@SuppressWarnings(
    "kotlin:S6508" // Mono<Void>
)
class KtorProducer(private val loadBalancer: LoadBalancer) : Producer {

    private val logger: Logger = LoggerFactory.getLogger(javaClass)

    private val supportedSchemes = listOf("get", "post")

    private val retryDelayMs = 1000L

    var httpClient: HttpClient = httpClient()

    var getUrl: (Instance, URI) -> String = this::url

    private fun httpClient(): HttpClient {
        return HttpClient(CIO) {
            install(HttpRequestRetry) {
                retryOnServerErrors(maxRetries = 3)
            }
        }
    }

    private fun url(instance: Instance, topic: URI): String {
        return "http://${instance.host}:${instance.port}" + topic.path
    }

    private fun nextUrl(topic: URI): String {
        val instance = loadBalancer.get()
        return getUrl(instance, topic)
    }

    private suspend fun get(topic: URI, client: HttpClient): HttpResponse {
        return client.get {
            val builder = this
            retry {
                val url = nextUrl(topic)
                builder.url.takeFrom(url)
                delayMillis { retry ->
                    logger.info("Connecting $url ($retry)")
                    retryDelayMs
                }
            }
        }
    }

    private suspend fun post(topic: URI, msg: ByteArray, client: HttpClient): HttpResponse {
        return client.post {
            setBody(ByteArrayContent(msg))
            contentType(ContentType.Application.OctetStream)
            val builder = this
            retry {
                val url = nextUrl(topic)
                builder.url.takeFrom(url)
                delayMillis { retry ->
                    logger.info("Connecting $url ($retry)")
                    retryDelayMs
                }
            }
        }
    }

    override fun request(topic: URI, msg: ByteArray): Flux<ByteArray> {

        require(supportedSchemes.contains(topic.scheme))

        return Flux.create { emitter ->

            val bytes = runBlocking {
                val response: HttpResponse = when (topic.scheme) {
                    "post" -> post(topic, msg, httpClient)
                    else -> get(topic, httpClient)
                }
                logger.info("response=$response")
                if (response.status == HttpStatusCode.OK) response.readBytes() else byteArrayOf()
            }

            emitter.next(bytes)
            emitter.complete()
        }
    }

    override fun request(topic: URI): Flux<ByteArray> {
        return request(topic, byteArrayOf())
    }

    override fun send(topic: URI, msg: ByteArray): Mono<Void> {
        return Mono.empty()
    }
}