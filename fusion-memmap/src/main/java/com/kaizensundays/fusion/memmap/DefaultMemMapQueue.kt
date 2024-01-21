package com.kaizensundays.fusion.memmap

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import java.io.File
import java.io.RandomAccessFile
import java.nio.channels.FileChannel
import java.time.Duration

/**
 * Created: Saturday 1/20/2024, 6:45 PM Eastern Time
 *
 * @author Sergey Chuykov
 */
class DefaultMemMapQueue(
    private val baseDir: String,
    private val queueName: String,
    private val maxQueueSize: Int
) : ReactiveQueue<ByteArray> {

    private val logger: Logger = LoggerFactory.getLogger(javaClass)

    companion object {
        const val MESSAGE_META_SIZE = 4
        const val QUEUE_META_SIZE = 8

        const val HEAD_BYTE_LOC = 0
        const val TAIL_BYTE_LOC = 4
    }

    init {
        File(baseDir).mkdirs()
    }

    private val mode = "rw"

    private val fileName = baseDir + File.separatorChar + queueName

    private val dataFile = RandomAccessFile("$fileName.data", mode)
    private val metaFile = RandomAccessFile("$fileName.meta", mode)

    private val dataBuffer = dataFile.channel.map(FileChannel.MapMode.READ_WRITE, 0, maxQueueSize.toLong())
    private val metaBuffer = metaFile.channel.map(FileChannel.MapMode.READ_WRITE, 0, QUEUE_META_SIZE.toLong())

    private fun head(): Int {
        return metaBuffer.getInt(HEAD_BYTE_LOC)
    }

    private fun head(loc: Int) {
        metaBuffer.putInt(HEAD_BYTE_LOC, loc)
    }

    private fun tail(): Int {
        return metaBuffer.getInt(TAIL_BYTE_LOC)
    }

    private fun tail(loc: Int) {
        metaBuffer.putInt(TAIL_BYTE_LOC, loc)
    }

    private fun isEmpty(): Boolean = head() == tail()

    private fun isFull(messageSize: Int): Boolean {
        val head = head()
        val tail = tail()
        val totalMessageSize = messageSize + MESSAGE_META_SIZE
        val dataSize = when {
            (head < tail) -> {
                tail - head
            }

            (head > tail) -> {
                tail + (maxQueueSize - head)
            }

            else -> {
                0
            }
        }
        val freeSpace = maxQueueSize - dataSize
        return if ((tail + totalMessageSize) % maxQueueSize == head) {
            true
        } else {
            totalMessageSize > freeSpace
        }
    }

    override fun offer(data: ByteArray, timeout: Duration): Mono<Boolean> {
        if (isFull(data.size)) {
            return Mono.just(false)
        }
        return Mono.just(true)
    }

    override fun poll(timeout: Duration): Mono<ByteArray> {
        if (isEmpty()) {
            return Mono.empty()
        }
        return Mono.just("?".toByteArray())
    }

}