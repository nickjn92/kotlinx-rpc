/*
 * Copyright 2023-2026 JetBrains s.r.o and contributors. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.rpc.krpc.compression.zstd

import com.squareup.zstd.ZSTD_e_continue
import com.squareup.zstd.ZSTD_e_end
import com.squareup.zstd.getErrorName
import com.squareup.zstd.zstdCompressor
import com.squareup.zstd.zstdDecompressor
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.withContext
import kotlinx.rpc.krpc.KrpcConfigBuilder
import kotlinx.rpc.krpc.KrpcMessageCodec
import kotlinx.rpc.krpc.KrpcMessageCompression

private const val BUFFER_SIZE = 8 * 1024
private const val COMPRESSION_INPUT_CHUNK_SIZE = 128 * 1024
// Zstandard recommends that encoders and decoders support windows up to 8 MiB.
private const val MAX_SUPPORTED_WINDOW_SIZE = 8 * 1024 * 1024
private const val MAX_ZSTD_BLOCK_SIZE = 128 * 1024
private const val MAX_BLOCK_COUNT = 65_536
private const val ZSTD_C_COMPRESSION_LEVEL = 100
private const val ZSTD_DEFAULT_COMPRESSION_LEVEL = 3
private const val ZSTD_MIN_COMPRESSION_LEVEL = -131_072
private const val ZSTD_MAX_COMPRESSION_LEVEL = 22

/**
 * Enables Zstandard compression for serialized kRPC transport messages.
 *
 * Both endpoints must configure Zstd compression for it to be used.
 *
 * @param minimumMessageSize minimum serialized message size in bytes before compression is attempted.
 * @param maxDecompressedMessageSize maximum decompressed message size accepted from a peer.
 * @param compressionLevel zstd compression level passed to zstd-kmp.
 */
public fun KrpcConfigBuilder.Connector.zstd(
    minimumMessageSize: Int = KrpcMessageCompression.DEFAULT_MINIMUM_MESSAGE_SIZE,
    maxDecompressedMessageSize: Int = KrpcMessageCompression.DEFAULT_MAX_DECOMPRESSED_MESSAGE_SIZE,
    compressionLevel: Int = ZSTD_DEFAULT_COMPRESSION_LEVEL,
): Unit {
    messageCompression = KrpcMessageCompression(
        codec = ZstdKrpcMessageCodec(compressionLevel),
        minimumMessageSize = minimumMessageSize,
        maxDecompressedMessageSize = maxDecompressedMessageSize,
    )
}

/**
 * Zstandard codec for kRPC message compression.
 *
 * @param compressionLevel zstd compression level passed to zstd-kmp.
 */
public class ZstdKrpcMessageCodec(
    /**
     * Zstd compression level passed to zstd-kmp.
     */
    public val compressionLevel: Int = ZSTD_DEFAULT_COMPRESSION_LEVEL,
) : KrpcMessageCodec {
    init {
        require(compressionLevel in ZSTD_MIN_COMPRESSION_LEVEL..ZSTD_MAX_COMPRESSION_LEVEL) {
            "compressionLevel must be between $ZSTD_MIN_COMPRESSION_LEVEL and $ZSTD_MAX_COMPRESSION_LEVEL"
        }
    }

    override val name: String = "zstd"

    override suspend fun compress(data: ByteArray): ByteArray = withContext(Dispatchers.Default) {
        val compressor = zstdCompressor()
        val output = ByteArrayBuilder()
        val buffer = ByteArray(BUFFER_SIZE)

        try {
            compressor.setParameter(ZSTD_C_COMPRESSION_LEVEL, compressionLevel).checkZstd("configure")

            var inputStart = 0
            var inputEnd = minOf(data.size, COMPRESSION_INPUT_CHUNK_SIZE)

            while (true) {
                currentCoroutineContext().ensureActive()
                val isFinalChunk = inputEnd == data.size
                val remaining = compressor.compressStream2(
                    outputByteArray = buffer,
                    outputEnd = buffer.size,
                    outputStart = 0,
                    inputByteArray = data,
                    inputEnd = inputEnd,
                    inputStart = inputStart,
                    mode = if (isFinalChunk) ZSTD_e_end else ZSTD_e_continue,
                ).checkZstd("compress")

                inputStart += compressor.inputBytesProcessed
                output.write(buffer, compressor.outputBytesProcessed)

                val isDone = isFinalChunk && inputStart == data.size && remaining == 0L
                require(
                    compressor.inputBytesProcessed != 0 ||
                        compressor.outputBytesProcessed != 0 ||
                        isDone,
                ) {
                    "zstd compress failed: no progress"
                }

                if (isDone) break
                if (!isFinalChunk && inputStart == inputEnd) {
                    inputEnd += minOf(COMPRESSION_INPUT_CHUNK_SIZE, data.size - inputEnd)
                }
            }
        } finally {
            compressor.close()
        }

        output.toByteArray()
    }

    override suspend fun decompress(data: ByteArray, maxDecompressedSize: Int): ByteArray =
        withContext(Dispatchers.Default) {
            require(maxDecompressedSize > 0) {
                "maxDecompressedSize must be positive"
            }
            data.requireSingleZstdFrame(
                maxWindowSize = MAX_SUPPORTED_WINDOW_SIZE,
                maxDecompressedSize = maxDecompressedSize,
            )

            val decompressor = zstdDecompressor()
            val output = ByteArrayBuilder(minOf(maxDecompressedSize, BUFFER_SIZE))
            val buffer = ByteArray(minOf(maxDecompressedSize, BUFFER_SIZE))

            try {
                var inputStart = 0
                var remaining: Long

                do {
                    currentCoroutineContext().ensureActive()
                    val outputLimit = if (output.size == maxDecompressedSize) {
                        1
                    } else {
                        minOf(buffer.size, maxDecompressedSize - output.size)
                    }

                    remaining = decompressor.decompressStream(
                        outputByteArray = buffer,
                        outputEnd = outputLimit,
                        outputStart = 0,
                        inputByteArray = data,
                        inputEnd = data.size,
                        inputStart = inputStart,
                    ).checkZstd("decompress")

                    inputStart += decompressor.inputBytesProcessed

                    require(output.size <= maxDecompressedSize - decompressor.outputBytesProcessed) {
                        "Decompressed kRPC message exceeds the configured limit of $maxDecompressedSize bytes"
                    }

                    output.write(buffer, decompressor.outputBytesProcessed)

                    require(
                        decompressor.inputBytesProcessed != 0 ||
                            decompressor.outputBytesProcessed != 0 ||
                            remaining == 0L && inputStart == data.size,
                    ) {
                        "zstd decompress failed: EOF before end of stream"
                    }
                } while (remaining != 0L || inputStart < data.size)
            } finally {
                decompressor.close()
            }

            output.toByteArray()
        }
}

private fun ByteArray.requireSingleZstdFrame(maxWindowSize: Int, maxDecompressedSize: Int): Unit {
    requireAvailable(offset = 0, byteCount = 5, description = "zstd frame header")
    require(
        this[0] == 0x28.toByte() &&
            this[1] == 0xb5.toByte() &&
            this[2] == 0x2f.toByte() &&
            this[3] == 0xfd.toByte()
    ) {
        "zstd decompress failed: invalid frame magic"
    }

    val descriptor = this[4].toInt() and 0xff
    require(descriptor and 0x08 == 0) {
        "zstd decompress failed: reserved frame header bit is set"
    }

    val singleSegment = descriptor and 0x20 != 0
    val hasChecksum = descriptor and 0x04 != 0
    val dictionaryIdSize = when (descriptor and 0x03) {
        0 -> 0
        1 -> 1
        2 -> 2
        else -> 4
    }
    val frameContentSizeFlag = descriptor ushr 6
    val frameContentSizeFieldSize = when (frameContentSizeFlag) {
        0 -> if (singleSegment) 1 else 0
        1 -> 2
        2 -> 4
        else -> 8
    }

    var offset = 5
    val windowSize = if (singleSegment) {
        null
    } else {
        requireAvailable(offset, 1, "zstd window descriptor")
        val windowDescriptor = this[offset++].toInt() and 0xff
        val windowLog = 10 + (windowDescriptor ushr 3)
        val windowBase = 1L shl windowLog
        windowBase + windowBase / 8 * (windowDescriptor and 0x07)
    }

    requireAvailable(offset, dictionaryIdSize + frameContentSizeFieldSize, "zstd frame header fields")
    offset += dictionaryIdSize

    val frameContentSize = if (frameContentSizeFieldSize == 0) {
        null
    } else {
        readLittleEndianSize(offset, frameContentSizeFieldSize) +
            if (frameContentSizeFieldSize == 2) 256 else 0
    }
    offset += frameContentSizeFieldSize

    val requiredWindowSize = windowSize ?: frameContentSize ?: 0
    require(requiredWindowSize <= maxWindowSize.toLong()) {
        "zstd decompress failed: frame window of $requiredWindowSize bytes exceeds the supported limit of " +
            "$maxWindowSize bytes"
    }
    require(frameContentSize == null || frameContentSize <= maxDecompressedSize.toLong()) {
        "Decompressed kRPC message exceeds the configured limit of $maxDecompressedSize bytes"
    }

    var isLastBlock: Boolean
    var blockCount = 0
    do {
        require(++blockCount <= MAX_BLOCK_COUNT) {
            "zstd decompress failed: frame has too many blocks"
        }
        requireAvailable(offset, 3, "zstd block header")
        val blockHeader = (this[offset].toInt() and 0xff) or
            ((this[offset + 1].toInt() and 0xff) shl 8) or
            ((this[offset + 2].toInt() and 0xff) shl 16)
        offset += 3

        isLastBlock = blockHeader and 1 != 0
        val blockType = blockHeader ushr 1 and 0x03
        val blockSize = blockHeader ushr 3
        require(blockType != 3) {
            "zstd decompress failed: reserved block type"
        }
        require(blockSize.toLong() <= minOf(MAX_ZSTD_BLOCK_SIZE.toLong(), requiredWindowSize)) {
            "zstd decompress failed: block size exceeds the frame window"
        }

        val blockContentSize = if (blockType == 1) 1 else blockSize
        requireAvailable(offset, blockContentSize, "zstd block content")
        offset += blockContentSize
    } while (!isLastBlock)

    if (hasChecksum) {
        requireAvailable(offset, 4, "zstd content checksum")
        offset += 4
    }

    require(offset == size) {
        "zstd decompress failed: expected exactly one frame"
    }
}

private fun ByteArray.requireAvailable(offset: Int, byteCount: Int, description: String): Unit {
    require(offset >= 0 && byteCount >= 0 && offset <= size - byteCount) {
        "zstd decompress failed: incomplete $description"
    }
}

private fun ByteArray.readLittleEndianSize(offset: Int, byteCount: Int): Long {
    var result = 0L

    for (index in 0 until byteCount) {
        val value = this[offset + index].toLong() and 0xff
        if (index >= Int.SIZE_BYTES && value != 0L) {
            return Int.MAX_VALUE.toLong() + 1
        }
        if (index < Int.SIZE_BYTES) {
            result = result or (value shl (index * 8))
        }
    }

    return result
}

private fun Long.checkZstd(operation: String): Long {
    val errorName = getErrorName(this) ?: return this
    throw IllegalArgumentException("zstd $operation failed: $errorName")
}

private class ByteArrayBuilder(initialCapacity: Int = BUFFER_SIZE) {
    private var buffer: ByteArray = ByteArray(initialCapacity.coerceAtLeast(1))

    var size: Int = 0
        private set

    fun write(source: ByteArray, byteCount: Int): Unit {
        if (byteCount == 0) return

        ensureCapacity(size + byteCount)
        source.copyInto(buffer, destinationOffset = size, startIndex = 0, endIndex = byteCount)
        size += byteCount
    }

    fun toByteArray(): ByteArray {
        return if (size == buffer.size) buffer else buffer.copyOf(size)
    }

    private fun ensureCapacity(requiredCapacity: Int): Unit {
        if (requiredCapacity <= buffer.size) return

        var newCapacity = buffer.size
        while (newCapacity < requiredCapacity) {
            newCapacity = if (newCapacity > Int.MAX_VALUE / 2) {
                requiredCapacity
            } else {
                maxOf(requiredCapacity, newCapacity * 2)
            }
        }

        buffer = buffer.copyOf(newCapacity)
    }
}
