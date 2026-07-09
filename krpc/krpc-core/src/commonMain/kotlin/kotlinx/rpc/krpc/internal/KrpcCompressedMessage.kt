/*
 * Copyright 2023-2026 JetBrains s.r.o and contributors. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.rpc.krpc.internal

import kotlinx.rpc.krpc.KrpcMessageCompression
import kotlinx.rpc.krpc.KrpcTransportMessage
import kotlin.coroutines.cancellation.CancellationException
import kotlin.io.encoding.Base64

/*
 * Compressed message envelope layout:
 *
 * | magic "krpc-cmp" (8 bytes) | version (1 byte) | message type (1 byte) |
 * | codec name size (1 byte) | codec name | compressed payload |
 *
 * String messages are wrapped into the same binary envelope, which is then Base64-encoded
 * and prefixed with "krpc-cmp:", so that the transport message kind (string vs binary)
 * is preserved and transports that support only one kind keep working.
 */

private val COMPRESSED_MESSAGE_MAGIC = byteArrayOf(
    'k'.code.toByte(),
    'r'.code.toByte(),
    'p'.code.toByte(),
    'c'.code.toByte(),
    '-'.code.toByte(),
    'c'.code.toByte(),
    'm'.code.toByte(),
    'p'.code.toByte(),
)

private const val STRING_ENVELOPE_PREFIX = "krpc-cmp:"
private const val MAX_CODEC_NAME_SIZE = 255
private const val MAX_ENVELOPE_HEADER_SIZE = 8 + 3 + MAX_CODEC_NAME_SIZE

internal const val COMPRESSED_MESSAGE_ENVELOPE_VERSION: Byte = 1

private const val STRING_MESSAGE: Byte = 1
private const val BINARY_MESSAGE: Byte = 2

internal fun KrpcTransportMessage.isCompressedEnvelope(): Boolean {
    return when (this) {
        is KrpcTransportMessage.StringMessage -> value.startsWith(STRING_ENVELOPE_PREFIX)
        is KrpcTransportMessage.BinaryMessage -> value.startsWith(COMPRESSED_MESSAGE_MAGIC)
    }
}

/**
 * Thrown when a received compressed kRPC message envelope cannot be decoded.
 */
internal class KrpcCompressionException(
    message: String,
    cause: Throwable? = null,
) : RuntimeException(message, cause)

internal suspend fun KrpcTransportMessage.compressWith(
    compression: KrpcMessageCompression,
    peerMaxDecompressedMessageSize: Int?,
): KrpcTransportMessage {
    // a raw message that starts like an envelope would be misparsed by the receiver,
    // so such messages are always enveloped, never passed through
    val mustEnvelope = isCompressedEnvelope()

    val (messageType, payload) = when (this) {
        is KrpcTransportMessage.StringMessage -> STRING_MESSAGE to encodePayload(mustEnvelope)
        is KrpcTransportMessage.BinaryMessage -> BINARY_MESSAGE to value
    }

    if (!mustEnvelope && payload.size < compression.minimumMessageSize) {
        return this
    }

    // a compliant peer rejects messages that decompress beyond its limit,
    // so send them uncompressed instead, as if compression was not negotiated
    if (peerMaxDecompressedMessageSize != null && payload.size > peerMaxDecompressedMessageSize) {
        if (mustEnvelope) {
            throw KrpcCompressionException(
                "Message starts with the compression envelope prefix, but exceeds the peer's maximum " +
                    "decompressed message size of $peerMaxDecompressedMessageSize bytes and cannot be enveloped"
            )
        }

        return this
    }

    val codecName = compression.codec.name.encodeToByteArray()
    val compressed = try {
        compression.codec.compress(payload)
    } catch (e: CancellationException) {
        throw e
    } catch (e: Exception) {
        if (mustEnvelope) {
            throw KrpcCompressionException("Failed to compress a message that must be enveloped", e)
        }

        // safe to rethrow: the connector falls back to sending the message uncompressed
        throw e
    }

    if (peerMaxDecompressedMessageSize != null && compressed.size > peerMaxDecompressedMessageSize) {
        if (mustEnvelope) {
            throw KrpcCompressionException(
                "Compressed message exceeds the peer's maximum envelope payload size of " +
                    "$peerMaxDecompressedMessageSize bytes"
            )
        }

        return this
    }

    val envelopeSize = COMPRESSED_MESSAGE_MAGIC.size + 3 + codecName.size + compressed.size

    if (!mustEnvelope && this is KrpcTransportMessage.BinaryMessage && envelopeSize >= payload.size) {
        return this
    }

    val result = ByteArray(envelopeSize)
    var offset = 0

    COMPRESSED_MESSAGE_MAGIC.copyInto(result, offset)
    offset += COMPRESSED_MESSAGE_MAGIC.size

    result[offset++] = COMPRESSED_MESSAGE_ENVELOPE_VERSION
    result[offset++] = messageType
    result[offset++] = codecName.size.toByte()

    codecName.copyInto(result, offset)
    offset += codecName.size

    compressed.copyInto(result, offset)

    return when (this) {
        is KrpcTransportMessage.StringMessage -> {
            val envelope = STRING_ENVELOPE_PREFIX + Base64.encode(result)

            // Base64 inflates the envelope by a third, the comparison accounts for it
            if (!mustEnvelope && envelope.length >= value.length) this else KrpcTransportMessage.StringMessage(envelope)
        }

        is KrpcTransportMessage.BinaryMessage -> KrpcTransportMessage.BinaryMessage(result)
    }
}

private fun KrpcTransportMessage.StringMessage.encodePayload(mustEnvelope: Boolean): ByteArray {
    return try {
        value.encodeToByteArray(throwOnInvalidSequence = true)
    } catch (e: CharacterCodingException) {
        if (mustEnvelope) {
            throw KrpcCompressionException(
                "Message starts with the compression envelope prefix, but is not a valid UTF-16 string " +
                    "and cannot be enveloped",
                e,
            )
        }

        // safe to rethrow: the connector falls back to sending the message uncompressed
        throw e
    }
}

internal suspend fun KrpcTransportMessage.decompressWith(compression: KrpcMessageCompression?): KrpcTransportMessage {
    if (compression == null) {
        return this
    }

    val envelope = when (this) {
        is KrpcTransportMessage.StringMessage -> {
            if (!value.startsWith(STRING_ENVELOPE_PREFIX)) {
                return this
            }

            val encodedSize = value.length - STRING_ENVELOPE_PREFIX.length
            val maxEnvelopeSize = MAX_ENVELOPE_HEADER_SIZE + compression.maxDecompressedMessageSize.toLong()
            val maxEncodedSize = ((maxEnvelopeSize + 2) / 3) * 4
            if (encodedSize.toLong() > maxEncodedSize) {
                throw KrpcCompressionException("Compressed kRPC string envelope exceeds the configured size limit")
            }

            try {
                Base64.decode(value, startIndex = STRING_ENVELOPE_PREFIX.length)
            } catch (e: IllegalArgumentException) {
                throw KrpcCompressionException("Compressed kRPC message envelope is not valid Base64", e)
            }
        }

        is KrpcTransportMessage.BinaryMessage -> {
            if (!value.startsWith(COMPRESSED_MESSAGE_MAGIC)) {
                return this
            }

            if (value.size.toLong() > MAX_ENVELOPE_HEADER_SIZE + compression.maxDecompressedMessageSize.toLong()) {
                throw KrpcCompressionException("Compressed kRPC binary envelope exceeds the configured size limit")
            }

            value
        }
    }

    return decompressEnvelope(envelope, compression)
}

private suspend fun decompressEnvelope(
    value: ByteArray,
    compression: KrpcMessageCompression,
): KrpcTransportMessage {
    var offset = COMPRESSED_MESSAGE_MAGIC.size

    if (!value.startsWith(COMPRESSED_MESSAGE_MAGIC) || value.size < offset + 3) {
        throw KrpcCompressionException("Compressed kRPC message envelope is incomplete")
    }

    val version = value[offset++]
    if (version != COMPRESSED_MESSAGE_ENVELOPE_VERSION) {
        throw KrpcCompressionException("Unsupported compressed kRPC message version: $version")
    }

    val messageType = value[offset++]
    if (messageType != STRING_MESSAGE && messageType != BINARY_MESSAGE) {
        throw KrpcCompressionException("Unsupported compressed kRPC message type: $messageType")
    }

    val codecNameSize = value[offset++].toInt() and 0xff

    if (value.size < offset + codecNameSize) {
        throw KrpcCompressionException("Compressed kRPC message codec name is incomplete")
    }

    val codecName = try {
        value.decodeToString(offset, offset + codecNameSize, throwOnInvalidSequence = true)
    } catch (e: CharacterCodingException) {
        throw KrpcCompressionException("Compressed kRPC message codec name is not valid UTF-8", e)
    }
    if (codecName != compression.codec.name) {
        throw KrpcCompressionException(
            "Received compressed kRPC message with codec '$codecName', but negotiated '${compression.codec.name}'"
        )
    }

    offset += codecNameSize

    if (value.size - offset > compression.maxDecompressedMessageSize) {
        throw KrpcCompressionException(
            "Compressed kRPC message exceeds the configured limit of " +
                "${compression.maxDecompressedMessageSize} bytes"
        )
    }

    val compressed = value.copyOfRange(offset, value.size)
    val decompressed = try {
        compression.codec.decompress(
            data = compressed,
            maxDecompressedSize = compression.maxDecompressedMessageSize,
        )
    } catch (e: CancellationException) {
        throw e
    } catch (e: Exception) {
        throw KrpcCompressionException("Failed to decompress kRPC message", e)
    }

    if (decompressed.size > compression.maxDecompressedMessageSize) {
        throw KrpcCompressionException(
            "Decompressed kRPC message exceeds the configured limit of " +
                "${compression.maxDecompressedMessageSize} bytes"
        )
    }

    return when (messageType) {
        STRING_MESSAGE -> {
            val string = try {
                decompressed.decodeToString(throwOnInvalidSequence = true)
            } catch (e: CharacterCodingException) {
                throw KrpcCompressionException("Decompressed kRPC message is not valid UTF-8", e)
            }

            KrpcTransportMessage.StringMessage(string)
        }

        else -> KrpcTransportMessage.BinaryMessage(decompressed)
    }
}

private fun ByteArray.startsWith(prefix: ByteArray): Boolean {
    if (size < prefix.size) {
        return false
    }

    for (i in prefix.indices) {
        if (this[i] != prefix[i]) {
            return false
        }
    }

    return true
}
