/*
 * Copyright 2023-2026 JetBrains s.r.o and contributors. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.rpc.krpc

/**
 * Compresses serialized kRPC transport messages.
 */
public interface KrpcMessageCodec {
    /**
     * Stable codec name sent in the kRPC handshake.
     */
    public val name: String

    /**
     * Compresses [data].
     */
    public suspend fun compress(data: ByteArray): ByteArray

    /**
     * Decompresses [data].
     *
     * Implementations must fail before producing more than [maxDecompressedSize] bytes.
     */
    public suspend fun decompress(data: ByteArray, maxDecompressedSize: Int): ByteArray
}

/**
 * kRPC transport message compression settings.
 *
 * Compression is used only when both endpoints configure a codec with the same [KrpcMessageCodec.name].
 *
 * @param codec codec used for compressed messages.
 * @param minimumMessageSize minimum serialized message size in bytes before compression is attempted.
 * @param maxDecompressedMessageSize maximum decompressed message size accepted from a peer.
 * The limit is advertised during the handshake, and peers send larger messages uncompressed instead.
 */
public class KrpcMessageCompression(
    public val codec: KrpcMessageCodec,
    public val minimumMessageSize: Int = DEFAULT_MINIMUM_MESSAGE_SIZE,
    public val maxDecompressedMessageSize: Int = DEFAULT_MAX_DECOMPRESSED_MESSAGE_SIZE,
) {
    init {
        val encodedNameSize = try {
            codec.name.encodeToByteArray(throwOnInvalidSequence = true).size
        } catch (e: CharacterCodingException) {
            throw IllegalArgumentException("Compression codec name must be valid UTF-8", e)
        }

        require(encodedNameSize in 1..255) {
            "Compression codec name must be 1..255 UTF-8 bytes, but was $encodedNameSize"
        }
        require(minimumMessageSize >= 0) {
            "minimumMessageSize must be non-negative"
        }
        require(maxDecompressedMessageSize > 0) {
            "maxDecompressedMessageSize must be positive"
        }
    }

    public companion object {
        /**
         * Default minimum serialized message size before compression is attempted.
         */
        public const val DEFAULT_MINIMUM_MESSAGE_SIZE: Int = 1024

        /**
         * Default maximum decompressed transport message size.
         */
        public const val DEFAULT_MAX_DECOMPRESSED_MESSAGE_SIZE: Int = 64 * 1024 * 1024
    }
}
