/*
 * Copyright 2023-2026 JetBrains s.r.o and contributors. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.rpc.krpc.test

import kotlinx.rpc.annotations.Rpc
import kotlinx.rpc.krpc.KrpcMessageCodec
import kotlinx.rpc.krpc.KrpcMessageCompression
import kotlinx.rpc.krpc.compression.zstd.ZstdKrpcMessageCodec
import kotlinx.rpc.krpc.compression.zstd.zstd
import kotlinx.rpc.krpc.rpcClientConfig
import kotlinx.rpc.krpc.rpcServerConfig
import kotlinx.rpc.krpc.serialization.json.json
import kotlinx.rpc.registerService
import kotlinx.rpc.withService
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@Rpc
interface ZstdEchoService {
    suspend fun echo(value: String): String
}

private class ZstdEchoServiceImpl : ZstdEchoService {
    override suspend fun echo(value: String): String = value
}

private class CountingCodec(private val delegate: KrpcMessageCodec) : KrpcMessageCodec {
    var compressions = 0
        private set

    var decompressions = 0
        private set

    override val name: String get() = delegate.name

    override suspend fun compress(data: ByteArray): ByteArray {
        compressions++

        return delegate.compress(data)
    }

    override suspend fun decompress(data: ByteArray, maxDecompressedSize: Int): ByteArray {
        decompressions++

        return delegate.decompress(data, maxDecompressedSize)
    }
}

class ZstdCompressionTest : ProtocolTestBase() {
    @Test
    fun `large payloads round trip through real zstd on both endpoints`() {
        // counting wrapper around the real codec, so the test can prove compression actually engaged
        val clientCodec = CountingCodec(ZstdKrpcMessageCodec())

        val clientConfig = rpcClientConfig {
            serialization {
                json()
            }

            connector {
                messageCompression = KrpcMessageCompression(clientCodec)
            }
        }

        val serverConfig = rpcServerConfig {
            serialization {
                json()
            }

            connector {
                zstd()
            }
        }

        runTest(clientConfig, serverConfig) {
            defaultServer.registerService<ZstdEchoService> { ZstdEchoServiceImpl() }
            val echo = defaultClient.withService<ZstdEchoService>()

            val payload = "kotlinx-rpc zstd end-to-end ".repeat(10_000)
            assertEquals(payload, echo.echo(payload))

            // the client compressed the request and decompressed the response
            assertTrue(clientCodec.compressions > 0)
            assertTrue(clientCodec.decompressions > 0)
        }
    }
}
