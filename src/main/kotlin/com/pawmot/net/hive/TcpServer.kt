package com.pawmot.net.hive

import com.pawmot.net.hive.util.loggerFor
import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInitializer
import io.netty.channel.SimpleChannelInboundHandler
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.logging.LogLevel
import io.netty.handler.logging.LoggingHandler
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.CommandLineRunner
import org.springframework.core.annotation.Order
import org.springframework.stereotype.Component

@Component
@Order(1)
class TcpServer(
        @Value("\${hive.address}") private val addr: String?,
        @Value("\${hive.tcpPort}") private val tcpPort: Int)
    : CommandLineRunner {
    companion object {
        private val log = loggerFor<TcpServer>()
    }

    override fun run(vararg args: String?) {
        val eventLoopGroup = NioEventLoopGroup(1)
        val childEventLoopGroup = NioEventLoopGroup()
        val bootstrap = ServerBootstrap()

        bootstrap
                .group(eventLoopGroup, childEventLoopGroup)
                .channel(NioServerSocketChannel::class.java)
                .localAddress(addr ?: "0.0.0.0", tcpPort)
                .handler(LoggingHandler(LogLevel.DEBUG))
                .childHandler(object : ChannelInitializer<NioSocketChannel>() {
                    override fun initChannel(ch: NioSocketChannel) {
                        ch.pipeline().addFirst(object : SimpleChannelInboundHandler<ByteBuf>() {
                            override fun channelRead0(ctx: ChannelHandlerContext, msg: ByteBuf) {
                                msg.retain(1)
                                ctx.writeAndFlush(msg)
                            }
                        })
                    }

                })

        log.debug("Trying to bind to tcp://$addr:$tcpPort...")
        val bound = bootstrap.bind().addListener { log.info("Listening on tcp://$addr:$tcpPort!") }
        bound.channel().closeFuture().addListener {
            eventLoopGroup.shutdownGracefully().sync()
        }
    }
}