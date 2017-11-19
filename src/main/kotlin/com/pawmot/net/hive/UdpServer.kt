package com.pawmot.net.hive

import com.pawmot.net.hive.util.loggerFor
import io.netty.bootstrap.Bootstrap
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInitializer
import io.netty.channel.SimpleChannelInboundHandler
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.DatagramPacket
import io.netty.channel.socket.nio.NioDatagramChannel
import io.netty.handler.logging.LoggingHandler
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.CommandLineRunner
import org.springframework.core.annotation.Order
import org.springframework.stereotype.Component

@Component
@Order(2)
class UdpServer(
        @Value("\${hive.address}") private val addr: String?,
        @Value("\${hive.udpPort}") private val udpPort: Int)
    : CommandLineRunner {
    companion object {
        private val log = loggerFor<UdpServer>()
    }

    override fun run(vararg args: String?) {
        val eventLoopGroup = NioEventLoopGroup(1)
        val udpBootstrap = Bootstrap()

        udpBootstrap
                .group(eventLoopGroup)
                .channel(NioDatagramChannel::class.java)
                .localAddress(addr ?: "0.0.0.0", udpPort)
                .handler(object : ChannelInitializer<NioDatagramChannel>() {
                    override fun initChannel(ch: NioDatagramChannel) {
                        ch.pipeline().addLast(LoggingHandler())
                        ch.pipeline().addLast(object : SimpleChannelInboundHandler<DatagramPacket>() {
                            override fun channelRead0(ctx: ChannelHandlerContext, msg: DatagramPacket) {
                                msg.retain(1)
                                ctx.writeAndFlush(DatagramPacket(msg.content(), msg.sender()))
                            }
                        })
                    }
                })

        log.debug("Trying to bind to udp://$addr:$udpPort...")
        val bound = udpBootstrap.bind()
                .addListener { log.info("Listening on udp://$addr:$udpPort!") }

        bound.channel().closeFuture().addListener {
            eventLoopGroup.shutdownGracefully().sync()
        }
    }
}