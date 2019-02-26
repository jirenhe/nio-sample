import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.ChannelInitializer
import io.netty.channel.EventLoopGroup
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.serialization.ObjectDecoder
import io.netty.handler.codec.serialization.ObjectEncoder
import io.netty.handler.timeout.IdleStateHandler

import java.util.concurrent.TimeUnit

ServerBootstrap bootstrap = new ServerBootstrap()
EventLoopGroup eventLoopGroup = new NioEventLoopGroup()
InboundHandler handler = new InboundHandler()
bootstrap.group(eventLoopGroup)
        .channel(NioServerSocketChannel)
        .localAddress("localhost", 8888)
        .childHandler(new ChannelInitializer<SocketChannel>() {
    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline()
                .addLast(new ObjectDecoder(new MapClassResolver()))
                .addLast(new ObjectEncoder())
                .addLast(new IdleStateHandler(10, 10, 10, TimeUnit.SECONDS))
                .addLast(handler)
    }
})

bootstrap.bind().sync()
println "server start"