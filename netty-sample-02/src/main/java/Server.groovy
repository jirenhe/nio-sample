import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import io.netty.channel.ChannelInitializer
import io.netty.channel.EventLoopGroup
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.util.CharsetUtil

EventLoopGroup eventLoopGroup = new NioEventLoopGroup()
ServerBootstrap bootstrap = new ServerBootstrap()
bootstrap.channel(NioServerSocketChannel)
        .group(eventLoopGroup)
        .localAddress("localhost", 8888)
        .childHandler(new ChannelInitializer<SocketChannel>() {
    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline()
                .addLast(new MyDeEncodeHandler())
                .addLast(new MyHandler())
    }
})
bootstrap.bind().sync()


class MyDeEncodeHandler extends ChannelInboundHandlerAdapter {

    @Override
    void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf bin = (ByteBuf) msg
        String str = bin.toString(CharsetUtil.UTF_8)
        if (str.startsWith("abcdefg")) {
            ctx.fireChannelRead(str.substring(7))
        } else {
            ctx.close().sync()
        }
    }
}

class MyHandler extends ChannelInboundHandlerAdapter {

    @Override
    void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx)
        println "connected from client!"
    }

    @Override
    void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        println msg
    }

    @Override
    void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        ctx.close().sync()
    }
}