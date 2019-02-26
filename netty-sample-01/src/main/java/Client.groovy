import io.netty.bootstrap.Bootstrap
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.*
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.util.CharsetUtil

EventLoopGroup eventLoopGroup = new NioEventLoopGroup()
Bootstrap bootstrap = new Bootstrap()
bootstrap.group(eventLoopGroup)
        .channel(NioSocketChannel)
        .remoteAddress("localhost", 8888)
        .handler(new ChannelInitializer<SocketChannel>() {
    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline().addLast(new SimpleChannelInboundHandler<ByteBuf>() {

            @Override
            void channelActive(ChannelHandlerContext ctx) throws Exception {
                ctx.writeAndFlush(Unpooled.copiedBuffer("hello world!", CharsetUtil.UTF_8))
            }

            @Override
            void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                cause.printStackTrace()
                ctx.close()
            }

            @Override
            protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
                println "client recevied : ${msg.toString(CharsetUtil.UTF_8)}"
            }
        })
    }
})
ChannelFuture future = bootstrap.connect().sync()
future.channel().closeFuture().sync()
eventLoopGroup.shutdownGracefully().sync()