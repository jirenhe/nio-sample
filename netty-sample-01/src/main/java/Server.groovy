import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.*
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.util.CharsetUtil

EventLoopGroup eventLoop = new NioEventLoopGroup()
ServerBootstrap bootstrap = new ServerBootstrap()
bootstrap.group(eventLoop)
        .localAddress("localhost", 8888)
        .channel(NioServerSocketChannel)
        .childHandler(new ChannelInitializer<SocketChannel>() {
    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline().addLast(new EchoServerHandler()).addLast(new OutboundServerHandler())
    }
})
//(6) 异步地绑定服务器；调用 sync()方法阻塞等待直到绑定完成
ChannelFuture f = bootstrap.bind().sync()
System.out.println(" started and listening for connections on " + f.channel().localAddress())
//(7) 获取 Channel 的CloseFuture，并且阻塞当前线程直到它完成
f.channel().closeFuture().sync()
//(8) 关闭 EventLoopGroup，释放所有的资源
eventLoop.shutdownGracefully().sync()

@Sharable
public class EchoServerHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        ByteBuf bin = (ByteBuf) msg
        //将消息记录到控制台
        System.out.println(
                "Server received: " + bin.toString(CharsetUtil.UTF_8))
        //将接收到的消息写给发送者，而不冲刷出站消息
        ctx.write(bin)
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx)
            throws Exception {
        //将未决消息冲刷到远程节点，并且关闭该 Channel
        ctx.writeAndFlush(Unpooled.EMPTY_BUFFER)
                .addListener(ChannelFutureListener.CLOSE)
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx,
                                Throwable cause) {
        //打印异常栈跟踪
        cause.printStackTrace()
        //关闭该Channel
        ctx.close()
    }
}

@Sharable
public class OutboundServerHandler extends ChannelOutboundHandlerAdapter {

    @Override
    void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        println "${ctx.channel().class.name} disconnect"
        super.disconnect(ctx, promise)
    }

    @Override
    void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        println "${ctx.channel().class.name} close"
        super.close(ctx, promise)
    }

    @Override
    void deregister(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        println "${ctx.channel().class.name} deregister"
        super.deregister(ctx, promise)
    }

    @Override
    void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        println "${ctx.channel().class.name} write"
        super.write(ctx, msg, promise)
    }

    @Override
    void flush(ChannelHandlerContext ctx) throws Exception {
        println "${ctx.channel().class.name} flush"
        super.flush(ctx)
    }

    @Override
    void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        println "${ctx.channel().class.name} handlerRemoved"
        super.handlerRemoved(ctx)
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx,
                                Throwable cause) {
        //打印异常栈跟踪
        cause.printStackTrace()
        //关闭该Channel
        ctx.close()
    }
}