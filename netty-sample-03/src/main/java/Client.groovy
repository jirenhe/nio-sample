import groovy.transform.Field
import io.netty.bootstrap.Bootstrap
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInitializer
import io.netty.channel.EventLoopGroup
import io.netty.channel.SimpleChannelInboundHandler
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.codec.serialization.ObjectDecoder
import io.netty.handler.codec.serialization.ObjectEncoder

@Field static boolean register = false
@Field static String id = false
EventLoopGroup eventLoopGroup = new NioEventLoopGroup()
Bootstrap bootstrap = new Bootstrap()
SocketChannel channel = bootstrap.group(eventLoopGroup)
        .remoteAddress("localhost", 8888)
        .channel(NioSocketChannel)
        .handler(new ChannelInitializer<SocketChannel>() {
    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline().addLast(new ObjectDecoder(new MapClassResolver())).addLast(new ObjectEncoder()).addLast(new MessageHandler())
    }
}).connect().sync().channel() as SocketChannel

Scanner scanner = new Scanner(System.in)
synchronized (Client.class) {
    while (!register) {
        println "please enter a id:"
        def str = scanner.nextLine()
        channel.writeAndFlush([
                "topic"  : "REGISTER",
                "content": str,
        ]).sync()
        Client.class.wait()
    }
}
for (; ;) {
    def str = scanner.nextLine()
    channel.writeAndFlush([
            "topic"  : "TALK",
            "content": str,
    ]).sync()
}

class MessageHandler extends SimpleChannelInboundHandler {

    @Override
    void channelActive(ChannelHandlerContext ctx) throws Exception {
        println "channel active"
        super.channelActive(ctx)
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
        if ("HEARTBEAT" != msg) {
            if (msg instanceof Map) {
                if (msg.topic == "REGISTER_SUCCESS") {
                    synchronized (Client.class){
                        Client.register = true
                        Client.id = msg.content
                        Client.class.notifyAll()
                    }
                    println "REGISTER_SUCCESS!"
                } else {
                    println msg.content
                }
            }
        }
    }

    @Override
    void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        ctx.close()
        println "lost connect!"
        System.exit(0)
    }
}