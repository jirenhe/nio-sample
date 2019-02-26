import groovy.json.JsonOutput
import io.netty.channel.ChannelFutureListener
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler
import io.netty.channel.socket.SocketChannel
import io.netty.handler.timeout.IdleStateEvent

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.regex.Matcher
import java.util.regex.Pattern

@Sharable
class InboundHandler extends SimpleChannelInboundHandler<Map> {

    private static final String HEARTBEAT_SEQUENCE = "HEARTBEAT"

    private ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors())

    private ClientsHolderGroup clientsHolderGroup = new ClientsHolderGroup()

    @Override
    void channelActive(ChannelHandlerContext ctx) throws Exception {
        println "channel active!"
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Map map) throws Exception {
        executorService.execute(new MessageHandler(ctx, map))
    }

    @Override
    void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            //发送心跳消息，并在发送失败时关闭该连接
            println "HEARTBEAT SEND!"
            ctx.writeAndFlush(HEARTBEAT_SEQUENCE).addListener(ChannelFutureListener.CLOSE_ON_FAILURE)
        } else {
            //不是 IdleStateEvent 事件，所以将它传递给下一个 ChannelInboundHandler
            super.userEventTriggered(ctx, evt)
        }
    }

    @Override
    void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        clientsHolderGroup.remove(ctx.channel() as SocketChannel)
    }

    @Override
    void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        clientsHolderGroup.remove(ctx.channel() as SocketChannel)
        println "channel unregistered"
        ctx.fireChannelUnregistered()
    }

    class MessageHandler implements Runnable {

        private ChannelHandlerContext ctx

        private Map msg

        MessageHandler(ChannelHandlerContext ctx, Map msg) {
            this.ctx = ctx
            this.msg = msg
        }

        @Override
        void run() {
            SocketChannel channel = ctx.channel() as SocketChannel
            println "revice msg : ${JsonOutput.toJson(msg)}"
            if (msg.topic == "REGISTER") {
                String id = msg.content
                synchronized (InboundHandler.class) {
                    if (clientsHolderGroup.find(id) == null) {
                        clientsHolderGroup.add(new ClientHolder(channel, id))
                        channel.writeAndFlush([
                                "topic"  : "REGISTER_SUCCESS",
                                "content": id
                        ])
                    } else {
                        channel.writeAndFlush([
                                "topic"  : "REGISTER_FAILURE",
                                "content": "user id is already exist!"
                        ])
                    }
                }
            } else if (msg.topic == "TALK") {
                ClientHolder clientHolder = clientsHolderGroup.find(channel)
                if (clientHolder) {
                    String content = msg.content
                    Matcher matcher = Pattern.compile("(@)(\\S*)(\\s)").matcher(content)
                    if (matcher.find()) {
                        def to = matcher.group(2)
                        clientsHolderGroup.find(to)?.channel?.writeAndFlush([
                                topic  : "TALK",
                                content: "from ${clientHolder.id} say to you : ${content.substring(to.length() + 2, content.length())}"
                        ])
                    }else{
                        clientsHolderGroup.writeExcept([
                                topic  : "TALK",
                                content: "from ${clientHolder.id} say to everybody : $content"
                        ], clientHolder)
                    }
                }
            } else if (msg.topic == "OFF_LINE") {
                clientsHolderGroup.remove(channel)
            }
        }
    }
}