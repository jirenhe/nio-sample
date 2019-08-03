package com.wanshifu.transformers.common.remote.netty.server;

import com.wanshifu.transformers.common.remote.RpcException;
import com.wanshifu.transformers.common.remote.netty.code.NettyDecoder;
import com.wanshifu.transformers.common.remote.netty.code.NettyEncoder;
import com.wanshifu.transformers.common.remote.protocol.RpcRequest;
import com.wanshifu.transformers.common.remote.protocol.RpcResponse;
import com.wanshifu.transformers.common.remote.protocol.serialize.Serializer;
import com.wanshifu.transformers.common.remote.server.RequestProcessor;
import com.wanshifu.transformers.common.remote.server.RpcServer;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;

import java.util.concurrent.TimeUnit;

public class NettyRpcServer implements RpcServer {

    private final ServerBootstrap bootstrap = new ServerBootstrap();

    private final EventLoopGroup eventLoopGroup = new NioEventLoopGroup();

    private final Serializer serializer;

    public NettyRpcServer(String localAddress, int port, Serializer serializer) {
        this.serializer = serializer;
        bootstrap.group(eventLoopGroup)
                .channel(NioServerSocketChannel.class)
                .localAddress(localAddress, port);
    }

    @Override
    public void start(final RequestProcessor requestProcessor) throws RpcException {
        bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline()
                        .addLast(new NettyDecoder(serializer, RpcRequest.class))
                        .addLast(new NettyEncoder(serializer, RpcResponse.class))
                        .addLast(new IdleStateHandler(10, 10, 10, TimeUnit.SECONDS))
                        .addLast(new SimpleChannelInboundHandler<RpcRequest>() {

                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, RpcRequest msg) throws Exception {
                                RpcResponse rpcResponse = requestProcessor.action(msg);
                                ctx.writeAndFlush(rpcResponse);
                            }

                            @Override
                            public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
                                System.out.println("ssssss");
                            }

                            @Override
                            public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
                                System.out.println("zzzzzzzzzzzz");
                                super.userEventTriggered(ctx, evt);
                            }

                            @Override
                            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                                System.out.println("wwwwwwwwwwwww");
                            }
                        });
            }
        });
        try {
            bootstrap.bind().sync();
        } catch (InterruptedException e) {
            throw new RpcException("server bind fail", e);
        }
    }

    @Override
    public void shutdown() {
        eventLoopGroup.shutdownGracefully();
    }
}
