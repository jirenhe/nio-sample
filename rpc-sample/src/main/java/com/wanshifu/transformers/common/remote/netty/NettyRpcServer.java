package com.wanshifu.transformers.common.remote.netty;

import com.wanshifu.transformers.common.remote.server.RequestProcessor;
import com.wanshifu.transformers.common.remote.server.RpcServer;
import com.wanshifu.transformers.common.remote.netty.code.NettyDecoder;
import com.wanshifu.transformers.common.remote.netty.code.NettyEncoder;
import com.wanshifu.transformers.common.remote.protocol.RpcRequest;
import com.wanshifu.transformers.common.remote.protocol.RpcResponse;
import com.wanshifu.transformers.common.remote.protocol.serialize.Serializer;
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
    public void start(final RequestProcessor requestProcessor) throws Exception {
        bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline()
                        .addLast(new NettyDecoder(serializer, RpcRequest.class))
                        .addLast(new NettyEncoder(serializer, RpcResponse.class))
                        .addLast(new IdleStateHandler(10, 10, 10, TimeUnit.SECONDS))
                        .addLast(new SimpleChannelInboundHandler<RpcRequest>(){

                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, RpcRequest msg) throws Exception {
                                RpcResponse rpcResponse = requestProcessor.action(msg);
                                ctx.writeAndFlush(rpcResponse);
                            }
                        });
            }
        });
        bootstrap.bind().sync();
    }

    @Override
    public void shutdown() {
        eventLoopGroup.shutdownGracefully();
    }
}
