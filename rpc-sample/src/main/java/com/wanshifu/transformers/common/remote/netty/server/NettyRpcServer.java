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
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

public class NettyRpcServer implements RpcServer {

    private final ServerBootstrap bootstrap = new ServerBootstrap();

    private final EventLoopGroup eventloopGroup = new NioEventLoopGroup();

    private final Serializer serializer;

    private ServerSocketChannel serverSocketChannel;

    public NettyRpcServer(String localAddress, int port, Serializer serializer) {
        this.serializer = serializer;
        bootstrap.group(eventloopGroup)
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
                        .addLast(new SimpleChannelInboundHandler<RpcRequest>() {

                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, RpcRequest msg) throws Exception {
                                RpcResponse rpcResponse = requestProcessor.action(msg);
                                ctx.writeAndFlush(rpcResponse);
                            }
                        });
            }
        });
        try {
            serverSocketChannel = (ServerSocketChannel) bootstrap.bind().sync().channel();
        } catch (InterruptedException e) {
            throw new RpcException("server bind fail", e);
        }
    }

    @Override
    public void shutdown() {
        eventloopGroup.shutdownGracefully();
        if (serverSocketChannel.isActive()) {
            try {
                serverSocketChannel.close().sync();
            } catch (InterruptedException ignore) {
            }
        }
    }
}
