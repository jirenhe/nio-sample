package com.wanshifu.transformers.common.remote.netty;

import com.wanshifu.transformers.common.remote.client.RpcClient;
import com.wanshifu.transformers.common.remote.netty.code.NettyDecoder;
import com.wanshifu.transformers.common.remote.netty.code.NettyEncoder;
import com.wanshifu.transformers.common.remote.protocol.RpcRequest;
import com.wanshifu.transformers.common.remote.protocol.RpcResponse;
import com.wanshifu.transformers.common.remote.protocol.serialize.Serializer;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class NettyRpcClient implements RpcClient {

    private final EventLoopGroup group = new NioEventLoopGroup();

    private final int timeout;

    private Channel channel;

    private final String remoteAddress;

    private final int port;

    private final Serializer serializer;

    private LinkedBlockingQueue<RpcRequest> rpcRequests = new LinkedBlockingQueue<>(10);

    public NettyRpcClient(String remoteAddress, int port, Serializer serializer) {
        this(remoteAddress, port, serializer, 2);
    }

    public NettyRpcClient(String remoteAddress, int port, Serializer serializer, int timeout) {
        this.remoteAddress = remoteAddress;
        this.port = port;
        this.serializer = serializer;
        this.timeout = timeout;
    }

    @Override
    public boolean isAlive() {
        return false;
    }

    @Override
    public void connect() throws Exception {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel channel) throws Exception {
                        channel.pipeline()
                                .addLast(new NettyDecoder(serializer, RpcResponse.class))
                                .addLast(new NettyEncoder(serializer, RpcRequest.class))
                                .addLast(new IdleStateHandler(10, 10, 10, TimeUnit.SECONDS))
                                .addLast(new SimpleChannelInboundHandler<RpcResponse>() {

                                    @Override
                                    protected void channelRead0(ChannelHandlerContext ctx, RpcResponse rpcResponse) throws Exception {

                                    }
                                });
                    }
                })
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000);
        this.channel = bootstrap.connect(remoteAddress, port).sync().channel();
    }

    @Override
    public void close() {

    }

    @Override
    public RpcResponse send(RpcRequest rpcRequest) {
        this.channel.writeAndFlush(rpcRequest).syncUninterruptibly();

    }

}
