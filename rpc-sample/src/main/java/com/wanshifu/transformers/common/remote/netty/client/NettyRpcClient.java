package com.wanshifu.transformers.common.remote.netty.client;

import com.wanshifu.transformers.common.remote.RpcException;
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

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class NettyRpcClient implements RpcClient {

    private static final EventLoopGroup GROUP = new NioEventLoopGroup();

    private final int timeout;

    private Channel channel;

    private final String remoteAddress;

    private final int port;

    private final Serializer serializer;

    private final Semaphore semaphore;

    private long lastSendTime;

    private final Map<String, Signal> signalMap = new ConcurrentHashMap<>();

    public NettyRpcClient(String remoteAddress, int port, Serializer serializer) {
        this(remoteAddress, port, serializer, 2);
    }

    public NettyRpcClient(String remoteAddress, int port, Serializer serializer, int timeout) {
        this(remoteAddress, port, serializer, timeout, 5);
    }

    public NettyRpcClient(String remoteAddress, int port, Serializer serializer, int timeout, int maxWait) {
        this.remoteAddress = remoteAddress;
        this.port = port;
        this.serializer = serializer;
        this.timeout = timeout;
        semaphore = new Semaphore(maxWait);
    }

    @Override
    public boolean isAlive() {
        return this.channel != null && this.channel.isActive();
    }

    @Override
    public void connect() throws RpcException {
        if (!isAlive()) {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(GROUP)
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
                                            String requestId = rpcResponse.getRequestId();
                                            final Signal signal = NettyRpcClient.this.signalMap.remove(requestId);
                                            if (signal != null) {
                                                signal.notifyWaiter(rpcResponse);
                                            }
                                        }
                                    });
                        }
                    })
                    .option(ChannelOption.TCP_NODELAY, true)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000);
            try {
                this.channel = bootstrap.connect(remoteAddress, port).sync().channel();
            } catch (InterruptedException e) {
                throw new RpcException(String.format("connect remote : %s fail", remoteAddress), e);
            }
        }
    }

    @Override
    public void close() {
        try {
            if (this.channel != null) {
                this.channel.close().sync();
            }
        } catch (InterruptedException ignore) {
        }
    }

    @Override
    public RpcResponse send(RpcRequest rpcRequest) throws RpcException {
        try {
            semaphore.acquire();
            this.channel.writeAndFlush(rpcRequest).sync();
            Signal signal = new Signal();
            signalMap.put(rpcRequest.getRequestId(), signal);
            RpcResponse rpcResponse = signal.waitResponse(this.timeout);
            lastSendTime = System.currentTimeMillis();
            return rpcResponse;
        } catch (InterruptedException e) {
            return null;
        } finally {
            semaphore.release();
        }
    }

    public long lastActiveTime() {
        return lastSendTime;
    }

    private static class Signal {

        private final Object lock = new Object();

        private volatile RpcResponse rpcResponse;

        private RpcResponse waitResponse(int timeout) {
            synchronized (lock) {
                while (rpcResponse == null) {
                    try {
                        lock.wait(timeout);
                    } catch (InterruptedException ignore) {
                    }
                }
                return rpcResponse;
            }
        }

        private void notifyWaiter(RpcResponse rpcResponse) {
            Objects.requireNonNull(rpcResponse);
            synchronized (lock) {
                this.rpcResponse = rpcResponse;
                lock.notifyAll();
            }
        }
    }

}
