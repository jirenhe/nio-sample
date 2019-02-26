import java.nio.channels.SocketChannel
import java.util.concurrent.TimeUnit

SocketChannel channel1 = SocketChannel.open(new InetSocketAddress("localhost", 8888))
channel1.close()
TimeUnit.SECONDS.sleep(2)
SocketChannel channel2 = SocketChannel.open(new InetSocketAddress("localhost", 7777))
channel2.close()