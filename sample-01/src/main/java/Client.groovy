import java.nio.ByteBuffer
import java.nio.channels.SocketChannel

SocketChannel channel = SocketChannel.open(new InetSocketAddress("localhost",8888))
ByteBuffer buffer = ByteBuffer.allocate(1024)
buffer.put("hello world!".getBytes())
buffer.flip()
channel.write(buffer)
channel?.close()
