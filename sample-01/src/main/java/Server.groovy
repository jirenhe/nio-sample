import java.nio.ByteBuffer
import java.nio.channels.ServerSocketChannel
import java.nio.channels.SocketChannel

ServerSocketChannel serverSocketChannel = ServerSocketChannel.open()
//serverSocketChannel.configureBlocking(false)
serverSocketChannel.bind(new InetSocketAddress("localhost", 8888))
SocketChannel channel = serverSocketChannel.accept()
ByteBuffer buffer = ByteBuffer.allocateDirect(1024)
println buffer.hasArray()
int length = channel.read(buffer)
byte[] bytes = new byte[length]
buffer.flip()
buffer.get(bytes)
println new String(bytes)
channel?.close()
serverSocketChannel.close()
