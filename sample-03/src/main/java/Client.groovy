import java.nio.ByteBuffer
import java.nio.channels.SocketChannel
import java.util.concurrent.TimeUnit

SocketChannel channel = SocketChannel.open(new InetSocketAddress("localhost", 8888))
ByteBuffer byteBuffer = ByteBuffer.allocate(1024)
int length = channel.read(byteBuffer)
byteBuffer.flip()
byte[] bytes = new byte[length]
byteBuffer.get(bytes)
println("receive data : ${new String(bytes)}")

int i = 0
while (true) {
    byteBuffer.clear()
    byteBuffer.put("hello world! ${++i}".getBytes())
    byteBuffer.flip()
    channel.write(byteBuffer)
    println("send data finish!")
    TimeUnit.SECONDS.sleep(2)
}
channel.close()