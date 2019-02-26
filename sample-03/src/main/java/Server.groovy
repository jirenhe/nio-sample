import java.nio.ByteBuffer
import java.nio.channels.SelectionKey
import java.nio.channels.Selector
import java.nio.channels.ServerSocketChannel
import java.nio.channels.SocketChannel

ServerSocketChannel serverSocketChannel = ServerSocketChannel.open()
serverSocketChannel.bind(new InetSocketAddress("localhost", 8888))
SocketChannel socketChannel = serverSocketChannel.accept()
socketChannel.configureBlocking(false)
Selector selector = Selector.open()
socketChannel.register(selector, SelectionKey.OP_READ)
ByteBuffer byteBuffer = ByteBuffer.allocate(1024)
byteBuffer.put("hello world!".getBytes())
byteBuffer.flip()
socketChannel.write(byteBuffer)
println("send data finish!")
while (true) {
    selector.select()
    Set<SelectionKey> keySet = selector.selectedKeys()
    println keySet.size()
    Iterator<SelectionKey> iterator = keySet.iterator()
    while (iterator.hasNext()) {
        SelectionKey key = iterator.next()
        SocketChannel channel = key.channel() as SocketChannel
        switch (key){
            case { (it as SelectionKey).isReadable()}:
                byteBuffer.clear()
                int length = channel.read(byteBuffer)
                if(length > -1){
                    byteBuffer.flip()
                    byte[] bytes = new byte[length]
                    byteBuffer.get(bytes)
                    println("${key} receive data : ${new String(bytes)}")
                }
                iterator.remove()
                break
        }
    }
}