import java.nio.channels.SelectionKey
import java.nio.channels.Selector
import java.nio.channels.ServerSocketChannel
import java.nio.channels.SocketChannel

ServerSocketChannel serverSocketChannel1 = ServerSocketChannel.open()
ServerSocketChannel serverSocketChannel2 = ServerSocketChannel.open()
serverSocketChannel1.configureBlocking(false)
serverSocketChannel2.configureBlocking(false)
serverSocketChannel1.bind(new InetSocketAddress("localhost", 8888))
serverSocketChannel2.bind(new InetSocketAddress("localhost", 7777))
Selector selector = Selector.open()
serverSocketChannel1.register(selector, SelectionKey.OP_ACCEPT)
serverSocketChannel2.register(selector, SelectionKey.OP_ACCEPT)
while (true) {
    selector.select()
    Set<SelectionKey> keySet = selector.selectedKeys()
    Iterator<SelectionKey> iterator = keySet.iterator()
    while (iterator.hasNext()) {
        SelectionKey key = iterator.next()
        ServerSocketChannel channel = key.channel() as ServerSocketChannel
        SocketChannel socketChannel = channel.accept()
        println channel.getLocalAddress().toString() + "---" + socketChannel?.toString()
        iterator.remove()
    }
}