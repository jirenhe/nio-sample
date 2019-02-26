import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import groovy.transform.Field

import java.nio.ByteBuffer
import java.nio.channels.SelectionKey
import java.nio.channels.Selector
import java.nio.channels.ServerSocketChannel
import java.nio.channels.SocketChannel

@Field Selector selector = Selector.open()
@Field def allClients = [:]
@Field ServerSocketChannel serverSocketChannel = ServerSocketChannel.open()
serverSocketChannel.bind(new InetSocketAddress("localhost", 8888))
serverSocketChannel.configureBlocking(false)
serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT)
while (true) {
    selector.select()
    Set<SelectionKey> keySet = selector.selectedKeys()
    Iterator<SelectionKey> iterator = keySet.iterator()
    while (iterator.hasNext()) {
        SelectionKey key = iterator.next()
        def targetChannel = key.channel()
        if (targetChannel instanceof ServerSocketChannel) {
            if (key.isAcceptable()) {
                SocketChannel channel = targetChannel.accept()
                channel.configureBlocking(false)
                channel.register(selector, SelectionKey.OP_READ)
                ByteBuffer byteBuffer = ByteBuffer.allocate(1024)
                def client = [
                        id    : UUID.randomUUID(),
                        buffer: byteBuffer
                ]
                allClients.put(channel, client)
                byteBuffer.put(JsonOutput.toJson([
                        data : [
                                onlineCount : allClients.size(),
                                id : client.id
                        ],
                        topic: "clients-size"
                ]).getBytes())
                byteBuffer.flip()
                channel.write(byteBuffer)
                println "${client.id} on line current clients size : ${allClients.size()}"
            }
        } else {
            targetChannel = targetChannel as SocketChannel
            if (!targetChannel.isConnected()) {
                off(null, targetChannel, null)
            } else if (key.isReadable()) {
                def client = allClients.get(targetChannel)
                if (client) {
                    ByteBuffer byteBuffer = client.buffer
                    byteBuffer.clear()
                    int length = targetChannel.read(byteBuffer)
                    byteBuffer.flip()
                    byte[] bytes = new byte[length]
                    byteBuffer.get(bytes)
                    JsonSlurper jsonSlurper = new JsonSlurper()
                    def msg = jsonSlurper.parse(bytes)
                    println "recever data : ${new String(bytes)} from ${client.id}"
                    this.invokeMethod(msg.topic, [msg, targetChannel, client.id])
                }
            }
        }
        iterator.remove()
    }
}

def off(def msg, SocketChannel socketChannel, clientId) {
    socketChannel.close()
    allClients.remove(socketChannel)
}

def talk(def msg, SocketChannel socketChannel, clientId) {
    byte[] bytes = JsonOutput.toJson([
            topic:"talk",
            data : [
                    content : msg.data.toString(),
                    from: clientId
            ]
    ]).getBytes()
    allClients.findAll {
        it.key != socketChannel
    }.each {
        SocketChannel otherChannel = it.key
        if (!otherChannel.isConnected()) {
            off(null, otherChannel, clientId)
        }
        def client = it.value
        ByteBuffer byteBuffer = client.buffer
        byteBuffer.clear()
        byteBuffer.put(bytes)
        byteBuffer.flip()
        otherChannel.write(byteBuffer)
    }
}