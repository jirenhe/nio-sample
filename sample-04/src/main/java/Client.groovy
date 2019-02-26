import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import groovy.transform.Field

import java.nio.ByteBuffer
import java.nio.channels.SelectionKey
import java.nio.channels.Selector
import java.nio.channels.SocketChannel

@Field Selector selector = Selector.open()
@Field SocketChannel channel = SocketChannel.open(new InetSocketAddress("localhost", 8888))
@Field ByteBuffer readBuffer = ByteBuffer.allocate(1024)
@Field ByteBuffer writeBuffer = ByteBuffer.allocate(1024)
channel.configureBlocking(false)
channel.register(selector, SelectionKey.OP_READ)
new Thread({
    while(true){
        selector.select()
        Set<SelectionKey> keySet = selector.selectedKeys()
        Iterator<SelectionKey> iterator = keySet.iterator()
        while (iterator.hasNext()) {
            SelectionKey key = iterator.next()
            if (key.isReadable()) {
                readBuffer.clear()
                int length = channel.read(readBuffer)
                readBuffer.flip()
                byte[] bytes = new byte[length]
                readBuffer.get(bytes)
                JsonSlurper jsonSlurper = new JsonSlurper()
                def msg = jsonSlurper.parse(bytes)
                if (msg.topic == "clients-size") {
                    println "online your id is : ${msg.data.id} current clines size : ${msg.data.onlineCount}"
                } else {
                    def from = msg.data.from
                    def content = msg.data.content
                    println "talk from ${from} say : 【${content}】"
                }
            }
        }
    }
}).start()
while (true) {
    Scanner scanner = new Scanner(System.in)
    def msg = scanner.nextLine()
    if (msg == "off") {
        send("off", null)
    } else {
        send("talk", msg)
    }
}

def send(String topic, String data) {
    def msg = [
            topic: topic,
            data : data,
    ]
    writeBuffer.clear()
    writeBuffer.put(JsonOutput.toJson(msg).getBytes())
    writeBuffer.flip()
    channel.write(writeBuffer)
}