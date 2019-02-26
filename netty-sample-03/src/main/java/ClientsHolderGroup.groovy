import io.netty.channel.socket.SocketChannel

class ClientsHolderGroup implements Iterable<ClientHolder> {

    private Map<String, ClientHolder> idMap = [:]

    private Map<SocketChannel, ClientHolder> channelMap = [:]

    def synchronized add(ClientHolder clientHolder) {
        idMap.put(clientHolder.id, clientHolder)
        channelMap.put(clientHolder.channel, clientHolder)
    }

    ClientHolder find(String id) {
        return idMap.get(id)
    }

    ClientHolder find(SocketChannel channel) {
        return channelMap.get(channel)
    }

    def synchronized remove(SocketChannel channel) {
        ClientHolder clientHolder = channelMap.remove(channel)
        if (clientHolder) {
            idMap.remove(clientHolder.id)
        }
    }

    def synchronized remove(String id) {
        ClientHolder clientHolder = idMap.remove(id)
        if (clientHolder) {
            channelMap.remove(clientHolder.channel)
        }
    }

    def write(def map) {
        channelMap.each {
            it.value.channel.writeAndFlush(map)
        }
    }

    def writeExcept(def map, ClientHolder clientHolder) {
        channelMap.each {
            if (it.value != clientHolder) {
                it.value.channel.writeAndFlush(map)
            }
        }
    }

    @Override
    Iterator<ClientHolder> iterator() {
        return channelMap.values().iterator()
    }
}
