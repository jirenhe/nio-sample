import io.netty.channel.socket.SocketChannel

class ClientHolder {

    SocketChannel channel

    String id

    ClientHolder(SocketChannel channel) {
        this.channel = channel
    }

    ClientHolder(SocketChannel channel, String id) {
        this.channel = channel
        this.id = id
    }
}
