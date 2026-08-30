package net.minestom.server.network.socket;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerDomainSocketChannel;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.unix.DomainSocketAddress;
import io.netty.handler.codec.ByteToMessageDecoder;
import net.minestom.server.MinecraftServer;
import net.minestom.server.ServerFlag;
import net.minestom.server.network.NetworkBuffer;
import net.minestom.server.network.packet.PacketParser;
import net.minestom.server.network.packet.PacketVanilla;
import net.minestom.server.network.packet.client.ClientPacket;
import net.minestom.server.network.player.PlayerSocketConnection;
import net.minestom.server.network.player.ProxyProtocolDecoder;
import org.jetbrains.annotations.ApiStatus;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnixDomainSocketAddress;
import java.nio.file.Files;
import java.util.List;

public final class Server {

    private final PacketParser.Client packetParser;

    private volatile boolean stop;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    private SocketAddress socketAddress;
    private String address;
    private int port;

    public Server(PacketParser.Client packetParser) {
        this.packetParser = packetParser;
    }

    public Server() {
        this(PacketVanilla.CLIENT_PACKET_PARSER);
    }

    @ApiStatus.Internal
    public void init(SocketAddress address) throws IOException {
        switch (address) {
            case InetSocketAddress inet -> {
                this.address = inet.getHostString();
                this.port    = inet.getPort();
            }
            case UnixDomainSocketAddress unix -> {
                this.address = "unix://" + unix.getPath();
                this.port    = 0;
            }
            default -> throw new IllegalArgumentException(
                    "Address must be InetSocketAddress or UnixDomainSocketAddress");
        }
        this.socketAddress = address;
        start();
    }

    @ApiStatus.Internal
    public void start() {
        if (serverChannel != null) return;
        final boolean epoll = Epoll.isAvailable();

        bossGroup   = epoll ? new EpollEventLoopGroup(1)
                : new NioEventLoopGroup(1);
        workerGroup = epoll ? new EpollEventLoopGroup()
                : new NioEventLoopGroup();

        final Class<? extends ServerChannel> channelClass;
        if (socketAddress instanceof UnixDomainSocketAddress) {
            if (!epoll) throw new IllegalStateException("Unix-domain sockets require Netty epoll transport");
            channelClass = EpollServerDomainSocketChannel.class;
        } else {
            channelClass = epoll ? EpollServerSocketChannel.class : NioServerSocketChannel.class;
        }

        final PacketParser<ClientPacket> parser = this.packetParser;

        final ServerBootstrap bootstrap = new ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(channelClass)
                .childOption(ChannelOption.TCP_NODELAY, ServerFlag.SOCKET_NO_DELAY)
                .childOption(ChannelOption.SO_SNDBUF,   ServerFlag.SOCKET_SEND_BUFFER_SIZE)
                .childOption(ChannelOption.SO_RCVBUF,   ServerFlag.SOCKET_RECEIVE_BUFFER_SIZE)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        final ChannelPipeline pipeline = ch.pipeline();

                        final PlayerSocketConnection conn =
                                new PlayerSocketConnection(ch, ch.remoteAddress(), parser);
                        if (ServerFlag.PROXY_PROTOCOL) {
                            pipeline.addLast("proxy-protocol", new ProxyProtocolHandler(conn));
                        }
                        pipeline.addLast("frame-decoder", new MinecraftVarintFrameDecoder());
                        pipeline.addLast("handler", conn.channelHandler());
                    }
                });

        final ChannelFuture future;
        if (socketAddress instanceof InetSocketAddress inet) {
            future = bootstrap.bind(inet);
        } else if (socketAddress instanceof UnixDomainSocketAddress unix) {
            future = bootstrap.bind(new DomainSocketAddress(unix.getPath().toString()));
        } else {
            throw new IllegalStateException("Unsupported address type: " + socketAddress);
        }

        try {
            serverChannel = future.sync().channel();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Server bind interrupted", e);
        }

        if (socketAddress instanceof InetSocketAddress && port == 0) {
            port = ((InetSocketAddress) serverChannel.localAddress()).getPort();
        }
    }

    public boolean isOpen() {
        return !stop;
    }

    public void stop() {
        this.stop = true;
        if (serverChannel != null) {
            serverChannel.close().awaitUninterruptibly();
        }
        if (bossGroup != null) bossGroup.shutdownGracefully().syncUninterruptibly();
        if (workerGroup != null) workerGroup.shutdownGracefully().syncUninterruptibly();

        if (socketAddress instanceof UnixDomainSocketAddress unix) {
            try {
                Files.deleteIfExists(unix.getPath());
            } catch (IOException e) {
                MinecraftServer.getExceptionManager().handleException(e);
            }
        }
    }


    @ApiStatus.Internal
    public PacketParser.Client packetParser() {
        return packetParser;
    }

    public SocketAddress socketAddress() {
        return socketAddress;
    }

    public String getAddress() {
        return address;
    }

    public int getPort() {
        return port;
    }

    private static final class ProxyProtocolHandler extends ByteToMessageDecoder {
        private final PlayerSocketConnection connection;

        private ProxyProtocolHandler(PlayerSocketConnection connection) {
            this.connection = connection;
        }

        @Override
        protected void decode(ChannelHandlerContext context, ByteBuf input, List<Object> output) throws Exception {
            final int readable = input.readableBytes();
            final byte[] bytes = new byte[readable];
            input.getBytes(input.readerIndex(), bytes);
            final NetworkBuffer buffer = NetworkBuffer.wrap(bytes, 0, bytes.length);
            final var result = ProxyProtocolDecoder.parse(connection.getRemoteAddress(), buffer);
            if (result.status() == ProxyProtocolDecoder.Status.NEED_MORE) return;
            if (result.status() == ProxyProtocolDecoder.Status.ABSENT && ServerFlag.PROXY_PROTOCOL_REQUIRED) {
                throw new IOException("Missing required PROXY protocol header");
            }
            if (result.status() == ProxyProtocolDecoder.Status.PRESENT) {
                connection.setRemoteAddress(result.clientAddress());
                input.skipBytes(Math.toIntExact(buffer.readIndex()));
            }
            context.pipeline().remove(this);
            if (input.isReadable()) output.add(input.readRetainedSlice(input.readableBytes()));
        }
    }
}
