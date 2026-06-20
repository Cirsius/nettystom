package net.minestom.server.network.socket;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.unix.DomainSocketAddress;
import net.minestom.server.MinecraftServer;
import net.minestom.server.ServerFlag;
import net.minestom.server.network.packet.PacketParser;
import net.minestom.server.network.packet.PacketVanilla;
import net.minestom.server.network.packet.client.ClientPacket;
import net.minestom.server.network.player.PlayerSocketConnection;
import org.jetbrains.annotations.ApiStatus;

import java.io.IOException;
import java.net.*;
import java.nio.file.Files;

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
    }

    @ApiStatus.Internal
    public void start() {
        final boolean epoll = Epoll.isAvailable();

        bossGroup   = epoll ? new EpollEventLoopGroup(1)
                : new NioEventLoopGroup(1);
        workerGroup = epoll ? new EpollEventLoopGroup()
                : new NioEventLoopGroup();

        final Class<? extends ServerChannel> channelClass =
                epoll ? EpollServerSocketChannel.class
                        : NioServerSocketChannel.class;

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

                        // reads the varint-length-prefixed packets
                        pipeline.addLast("frame-decoder", new MinecraftVarintFrameDecoder());

                        final PlayerSocketConnection conn =
                                new PlayerSocketConnection(ch, ch.remoteAddress(), parser);
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
        if (bossGroup   != null) bossGroup.shutdownGracefully();
        if (workerGroup != null) workerGroup.shutdownGracefully();

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
}