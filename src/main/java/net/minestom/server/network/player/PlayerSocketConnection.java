package net.minestom.server.network.player;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import net.minestom.server.MinecraftServer;
import net.minestom.server.ServerFlag;
import net.minestom.server.adventure.MinestomAdventure;
import net.minestom.server.entity.Player;
import net.minestom.server.event.EventDispatcher;
import net.minestom.server.event.ListenerHandle;
import net.minestom.server.event.player.PlayerPacketOutEvent;
import net.minestom.server.extras.mojangAuth.MojangCrypt;
import net.minestom.server.network.ConnectionState;
import net.minestom.server.network.NetworkBuffer;
import net.minestom.server.network.packet.PacketParser;
import net.minestom.server.network.packet.PacketReading;
import net.minestom.server.network.packet.PacketVanilla;
import net.minestom.server.network.packet.PacketWriting;
import net.minestom.server.network.packet.client.ClientPacket;
import net.minestom.server.network.packet.client.common.ClientCookieResponsePacket;
import net.minestom.server.network.packet.client.common.ClientKeepAlivePacket;
import net.minestom.server.network.packet.client.common.ClientPingRequestPacket;
import net.minestom.server.network.packet.client.configuration.ClientFinishConfigurationPacket;
import net.minestom.server.network.packet.client.configuration.ClientSelectKnownPacksPacket;
import net.minestom.server.network.packet.client.handshake.ClientHandshakePacket;
import net.minestom.server.network.packet.client.login.ClientEncryptionResponsePacket;
import net.minestom.server.network.packet.client.login.ClientLoginAcknowledgedPacket;
import net.minestom.server.network.packet.client.login.ClientLoginPluginResponsePacket;
import net.minestom.server.network.packet.client.login.ClientLoginStartPacket;
import net.minestom.server.network.packet.client.status.StatusRequestPacket;
import net.minestom.server.network.packet.server.BufferedPacket;
import net.minestom.server.network.packet.server.CachedPacket;
import net.minestom.server.network.packet.server.FramedPacket;
import net.minestom.server.network.packet.server.SendablePacket;
import net.minestom.server.network.packet.server.ServerPacket;
import net.minestom.server.network.packet.server.login.SetCompressionPacket;
import net.minestom.server.utils.collection.ConcurrentMessageQueues;
import net.minestom.server.utils.validate.Check;
import org.jctools.queues.MessagePassingQueue;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.Nullable;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.DataFormatException;


/**
 * Represents a socket connection.
 * <p>
 * It is the implementation used for all network client.
 */
@ApiStatus.Internal
public class PlayerSocketConnection extends PlayerConnection {
    private static final Set<Class<? extends ClientPacket>> IMMEDIATE_PROCESS_PACKETS = Set.of(
            ClientHandshakePacket.class, // First received packet
            ClientCookieResponsePacket.class,
            StatusRequestPacket.class,
            ClientPingRequestPacket.class,
            ClientKeepAlivePacket.class, // Used to calculate latency
            ClientLoginStartPacket.class,
            ClientEncryptionResponsePacket.class, // Auth request
            ClientLoginPluginResponsePacket.class,
            ClientSelectKnownPacksPacket.class, // Immediate answer to server request on config
            ClientLoginAcknowledgedPacket.class, // Handle config state
            ClientFinishConfigurationPacket.class // Enter play state
    );

    private final Channel channel;
    private SocketAddress remoteAddress;
    private final PacketParser<ClientPacket> packetParser;

    //Could be null. Only used for Mojang Auth
    private volatile @Nullable EncryptionContext encryptionContext;
    private byte[] nonce = new byte[4];

    // Data from client packets
    private @Nullable String loginUsername;
    private @Nullable GameProfile gameProfile;
    private @Nullable String serverAddress;
    private int serverPort;
    private int protocolVersion;

    private final NetworkBuffer readBuffer =
            NetworkBuffer.resizableBuffer(ServerFlag.POOLED_BUFFER_SIZE, MinecraftServer.getRegistries());

    private final MessagePassingQueue<SendablePacket> packetQueue =
            ConcurrentMessageQueues.mpscUnboundedArrayQueue(1024);

    private final AtomicLong sentPacketCounter = new AtomicLong();
    // Index where compression starts, linked to `sentPacketCounter`
    // Used instead of a simple boolean so we can get proper timing for serialization
    private volatile long compressionStart = Long.MAX_VALUE;

    private final ListenerHandle<PlayerPacketOutEvent> outgoing =
            EventDispatcher.getHandle(PlayerPacketOutEvent.class);

    private final ConnectionHandler handler = new ConnectionHandler();

    public PlayerSocketConnection(Channel channel,
                                  SocketAddress remoteAddress,
                                  PacketParser<ClientPacket> packetParser) {
        super();
        this.channel       = channel;
        this.remoteAddress = remoteAddress;
        this.packetParser  = packetParser;
    }

    public PlayerSocketConnection(SocketChannel socketChannel, SocketAddress remoteAddress,
                                  Thread readThread, Thread writeThread) {
        this(legacyChannel(socketChannel, readThread, writeThread), remoteAddress, PacketVanilla.CLIENT_PACKET_PARSER);
    }

    private static Channel legacyChannel(SocketChannel socketChannel, Thread readThread, Thread writeThread) {
        Objects.requireNonNull(socketChannel, "socketChannel");
        Objects.requireNonNull(readThread, "readThread");
        Objects.requireNonNull(writeThread, "writeThread");
        return new EmbeddedChannel();
    }

    public ConnectionHandler channelHandler() {
        return handler;
    }

    private void handleRead(ByteBuf frame) {
        final NetworkBuffer readBuffer = this.readBuffer;

        final long writeIndexBefore = readBuffer.writeIndex();
        final byte[] bytes = new byte[frame.readableBytes()];
        frame.readBytes(bytes);
        readBuffer.write(NetworkBuffer.RAW_BYTES, bytes);

        final EncryptionContext ctx = this.encryptionContext;
        if (ctx != null) {
            final long written = readBuffer.writeIndex() - writeIndexBefore;
            readBuffer.cipher(ctx.decrypt(), writeIndexBefore, written);
        }

        processPackets(readBuffer);
    }

    private boolean compression() {
        return compressionStart != Long.MAX_VALUE;
    }

    private void processPackets(NetworkBuffer readBuffer) {
        final ConnectionState startingState = getClientState();
        final PacketReading.Result<ClientPacket> result;
        try {
            result = PacketReading.readPackets(
                    readBuffer,
                    packetParser,
                    startingState, PacketVanilla::nextClientState,
                    compression()
            );
        } catch (DataFormatException e) {
            MinecraftServer.getExceptionManager().handleException(e);
            disconnect();
            return;
        }
        switch (result) {
            case PacketReading.Result.Success<ClientPacket> success -> {
                for (PacketReading.ParsedPacket<ClientPacket> parsed : success.packets()) {
                    final ClientPacket packet = parsed.packet();
                    try {
                        if (IMMEDIATE_PROCESS_PACKETS.contains(packet.getClass())) {
                            MinecraftServer.getPacketListenerManager()
                                    .processClientPacket(packet, this);
                        } else {
                            // To be processed during the next player tick
                            final Player player = getPlayer();
                            assert player != null;
                            player.addPacketToQueue(packet);
                        }
                    } catch (Exception e) {
                        MinecraftServer.getExceptionManager().handleException(e);
                    }
                }
                // Compact in case of incomplete read
                readBuffer.compact();
            }
            case PacketReading.Result.Empty<ClientPacket> _ -> { /* nothing yet */ }
            case PacketReading.Result.Failure<ClientPacket> failure -> {
                final long required = failure.requiredCapacity();
                assert required > readBuffer.capacity();
                readBuffer.resize(required);
            }
        }
    }

    @Override
    public void sendPacket(SendablePacket packet) {
        packetQueue.relaxedOffer(packet);
        channel.flush(); // schedule a write on the event loop
    }

    @Override
    public void sendPackets(Collection<SendablePacket> packets) {
        for (SendablePacket p : packets) packetQueue.relaxedOffer(p);
        channel.flush();
    }

    private void flushQueue() {
        if (packetQueue.isEmpty()) return;

        final NetworkBuffer buffer = PacketVanilla.PACKET_POOL.get();
        PacketWriting.writeQueue(buffer, packetQueue, 1, (b, packet) -> {
            final boolean compressed = sentPacketCounter.get() > compressionStart;
            final boolean ok = writePacketSync(b, packet, compressed);
            if (ok) sentPacketCounter.getAndIncrement();
            return ok;
        });

        final long readable = buffer.readableBytes();
        if (readable > 0) {
            final ByteBuf out = channel.alloc().buffer((int) readable);
            final byte[] bytes = new byte[Math.toIntExact(readable)];
            buffer.copyTo(buffer.readIndex(), bytes, 0, bytes.length);
            buffer.advanceRead(readable);
            out.writeBytes(bytes);

            final EncryptionContext ctx = this.encryptionContext;
            if (ctx != null && out.isReadable()) {
                final byte[] raw = new byte[out.readableBytes()];
                out.getBytes(out.readerIndex(), raw);
                try {
                    final byte[] encrypted = ctx.encrypt().update(raw);
                    out.clear();
                    out.writeBytes(encrypted);
                } catch (Exception e) {
                    out.release();
                    throw new RuntimeException(e);
                }
            }

            channel.writeAndFlush(out).addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
        }

        PacketVanilla.PACKET_POOL.add(buffer);
    }

    private boolean writePacketSync(NetworkBuffer buffer, SendablePacket packet, boolean compressed) {
        final Player player = getPlayer();
        final ConnectionState state = getServerState();
        if (player != null) {
            // Outgoing event
            if (outgoing.hasListener()) {
                final ServerPacket serverPacket = SendablePacket.extractServerPacket(state, packet);
                if (serverPacket != null) { // Events are not called for buffered packets
                    PlayerPacketOutEvent event = new PlayerPacketOutEvent(player, serverPacket);
                    outgoing.call(event);
                    if (event.isCancelled()) return true;
                }
            }
            // Translation
            if (ServerFlag.AUTOMATIC_COMPONENT_TRANSLATION && packet instanceof ServerPacket.ComponentHolding translatablePacket) {
                packet = translatablePacket.copyWithOperator(component ->
                        MinestomAdventure.COMPONENT_TRANSLATOR.apply(component, Objects.requireNonNullElseGet(player.getLocale(), MinestomAdventure::getDefaultLocale)));
            }
        }
        final long start = buffer.writeIndex();
        final int compressionThreshold = compressed ? MinecraftServer.getCompressionThreshold() : 0;
        try {
            return switch (packet) {
                case ServerPacket serverPacket -> {
                    var nextState = PacketVanilla.nextServerState(serverPacket, state);
                    if (nextState != state) setServerState(nextState);

                    PacketWriting.writeFramedPacket(buffer, state, serverPacket, compressionThreshold);
                    yield true;
                }
                case FramedPacket framedPacket -> {
                    final NetworkBuffer body = framedPacket.body();
                    yield writeBuffer(buffer, body, 0, body.capacity());
                }
                case CachedPacket cachedPacket -> {
                    final NetworkBuffer body = cachedPacket.body(state);
                    if (body != null) {
                        yield writeBuffer(buffer, body, 0, body.capacity());
                    } else {
                        PacketWriting.writeFramedPacket(buffer, state, cachedPacket.packet(state), compressionThreshold);
                        yield true;
                    }
                }
                case BufferedPacket bufferedPacket -> {
                    final NetworkBuffer rawBuffer = bufferedPacket.buffer();
                    final long index = bufferedPacket.index();
                    final long length = bufferedPacket.length();
                    yield writeBuffer(buffer, rawBuffer, index, length);
                }
            };
        } catch (IndexOutOfBoundsException _) {
            buffer.writeIndex(start);
            return false;
        }
    }

    private static boolean writeBuffer(NetworkBuffer dst, NetworkBuffer src,
                                long index, long length) {
        if (dst.writableBytes() < length) return false;
        NetworkBuffer.copy(src, index, dst, dst.writeIndex(), length);
        dst.advanceWrite(length);
        return true;
    }

    public void setEncryptionKey(SecretKey secretKey) {
        Check.stateCondition(encryptionContext != null, "Encryption is already enabled!");
        this.encryptionContext = new EncryptionContext(
                MojangCrypt.getCipher(1, secretKey),
                MojangCrypt.getCipher(2, secretKey));
    }

    public void startCompression() {
        Check.stateCondition(compression(), "Compression is already enabled!");
        this.compressionStart = sentPacketCounter.get();
        final int threshold = MinecraftServer.getCompressionThreshold();
        Check.stateCondition(threshold == 0,
                "Compression cannot be enabled because the threshold is equal to 0");
        sendPacket(new SetCompressionPacket(threshold));
    }

    @Override
    public SocketAddress getRemoteAddress() {
        return remoteAddress;
    }

    /**
     * Changes the internal remote address field.
     * <p>
     * Mostly unsafe, used internally when interacting with a proxy.
     *
     * @param remoteAddress the new connection remote address
     */
    @ApiStatus.Internal
    public void setRemoteAddress(SocketAddress remoteAddress) {
        this.remoteAddress = remoteAddress;
    }

    public Channel getChannel() {
        return channel;
    }

    public @Nullable GameProfile gameProfile() {
        return gameProfile;
    }

    public void UNSAFE_setProfile(GameProfile gameProfile) {
        this.gameProfile = gameProfile;
    }

    public @Nullable String getLoginUsername() {
        return loginUsername;
    }

    public void UNSAFE_setLoginUsername(String loginUsername) {
        this.loginUsername = loginUsername;
    }

    /**
     * Gets the server address that the client used to connect.
     * <p>
     * WARNING: it is given by the client, it is possible for it to be wrong.
     *
     * @return the server address used
     */
    @Override
    public @Nullable String getServerAddress() {
        return serverAddress;
    }

    /**
     * Gets the server port that the client used to connect.
     * <p>
     * WARNING: it is given by the client, it is possible for it to be wrong.
     *
     * @return the server port used
     */
    @Override
    public int getServerPort() {
        return serverPort;
    }

    /**
     * Gets the protocol version of a client.
     *
     * @return protocol version of client.
     */
    @Override
    public int getProtocolVersion() {
        return protocolVersion;
    }

    /**
     * Used in {@link ClientHandshakePacket} to change the internal fields.
     *
     * @param serverAddress   the server address which the client used
     * @param serverPort      the server port which the client used
     * @param protocolVersion the protocol version which the client used
     */
    public void refreshServerInformation(@Nullable String serverAddress, int serverPort, int protocolVersion) {
        this.serverAddress = serverAddress;
        this.serverPort = serverPort;
        this.protocolVersion = protocolVersion;
    }

    public byte[] getNonce() {
        return nonce;
    }

    public void setNonce(byte[] nonce) {
        this.nonce = nonce;
    }

    public final class ConnectionHandler extends ChannelDuplexHandler {

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof ByteBuf frame) {
                try {
                    handleRead(frame);
                } finally {
                    frame.release();
                }
            }
        }

        @Override
        public void flush(ChannelHandlerContext ctx) {
            flushQueue();
            ctx.flush();
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            final ChannelPromise promise = ctx.newPromise();

            try {
                disconnect(ctx, promise);
            } catch (Exception _) {
                ctx.close(promise).addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            final boolean expected =
                    cause instanceof IOException &&
                            (cause.getMessage() != null &&
                                    (cause.getMessage().contains("Connection reset") ||
                                            cause.getMessage().contains("Broken pipe")));
            if (!expected) {
                MinecraftServer.getExceptionManager().handleException(cause);
            }

            final ChannelPromise promise = ctx.newPromise();

            try {
                disconnect(ctx, promise);
            } catch (Exception _) {
                ctx.close(promise).addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
            }
        }
    }

    @Override
    public void disconnect() {
        super.disconnect();
    }

    record EncryptionContext(Cipher encrypt, Cipher decrypt) {
    }
}
