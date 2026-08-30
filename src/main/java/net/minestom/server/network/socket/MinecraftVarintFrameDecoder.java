package net.minestom.server.network.socket;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

public final class MinecraftVarintFrameDecoder extends ByteToMessageDecoder {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        if (!ctx.channel().isActive()) return;

        in.markReaderIndex();
        int length = 0;
        int shift = 0;

        for (int i = 0; i < 5; i++) {
            if (!in.isReadable()) {
                in.resetReaderIndex();
                return;
            }
            byte b = in.readByte();
            length |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                if (length < 0) throw new RuntimeException("Negative Paketlänge");

                if (in.readableBytes() < length) {
                    in.resetReaderIndex();
                    return;
                }

                int endIndex = in.readerIndex() + length;
                in.resetReaderIndex();
                out.add(in.readRetainedSlice(endIndex - in.readerIndex()));
                return;
            }
            shift += 7;
        }
        throw new RuntimeException("VarInt zu lang (Corrupted Stream)");
    }
}
