package net.minestom.server.network;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import net.minestom.server.registry.Registries;
import net.minestom.server.utils.ObjectPool;
import net.minestom.server.utils.nbt.BinaryTagReader;
import net.minestom.server.utils.nbt.BinaryTagWriter;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.UnknownNullability;

import javax.crypto.Cipher;
import javax.crypto.ShortBufferException;
import java.io.*;
import java.lang.foreign.MemorySegment;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

final class NetworkBufferImpl implements NetworkBuffer {

    private ByteBuf buf;

    private static final ByteBuf DUMMY_BUF = Unpooled.EMPTY_BUFFER;

    private long readIndex;
    private long writeIndex;
    private boolean readOnly;

    private @Nullable BinaryTagWriter nbtWriter;
    private @Nullable BinaryTagReader nbtReader;

    final @Nullable AutoResize autoResize;
    @Nullable Registries registries;

    NetworkBufferImpl(ByteBuf buf,
                      long readIndex, long writeIndex,
                      @Nullable AutoResize autoResize,
                      @Nullable Registries registries) {
        this.buf = buf;
        this.readIndex = readIndex;
        this.writeIndex = writeIndex;
        this.autoResize = autoResize;
        this.registries = registries;
    }

    private boolean isDummy() {
        return buf == DUMMY_BUF;
    }

    void assertDummy() {
        if (isDummy()) throw new UnsupportedOperationException("Buffer is a dummy buffer");
    }

    void assertReadOnly() {
        if (readOnly) throw new UnsupportedOperationException("Buffer is read-only");
    }

    @Override
    public <T> void write(Type<T> type, @UnknownNullability T value) {
        assertReadOnly();
        type.write(this, value);
    }

    @Override
    public <T> @UnknownNullability T read(Type<T> type) {
        assertDummy();
        return type.read(this);
    }

    @Override
    public <T> void writeAt(long index, Type<T> type, @UnknownNullability T value) {
        assertReadOnly();
        final long oldWriteIndex = writeIndex;
        writeIndex = index;
        try {
            write(type, value);
        } finally {
            writeIndex = oldWriteIndex;
        }
    }

    @Override
    public <T> @UnknownNullability T readAt(long index, Type<T> type) {
        assertDummy();
        final long oldReadIndex = readIndex;
        readIndex = index;
        try {
            return read(type);
        } finally {
            readIndex = oldReadIndex;
        }
    }

    @Deprecated(forRemoval = true)
    @Override
    public void copyTo(long srcOffset, byte[] dest, long destOffset, long length) {
        copyTo(srcOffset, dest, (int) destOffset, (int) length);
    }

    @Override
    public void copyTo(long srcOffset, byte[] dest, int destOffset, int length) {
        assertDummy();
        if (length == 0) return;
        if (dest.length < destOffset + length)
            throw new IndexOutOfBoundsException("Destination array is too small");
        buf.getBytes((int) srcOffset, dest, destOffset, length);
    }

    @Override
    public void copyTo(long srcOffset, MemorySegment dest, long destOffset, long length) {
        assertDummy();
        final byte[] tmp = new byte[(int) length];
        buf.getBytes((int) srcOffset, tmp);
        MemorySegment.copy(MemorySegment.ofArray(tmp), 0, dest, destOffset, length);
    }

    @Override
    public byte[] extractBytes(Consumer<NetworkBuffer> extractor) {
        assertDummy();
        final long start = readIndex();
        extractor.accept(this);
        final long end = readIndex();
        final int length = (int) (end - start);
        final byte[] out = new byte[length];
        buf.getBytes((int) start, out);
        return out;
    }

    @Override
    public NetworkBuffer clear() {
        return index(0, 0);
    }

    @Override
    public long writeIndex() {
        return writeIndex;
    }

    @Override
    public long readIndex() {
        return readIndex;
    }

    @Override
    public NetworkBuffer writeIndex(long writeIndex) {
        this.writeIndex = writeIndex;
        return this;
    }

    @Override
    public NetworkBuffer readIndex(long readIndex) {
        this.readIndex = readIndex;
        return this;
    }

    @Override
    public NetworkBuffer index(long readIndex, long writeIndex) {
        this.readIndex = readIndex;
        this.writeIndex = writeIndex;
        return this;
    }

    @Override
    public long advanceWrite(long length) {
        final long oldWriteIndex = writeIndex;
        writeIndex = oldWriteIndex + length;
        return oldWriteIndex;
    }

    @Override
    public long advanceRead(long length) {
        final long oldReadIndex = readIndex;
        readIndex = oldReadIndex + length;
        return oldReadIndex;
    }

    @Override
    public long readableBytes() {
        return writeIndex - readIndex;
    }

    @Override
    public long writableBytes() {
        return capacity() - writeIndex;
    }

    @Override
    public long capacity() {
        return isDummy() ? Long.MAX_VALUE : buf.capacity();
    }

    @Override
    public void readOnly() {
        this.readOnly = true;
    }

    @Override
    public boolean isReadOnly() {
        return readOnly;
    }

    @Override
    public void resize(long newSize) {
        assertDummy();
        assertReadOnly();
        final long capacity = capacity();
        if (newSize <= capacity)
            throw new IllegalArgumentException("New size must be larger than current capacity");
        buf.capacity((int) newSize);
    }

    @Override
    public void ensureWritable(long length) {
        assertReadOnly();
        if (writableBytes() >= length) return;
        final long newCapacity = newCapacity(length, capacity());
        if (isDummy()) return;
        buf.capacity((int) newCapacity);
    }

    private long newCapacity(long length, long capacity) {
        final long targetSize = writeIndex + length;
        final AutoResize strategy = this.autoResize;
        if (strategy == null)
            throw new IndexOutOfBoundsException("Buffer is full and cannot be resized: " + capacity + " -> " + targetSize);
        final long newCapacity = strategy.resize(capacity, targetSize);
        if (newCapacity == capacity)
            throw new IndexOutOfBoundsException("Buffer resized to the same capacity: " + capacity + " -> " + targetSize);
        return newCapacity;
    }

    @Override
    public void compact() {
        assertDummy();
        assertReadOnly();
        if (readIndex == 0) return;
        buf.discardReadBytes();
        writeIndex -= readIndex;
        readIndex = 0;
    }

    @Override
    public NetworkBuffer copy(long index, long length, long readIndex, long writeIndex) {
        assertDummy();
        Objects.checkFromIndexSize((int) index, (int) length, (int) capacity());
        final ByteBuf newBuf = ByteBufAllocator.DEFAULT.buffer((int) length);
        buf.getBytes((int) index, newBuf, 0, (int) length);
        newBuf.writerIndex((int) length);
        return new NetworkBufferImpl(newBuf, readIndex, writeIndex, autoResize, registries);
    }

    @Override
    public int readFromByteBuf(ByteBuf in) {
        assertDummy();
        assertReadOnly();
        final int readable = in.readableBytes();
        if (readable == 0) return 0;
        ensureWritable(readable);
        in.readBytes(buf, (int) writeIndex, readable);
        advanceWrite(readable);
        return readable;
    }

    @Override
    public boolean writeToByteBuf(ByteBuf out) {
        assertDummy();
        final int readable = (int) readableBytes();
        if (readable == 0) return true;
        out.writeBytes(buf, (int) readIndex, readable);
        advanceRead(readable);
        return true;
    }

    @Override
    public void cipher(Cipher cipher, long start, long length) {
        assertDummy();
        final byte[] plain = new byte[(int) length];
        buf.getBytes((int) start, plain);
        final byte[] result = new byte[(int) length];
        try {
            final int written = cipher.update(plain, 0, (int) length, result);
            buf.setBytes((int) start, result, 0, written);
        } catch (ShortBufferException e) {
            throw new RuntimeException(e);
        }
    }

    static class CompressionHolder {
        private static final ObjectPool<Deflater> DEFLATER_POOL = ObjectPool.pool(Deflater::new);
        private static final ObjectPool<Inflater> INFLATER_POOL = ObjectPool.pool(Inflater::new);
    }

    @Override
    public long compress(long start, long length, NetworkBuffer output) {
        assertDummy();
        impl(output).assertReadOnly();

        final byte[] input = new byte[(int) length];
        buf.getBytes((int) start, input);

        final ByteBuf outBuf = impl(output).buf;
        impl(output).ensureWritable(length + 64);

        Deflater deflater = CompressionHolder.DEFLATER_POOL.get();
        try {
            deflater.setInput(input);
            deflater.finish();
            final byte[] tmp = new byte[8192];
            int total = 0;
            while (!deflater.finished()) {
                final int n = deflater.deflate(tmp);
                if (n == 0) break;
                impl(output).ensureWritable(n);
                outBuf.setBytes((int) (output.writeIndex() + total), tmp, 0, n);
                total += n;
            }
            output.advanceWrite(total);
            return total;
        } finally {
            deflater.reset();
            CompressionHolder.DEFLATER_POOL.add(deflater);
        }
    }

    @Override
    public long decompress(long start, long length, NetworkBuffer output) {
        assertDummy();
        impl(output).assertReadOnly();

        final byte[] input = new byte[(int) length];
        buf.getBytes((int) start, input);

        final ByteBuf outBuf = impl(output).buf;

        Inflater inflater = CompressionHolder.INFLATER_POOL.get();
        try {
            inflater.setInput(input);
            final byte[] tmp = new byte[8192];
            int total = 0;
            while (!inflater.finished() && !inflater.needsInput()) {
                final int n = inflater.inflate(tmp);
                if (n == 0) break;
                impl(output).ensureWritable(n);
                outBuf.setBytes((int) (output.writeIndex() + total), tmp, 0, n);
                total += n;
            }
            output.advanceWrite(total);
            return total;
        } catch (DataFormatException e) {
            throw new RuntimeException(e);
        } finally {
            inflater.reset();
            CompressionHolder.INFLATER_POOL.add(inflater);
        }
    }

    @Override
    public @Nullable Registries registries() {
        return registries;
    }

    @Override
    public void registries(@Nullable Registries registries) {
        this.registries = registries;
    }

    @Override
    public String toString() {
        return String.format("NetworkBuffer{r%d|w%d->%d, registries=%s, autoResize=%s, readOnly=%s}",
                readIndex, writeIndex, capacity(), registries != null, autoResize != null, isReadOnly());
    }

    void _putBytes(long index, byte[] value) {
        if (isDummy()) return;
        assertReadOnly();
        buf.setBytes((int) index, value);
    }

    void _getBytes(long index, byte[] value) {
        assertDummy();
        buf.getBytes((int) index, value);
    }

    void _putByte(long index, byte value) {
        if (isDummy()) return;
        assertReadOnly();
        buf.setByte((int) index, value);
    }

    byte _getByte(long index) {
        assertDummy();
        return buf.getByte((int) index);
    }

    void _putShort(long index, short value) {
        if (isDummy()) return;
        assertReadOnly();
        buf.setShort((int) index, value);
    }

    short _getShort(long index) {
        assertDummy();
        return buf.getShort((int) index);
    }

    void _putInt(long index, int value) {
        if (isDummy()) return;
        assertReadOnly();
        buf.setInt((int) index, value);
    }

    int _getInt(long index) {
        assertDummy();
        return buf.getInt((int) index);
    }

    void _putLong(long index, long value) {
        if (isDummy()) return;
        assertReadOnly();
        buf.setLong((int) index, value);
    }

    long _getLong(long index) {
        assertDummy();
        return buf.getLong((int) index);
    }

    void _putFloat(long index, float value) {
        if (isDummy()) return;
        assertReadOnly();
        buf.setFloat((int) index, value);
    }

    float _getFloat(long index) {
        assertDummy();
        return buf.getFloat((int) index);
    }

    void _putDouble(long index, double value) {
        if (isDummy()) return;
        assertReadOnly();
        buf.setDouble((int) index, value);
    }

    double _getDouble(long index) {
        assertDummy();
        return buf.getDouble((int) index);
    }

    static NetworkBuffer wrap(byte[] bytes, long readIndex, long writeIndex, @Nullable Registries registries) {
        final ByteBuf buf = ByteBufAllocator.DEFAULT.buffer(bytes.length);
        buf.writeBytes(bytes);
        return new NetworkBufferImpl(buf, readIndex, writeIndex, null, registries);
    }

    static NetworkBuffer fromByteBuf(ByteBuf buf, @Nullable Registries registries) {
        return new NetworkBufferImpl(buf, buf.readerIndex(), buf.writerIndex(), null, registries);
    }

    static void copy(NetworkBuffer srcBuffer, long srcOffset,
                     NetworkBuffer dstBuffer, long dstOffset, long length) {
        final NetworkBufferImpl src = impl(srcBuffer);
        final NetworkBufferImpl dst = impl(dstBuffer);
        dst.assertReadOnly();
        src.buf.getBytes((int) srcOffset, dst.buf, (int) dstOffset, (int) length);
    }

    static boolean equals(NetworkBuffer buffer1, NetworkBuffer buffer2) {
        final NetworkBufferImpl b1 = impl(buffer1);
        final NetworkBufferImpl b2 = impl(buffer2);
        final int cap = (int) b1.capacity();
        if (cap != b2.capacity()) return false;
        for (int i = 0; i < cap; i++) {
            if (b1.buf.getByte(i) != b2.buf.getByte(i)) return false;
        }
        return true;
    }

    static NetworkBufferImpl dummy(Registries registries) {
        return new NetworkBufferImpl(DUMMY_BUF, 0, 0, null, registries);
    }

    static NetworkBufferImpl impl(NetworkBuffer buffer) {
        return (NetworkBufferImpl) buffer;
    }

    BinaryTagWriter nbtWriter() {
        if (this.nbtWriter == null) {
            this.nbtWriter = new BinaryTagWriter(new DataOutputStream(new OutputStream() {
                @Override
                public void write(int b) {
                    NetworkBufferImpl.this.write(BYTE, (byte) b);
                }
            }));
        }
        return this.nbtWriter;
    }

    BinaryTagReader nbtReader() {
        if (nbtReader == null) {
            this.nbtReader = new BinaryTagReader(new DataInputStream(new InputStream() {
                @Override
                public int read() {
                    return NetworkBufferImpl.this.read(BYTE) & 0xFF;
                }

                @Override
                public int available() {
                    return (int) NetworkBufferImpl.this.readableBytes();
                }
            }));
        }
        return nbtReader;
    }

    static final class Builder implements NetworkBuffer.Builder {
        private final long initialSize;
        private @Nullable AutoResize autoResize;
        private @Nullable Registries registries;

        Builder(long initialSize) {
            this.initialSize = initialSize;
        }

        @Override
        public NetworkBuffer.Builder autoResize(@Nullable AutoResize autoResize) {
            this.autoResize = autoResize;
            return this;
        }

        @Override
        public NetworkBuffer.Builder registry(@Nullable Registries registries) {
            this.registries = registries;
            return this;
        }

        @Override
        public NetworkBuffer build() {
            final ByteBuf buf = ByteBufAllocator.DEFAULT.buffer((int) initialSize, Integer.MAX_VALUE);
            return new NetworkBufferImpl(buf, 0, 0, autoResize, registries);
        }
    }

}