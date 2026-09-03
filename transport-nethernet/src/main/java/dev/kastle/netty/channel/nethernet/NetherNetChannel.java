package dev.kastle.netty.channel.nethernet;

import dev.kastle.netty.channel.nethernet.config.DefaultNetherChannelConfig;
import dev.kastle.webrtc.RTCDataChannel;
import dev.kastle.webrtc.RTCDataChannelBuffer;
import dev.kastle.webrtc.RTCDataChannelObserver;
import dev.kastle.webrtc.RTCDataChannelState;
import dev.kastle.webrtc.RTCPeerConnection;
import io.netty.buffer.ByteBuf;
import io.netty.channel.AbstractChannel;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelMetadata;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.EventLoop;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public abstract class NetherNetChannel extends AbstractChannel {
    private static final InternalLogger log = InternalLoggerFactory.getInstance(NetherNetChannel.class);
    protected static final ChannelMetadata METADATA = new ChannelMetadata(false);

    protected DefaultNetherChannelConfig config;
    protected volatile RTCPeerConnection peerConnection;
    protected volatile SocketAddress remoteAddress;
    protected volatile SocketAddress localAddress;

    protected RTCDataChannel reliableChannel;
    protected RTCDataChannel unreliableChannel;

    protected final Queue<Object> pendingWrites = new ConcurrentLinkedQueue<>();

    protected volatile boolean open = true;

    /**
     * Reassembly buffer for inbound segmented messages, held here rather than inside the data
     * channel observer so that doClose() can hand it back to the allocator.
     * <p>
     * It used to live in the anonymous observer, where clear() reset it but nothing ever released
     * it: with Netty's pooled allocator that meant one buffer per peer never returned to the pool,
     * for the life of the process.
     * <p>
     * Guarded by {@link #assemblyLock} because inbound messages are delivered on the native WebRTC
     * thread while doClose() runs on the event loop. Releasing it unguarded would leave a window in
     * which a late message writes into memory the allocator has already handed to another
     * connection, which is a far worse failure than the leak this replaces.
     */
    private ByteBuf assemblyBuf;

    private final Object assemblyLock = new Object();

    /** Rank of the ICE candidate the current remoteAddress came from; higher or equal wins. */
    private volatile int remoteAddressRank = -1;

    /**
     * Updates the peer address once ICE has told us where the remote actually is.
     * <p>
     * A child channel is constructed before any candidate has been exchanged, so its remote address
     * starts out as the wildcard 0.0.0.0:0. Without this, everything downstream - logging, proxy
     * protocol forwarding, per-IP rate limiting, anti-VPN checks - sees every NetherNet peer as
     * 0.0.0.0 and cannot tell them apart.
     * <p>
     * Candidates arrive in bursts and in no useful order, with "host" (the peer's LAN address)
     * usually first. Ranking rather than last-write-wins is what stops a later host candidate from
     * clobbering the public address we actually want.
     *
     * Synchronized because the rank check and the two assignments are a read-modify-write across
     * two fields: candidates that arrive concurrently could otherwise interleave so that a lower
     * ranked one is applied last, which is exactly the wrong address this method exists to avoid.
     *
     * @param remote the address parsed from the remote ICE candidate.
     * @param rank   higher for more externally meaningful candidate types; on a tie the most
     *               recent candidate wins, since two candidates of one type are equally valid.
     */
    public synchronized void updateRemoteAddress(InetSocketAddress remote, int rank) {
        if (remote == null || remote.getAddress() == null || remote.getAddress().isAnyLocalAddress()) {
            return;
        }
        if (rank >= this.remoteAddressRank) {
            this.remoteAddressRank = rank;
            this.remoteAddress = remote;
        }
    }

    protected NetherNetChannel(Channel parent, InetSocketAddress remote, InetSocketAddress local) {
        super(parent);
        this.remoteAddress = remote;
        this.localAddress = local;
    }

    public void setDataChannels(RTCDataChannel reliable, RTCDataChannel unreliable) {
        this.reliableChannel = reliable;
        this.unreliableChannel = unreliable;

        synchronized (this.assemblyLock) {
            // Skip the allocation if the peer already went away mid-handshake; doClose() has run
            // and nothing would ever release a buffer created after it.
            if (this.open) {
                this.assemblyBuf = config.getAllocator().buffer();
            }
        }

        RTCDataChannelObserver observer = new RTCDataChannelObserver() {
            private int currentSegmentCount = -1;

            @Override
            public void onBufferedAmountChange(long previousAmount) {
            }

            @Override
            public void onStateChange() {
                eventLoop().execute(() -> onDataChannelStateChange());
            }

            @Override
            public void onMessage(RTCDataChannelBuffer buffer) {
                ByteBuffer data = buffer.data;
                if (!data.hasRemaining())
                    return;

                synchronized (assemblyLock) {
                    ByteBuf assembly = assemblyBuf;
                    // Null once the channel is closed: drop anything the native thread still
                    // delivers rather than touching a buffer that has gone back to the pool.
                    if (assembly == null)
                        return;

                    int segments = data.get() & 0xFF;

                    if (currentSegmentCount == -1) {
                        currentSegmentCount = segments;
                    } else {
                        if (segments != currentSegmentCount - 1) {
                            assembly.clear();
                            currentSegmentCount = -1;
                            return;
                        }
                        currentSegmentCount = segments;
                    }

                    if (data.hasRemaining()) {
                        byte[] payload = new byte[data.remaining()];
                        data.get(payload);
                        assembly.writeBytes(payload);
                    }

                    if (segments == 0) {
                        try {
                            if (assembly.isReadable()) {
                                ByteBuf packet = assembly.copy();
                                assembly.skipBytes(assembly.readableBytes());

                                eventLoop().execute(() -> {
                                    pipeline().fireChannelRead(packet);
                                    pipeline().fireChannelReadComplete();
                                });
                            }
                        } catch (Exception e) {
                            log.error("Error processing packet", e);
                        } finally {
                            assembly.clear();
                            currentSegmentCount = -1;
                        }
                    }
                }
            }
        };

        this.reliableChannel.registerObserver(observer);

        if (reliableChannel.getState() == RTCDataChannelState.OPEN) {
            eventLoop().execute(this::onDataChannelStateChange);
        }
    }

    private void onDataChannelStateChange() {
        if (isActive()) {
            if (!pendingWrites.isEmpty()) {
                pipeline().fireChannelWritabilityChanged();
                unsafe().flush();
            }
        } else if (reliableChannel.getState() == RTCDataChannelState.CLOSED) {
            close();
        }
    }

    @Override
    protected void doWrite(ChannelOutboundBuffer in) throws Exception {
        if (!isActive()) {
            Object msg;
            while ((msg = in.current()) != null) {
                ReferenceCountUtil.retain(msg);
                pendingWrites.add(msg);
                in.remove();
            }
            return;
        }

        while (!pendingWrites.isEmpty()) {
            Object msg = pendingWrites.poll();
            try {
                writeInternal(msg);
            } finally {
                ReferenceCountUtil.release(msg);
            }
        }

        Object msg;
        while ((msg = in.current()) != null) {
            writeInternal(msg);
            in.remove();
        }
    }

    private void writeInternal(Object msg) {
        if (!(msg instanceof ByteBuf))
            return;

        ByteBuf payload = (ByteBuf) msg;

        ByteBuf framed = payload.retainedDuplicate();

        int totalLength = framed.readableBytes();
        int maxPayload = NetherNetConstants.MAX_SCTP_MESSAGE_SIZE - 1;

        int segments = (totalLength / maxPayload);
        if (totalLength % maxPayload != 0)
            segments++;

        try {
            int offset = 0;
            for (int i = 0; i < segments; i++) {
                int remaining = segments - 1 - i;
                int chunkSize = Math.min(maxPayload, framed.readableBytes() - offset);

                // Sized to this exact segment, and never pooled or reused: send() hands a direct
                // buffer straight to native code with no length, so the native side takes the
                // buffer's capacity as the payload length and ignores position and limit. A
                // larger buffer holding a shorter segment is therefore sent with trailing garbage,
                // which corrupts the peer's reassembly and drops the connection.
                ByteBuffer chunk = ByteBuffer.allocateDirect(1 + chunkSize);
                chunk.put((byte) remaining);

                framed.getBytes(offset, chunk);
                chunk.position(chunk.limit());
                chunk.flip();

                reliableChannel.send(new RTCDataChannelBuffer(chunk, true));
                offset += chunkSize;
            }
        } catch (Exception e) {
            pipeline().fireExceptionCaught(e);
        } finally {
            framed.release();
        }
    }

    @Override
    protected void doRegister() throws Exception {
    }

    @Override
    protected void doDeregister() throws Exception {
    }

    @Override
    protected void doBind(SocketAddress localAddress) throws Exception {
        throw new UnsupportedOperationException("NetherNetChannel cannot be bound directly");
    }

    @Override
    protected void doDisconnect() throws Exception {
        doClose();
    }

    @Override
    protected void doClose() throws Exception {
        this.open = false;

        if (reliableChannel != null) {
            reliableChannel.unregisterObserver();
            reliableChannel.close();
        }
        if (unreliableChannel != null) {
            unreliableChannel.unregisterObserver();
            unreliableChannel.close();
        }
        if (peerConnection != null) {
            peerConnection.close();
        }

        Object msg;
        while ((msg = pendingWrites.poll()) != null) {
            ReferenceCountUtil.release(msg);
        }

        synchronized (this.assemblyLock) {
            ByteBuf assembly = this.assemblyBuf;
            this.assemblyBuf = null;
            if (assembly != null) {
                assembly.release();
            }
        }
    }

    @Override
    protected void doBeginRead() throws Exception {
    }

    @Override
    protected boolean isCompatible(EventLoop loop) {
        return true;
    }

    @Override
    protected SocketAddress localAddress0() {
        return this.localAddress;
    }

    @Override
    protected SocketAddress remoteAddress0() {
        return this.remoteAddress;
    }

    @Override
    public ChannelConfig config() {
        return this.config;
    }

    @Override
    public boolean isOpen() {
        return this.open;
    }

    @Override
    public boolean isActive() {
        return isOpen() && this.reliableChannel != null && this.reliableChannel.getState() == RTCDataChannelState.OPEN;
    }

    @Override
    public ChannelMetadata metadata() {
        return METADATA;
    }
}