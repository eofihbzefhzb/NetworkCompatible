package dev.kastle.netty.channel.nethernet;

import dev.kastle.netty.channel.nethernet.config.DefaultNetherChannelConfig;
import dev.kastle.webrtc.RTCPeerConnection;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

public class NetherNetChildChannel extends NetherNetChannel {
    /**
     * Set once the server channel has fired this channel down its pipeline for the bootstrap to
     * register. Only read and written on the server channel's event loop.
     */
    boolean handedOff;

    public NetherNetChildChannel(Channel parent, RTCPeerConnection peerConnection, InetSocketAddress remote, InetSocketAddress local) {
        super(parent, remote, local);
        this.peerConnection = peerConnection;
        this.config = new DefaultNetherChannelConfig(this);
    }

    @Override
    protected AbstractUnsafe newUnsafe() {
        return new AbstractUnsafe() {
            @Override
            public void connect(SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) {
                promise.setFailure(new UnsupportedOperationException("Child channel cannot connect"));
            }
        };
    }

    @Override
    protected void doBind(SocketAddress localAddress) throws Exception {
        throw new UnsupportedOperationException("Child channel cannot be bound");
    }
}