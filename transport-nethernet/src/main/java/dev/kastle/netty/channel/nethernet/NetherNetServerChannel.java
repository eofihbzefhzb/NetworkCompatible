package dev.kastle.netty.channel.nethernet;

import dev.kastle.netty.channel.nethernet.config.DefaultNetherServerChannelConfig;
import dev.kastle.netty.channel.nethernet.config.NetherChannelOption;
import dev.kastle.netty.channel.nethernet.signaling.NetherNetServerSignaling;
import dev.kastle.netty.channel.nethernet.signaling.NetherNetSignaling.IceServerInfo;
import dev.kastle.netty.util.nethernet.ServerIdentity;
import dev.kastle.webrtc.CreateSessionDescriptionObserver;
import dev.kastle.webrtc.PeerConnectionFactory;
import dev.kastle.webrtc.PeerConnectionObserver;
import dev.kastle.webrtc.RTCAnswerOptions;
import dev.kastle.webrtc.RTCBundlePolicy;
import dev.kastle.webrtc.RTCConfiguration;
import dev.kastle.webrtc.RTCDataChannel;
import dev.kastle.webrtc.RTCIceCandidate;
import dev.kastle.webrtc.RTCIceServer;
import dev.kastle.webrtc.RTCPeerConnection;
import dev.kastle.webrtc.RTCPeerConnectionState;
import dev.kastle.webrtc.RTCSdpType;
import dev.kastle.webrtc.RTCSessionDescription;
import dev.kastle.webrtc.SetSessionDescriptionObserver;
import io.netty.channel.AbstractServerChannel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelMetadata;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.ScheduledFuture;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import org.jose4j.lang.JoseException;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class NetherNetServerChannel extends AbstractServerChannel {
    private static final InternalLogger log = InternalLoggerFactory.getInstance(NetherNetServerChannel.class);
    private static final ChannelMetadata METADATA = new ChannelMetadata(false, 16);

    private final DefaultNetherServerChannelConfig config;
    private final PeerConnectionFactory factory;
    /**
     * Whether this channel created the factory, and may therefore free its native handle on close.
     * <p>
     * A caller that supplies its own factory keeps ownership: the peer connections opened through it
     * outlive this server channel, so disposing it here would free native memory still in use, and
     * would do it again after the owner had already disposed it.
     */
    private final boolean ownsFactory;
    private final NetherNetServerSignaling signaling;
    
    private InetSocketAddress localAddress;
    private volatile boolean open = true;

    private ServerIdentity serverIdentity;

    /**
     * Creates a NetherNetServerChannel with a new PeerConnectionFactory.
     * 
     * @param signaling The NetherNetServerSignaling instance for signaling.
     */
    public NetherNetServerChannel(NetherNetServerSignaling signaling) {
        this(new PeerConnectionFactory(), signaling, true);
    }

    /**
     * Creates a NetherNetServerChannel.
     * 
     * @param factory   The PeerConnectionFactory to use for creating peer connections. Should be reused where possible.
     * @param signaling The NetherNetServerSignaling instance for signaling.
     */
    public NetherNetServerChannel(PeerConnectionFactory factory, NetherNetServerSignaling signaling) {
        this(factory, signaling, false);
    }

    private NetherNetServerChannel(PeerConnectionFactory factory, NetherNetServerSignaling signaling, boolean ownsFactory) {
        this.factory = factory;
        this.ownsFactory = ownsFactory;
        this.signaling = signaling;
        this.config = new DefaultNetherServerChannelConfig(this);
        try {
            this.serverIdentity = ServerIdentity.generate("self");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void doBind(SocketAddress localAddress) throws Exception {
        if (!(localAddress instanceof InetSocketAddress)) throw new IllegalArgumentException("Unsupported address type");
        this.localAddress = (InetSocketAddress) localAddress;
        
        this.signaling.setNewConnectionHandler((connectionId, remoteNetworkId, offerSdp) -> {
            acceptConnection(connectionId, offerSdp, remoteNetworkId);
        });

        this.signaling.bind(localAddress);
    }

    public void acceptConnection(long connectionId, String offerSdp, String remoteNetworkId) {
        RTCConfiguration rtcConfig = new RTCConfiguration();
        rtcConfig.portAllocatorConfig = this.config.getOption(NetherChannelOption.NETHER_PORT_ALLOCATOR_CONFIG);
        rtcConfig.bundlePolicy = RTCBundlePolicy.MAX_BUNDLE;

        // Inject ICE servers if the signaling implementation supports it
        List<IceServerInfo> iceServers = this.signaling.getIceServers();
        if (iceServers != null && !iceServers.isEmpty()) {
            log.trace("Injecting {} ICE Servers into PeerConnection for {}", iceServers.size(), Long.toUnsignedString(connectionId));
            for (IceServerInfo info : iceServers) {
                RTCIceServer iceServer = new RTCIceServer();
                iceServer.urls = info.urls();
                iceServer.username = info.username();
                iceServer.password = info.password();
                rtcConfig.iceServers.add(iceServer);
            }
        }

        ServerPeerConnectionObserver observer = new ServerPeerConnectionObserver(connectionId, remoteNetworkId);
        RTCPeerConnection pc = factory.createPeerConnection(rtcConfig, observer);

        NetherNetChildChannel child = new NetherNetChildChannel(this, pc, new InetSocketAddress(0), localAddress);
        observer.setChildChannel(child);

        child.closeFuture().addListener(future -> signaling.removeSignalHandler(connectionId));

        int handshakeTimeoutSeconds = this.config.getOption(NetherChannelOption.NETHER_SERVER_RTC_HANDSHAKE_TIMEOUT_SECONDS);
        ScheduledFuture<?> timeoutTask = eventLoop().schedule(() -> {
            if (!child.isActive()) {
                log.warn("Connection {} timed out during handshake ({}s)", Long.toUnsignedString(connectionId), handshakeTimeoutSeconds);
                child.close();
                pc.close();
            }
        }, handshakeTimeoutSeconds, TimeUnit.SECONDS);
        observer.setHandshakeTimeout(timeoutTask);
        
        // Register Signal Handler
        signaling.setSignalHandler(connectionId, (signal) -> {
            String[] parts = signal.split(" ", 3);
            if (parts.length < 3) return;
            String type = parts[0];
            String data = parts[2];

            switch (type) {
                case NetherNetConstants.RTC_NEGOTIATION_CANDIDATE_ADD -> {
                    log.trace("Applying Remote Candidate for {}: {}", Long.toUnsignedString(connectionId), data);
                    try {
                        pc.addIceCandidate(new RTCIceCandidate("0", 0, data));
                        // The candidate carries the peer's address. Feed it to the channel, otherwise
                        // the child keeps the wildcard 0.0.0.0:0 it was constructed with and nothing
                        // downstream can tell one NetherNet peer from another.
                        child.updateRemoteAddress(parseCandidateAddress(data), candidateRank(data));
                    } catch (Exception e) {
                        log.debug("Failed to apply ICE candidate for {} (Connection likely closed): {}", Long.toUnsignedString(connectionId), e.toString());
                    }
                }
                case NetherNetConstants.RTC_NEGOTIATION_CONNECT_ERROR -> {
                    log.debug("Received CONNECT_ERROR for {}", Long.toUnsignedString(connectionId));
                    child.close();
                }
            }
        });

        // Handle Offer
        pc.setRemoteDescription(new RTCSessionDescription(RTCSdpType.OFFER, offerSdp), new SetSessionDescriptionObserver() {
            @Override
            public void onSuccess() {
                log.trace("Remote description set for {}", Long.toUnsignedString(connectionId));
                pc.createAnswer(new RTCAnswerOptions(), new CreateSessionDescriptionObserver() {
                    @Override
                    public void onSuccess(RTCSessionDescription description) {
                        pc.setLocalDescription(description, new SetSessionDescriptionObserver() {
                            @Override
                            public void onSuccess() {
                                log.trace("Sending Answer SDP for {}", Long.toUnsignedString(connectionId));
                                try {
                                    signaling.sendSignal(
                                        remoteNetworkId,
                                        NetherNetConstants.buildSignalConnectResponse(connectionId, serverIdentity.augmentAnswer(description.sdp))
                                    );
                                } catch (JoseException e) {
                                    throw new RuntimeException(e);
                                }
                                pipeline().fireChannelRead(child);
                            }
                            @Override public void onFailure(String error) { log.error("SetLocalDesc failed: {}", error); }
                        });
                    }
                    @Override public void onFailure(String error) { log.error("CreateAnswer failed: {}", error); }
                });
            }
            @Override public void onFailure(String error) { log.error("SetRemoteDesc failed: {}", error); }
        });
    }

    /**
     * Extracts the address from an ICE candidate SDP line.
     * <p>
     * The format is fixed by RFC 5245: {@code candidate:<foundation> <component> <transport>
     * <priority> <ip> <port> typ <type> ...}, so the address is field 4 and the port field 5 after
     * the "candidate:" prefix.
     * <p>
     * "relay" is a TURN server rather than the peer, so it is skipped entirely; see
     * {@link #candidateRank(String)} for how the remaining types are prioritised.
     *
     * @return the parsed address, or null if this candidate carries no useful one.
     */
    private static InetSocketAddress parseCandidateAddress(String candidateSdp) {
        if (candidateSdp == null) {
            return null;
        }
        int start = candidateSdp.indexOf("candidate:");
        if (start < 0) {
            return null;
        }
        String[] parts = candidateSdp.substring(start + "candidate:".length()).trim().split("\\s+");
        if (parts.length < 8 || !"typ".equals(parts[6])) {
            return null;
        }
        // A relay candidate is the TURN server's address, never the peer's.
        if ("relay".equals(parts[7])) {
            return null;
        }
        // Literal addresses only. WebRTC also emits mDNS candidates ("<uuid>.local") that hide the
        // peer's LAN address behind a name; new InetSocketAddress(host, port) would try to resolve
        // those, blocking this signaling callback on a DNS lookup that can never tell a remote
        // server anything useful anyway.
        if (!isLiteralAddress(parts[4])) {
            return null;
        }
        try {
            return new InetSocketAddress(parts[4], Integer.parseInt(parts[5]));
        } catch (Exception ignored) {
            return null;
        }
    }

    /**
     * @return true for an IPv4 or IPv6 literal, false for anything that would need name resolution.
     */
    private static boolean isLiteralAddress(String host) {
        if (host == null || host.isEmpty()) {
            return false;
        }
        // In an ICE candidate line only an IPv6 literal can carry a colon.
        if (host.indexOf(':') >= 0) {
            return true;
        }
        for (int i = 0; i < host.length(); i++) {
            char c = host.charAt(i);
            if ((c < '0' || c > '9') && c != '.') {
                return false;
            }
        }
        return true;
    }

    /**
     * Ranks a candidate by how meaningful its address is to this server.
     * <p>
     * "srflx" is the peer's public address as seen through NAT and is what a remote server should
     * record; "host" is the peer's own LAN address, which is only useful when it really is on the
     * same network and is frequently a duplicate-prone 192.168.x.x.
     */
    private static int candidateRank(String candidateSdp) {
        if (candidateSdp == null) {
            return -1;
        }
        if (candidateSdp.contains(" typ srflx")) {
            return 2;
        }
        if (candidateSdp.contains(" typ prflx")) {
            return 1;
        }
        if (candidateSdp.contains(" typ host")) {
            return 0;
        }
        return -1;
    }

    /**
     * Observer to handle Data Channel creation from the client.
     */
    private class ServerPeerConnectionObserver implements PeerConnectionObserver {
        private final long connectionId;
        private final String remoteNetworkId;
        private NetherNetChildChannel child;
        
        private RTCDataChannel reliable;
        private RTCDataChannel unreliable;

        private ScheduledFuture<?> handshakeTimeout;

        public ServerPeerConnectionObserver(long connectionId, String remoteNetworkId) {
            this.connectionId = connectionId;
            this.remoteNetworkId = remoteNetworkId;
        }

        public void setHandshakeTimeout(ScheduledFuture<?> handshakeTimeout) {
            this.handshakeTimeout = handshakeTimeout;
        }

        public void setChildChannel(NetherNetChildChannel child) {
            this.child = child;
            checkDataChannels();
        }

        @Override
        public void onIceCandidate(RTCIceCandidate candidate) {
            if (log.isTraceEnabled()) {
                log.trace("Generated ICE Candidate for {}: {} (Type: {})", 
                    Long.toUnsignedString(this.connectionId), candidate.sdp, extractCandidateType(candidate.sdp));
            }
            signaling.sendSignal(
                remoteNetworkId, 
                NetherNetConstants.buildSignalCandidateAdd(connectionId, candidate.sdp)
            );
        }

        private String extractCandidateType(String sdp) {
            if (sdp.contains(" typ host ")) return "host";
            if (sdp.contains(" typ srflx ")) return "srflx";
            if (sdp.contains(" typ relay ")) return "relay";
            return "unknown";
        }

        @Override
        public void onConnectionChange(RTCPeerConnectionState state) {
            log.debug("Connection {} state changed: {}", Long.toUnsignedString(this.connectionId), state);
            if (state == RTCPeerConnectionState.FAILED || state == RTCPeerConnectionState.CLOSED) {
                if (child != null && child.isOpen()) {
                    log.debug("Closing connection {} due to state change: {}", Long.toUnsignedString(this.connectionId), state);
                    child.close();
                }
                if (handshakeTimeout != null) {
                    handshakeTimeout.cancel(false);
                }
            }
        }

        @Override
        public void onDataChannel(RTCDataChannel dataChannel) {
            String label = dataChannel.getLabel();
            log.debug("Received Data Channel: {}", label);
            
            if (NetherNetConstants.RELIABLE_CHANNEL_LABEL.equals(label)) {
                this.reliable = dataChannel;
            } else if (NetherNetConstants.UNRELIABLE_CHANNEL_LABEL.equals(label)) {
                this.unreliable = dataChannel;
            }
            
            checkDataChannels();
        }
        
        private void checkDataChannels() {
            if (child != null && reliable != null && unreliable != null) {
                if (handshakeTimeout != null) {
                    handshakeTimeout.cancel(false);
                }

                log.debug("Data Channels established for {}", Long.toUnsignedString(this.connectionId));
                child.setDataChannels(reliable, unreliable);
                
                if (child.pipeline() != null) {
                    child.pipeline().fireChannelActive();
                }
            }
        }
    }

    @Override
    protected void doClose() throws Exception {
        this.open = false;
        
        try {
            signaling.close();
        } finally {
            if (ownsFactory) {
                factory.dispose();
            }
        }
    }

    @Override
    protected void doBeginRead() throws Exception {
        // Server channel doesn't read data directly
    }

    @Override
    protected SocketAddress localAddress0() {
        return this.localAddress;
    }

    @Override
    protected boolean isCompatible(EventLoop loop) {
        return true; 
    }

    @Override
    public ChannelConfig config() { return config; }
    
    @Override 
    public boolean isOpen() { 
        return this.open;
    }
    
    @Override 
    public boolean isActive() { 
        return isOpen() && localAddress0() != null;
    }
    
    @Override 
    public ChannelMetadata metadata() { 
        return METADATA; 
    }
}