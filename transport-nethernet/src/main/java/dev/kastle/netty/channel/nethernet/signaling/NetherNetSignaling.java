package dev.kastle.netty.channel.nethernet.signaling;

import java.util.List;

public interface NetherNetSignaling extends AutoCloseable {

    /**
     * Sends a signaling message to the remote peer.
     *
     * @param targetNetworkId The Network ID of the destination (String to support Realms).
     * @param data            The raw signaling payload.
     */
    void sendSignal(String targetNetworkId, String data);

    /**
     * Sets a handler to receive signaling messages for a specific connection ID.
     * 
     * @param connectionId The connection ID to listen for.
     * @param handler      The handler to process incoming signaling messages.
     */
    void setSignalHandler(long connectionId, SignalHandler handler);

    /**
     * Removes the signaling handler for a specific connection ID.
     * 
     * @param connectionId The connection ID whose handler should be removed.
     */
    void removeSignalHandler(long connectionId);

    /**
     * Returns the Local Network ID of this client as a String.
     * This is required for formatting the 'candidate:' string in SDP.
     */
    String getLocalNetworkId();

    /**
     * Whether this signaling link is still usable.
     * <p>
     * An Xbox signaling websocket can drop at any time - a network blip, or Xbox closing an idle
     * connection - and nothing here reconnects it. The owner of a server channel has no other way
     * to notice: no offers simply stop arriving, which is indistinguishable from a quiet server.
     * Implementations backed by a persistent connection must report its real state so the owner can
     * rebind; implementations without one keep the default.
     *
     * @return true if signals can still be exchanged.
     */
    default boolean isConnected() {
        return true;
    }

    /**
     * Closes the signaling channel and releases any associated resources.
     */
    @Override
    void close();

    /**
     * Functional interface for handling incoming signals.
     */
    @FunctionalInterface
    interface SignalHandler {
        /**
         * Called when a signal is received for the registered connection ID.
         * 
         * @param signal The raw signal payload.
         */
        void onSignal(String signal);
    }

    /**
     * Data structure for ICE server information.
     *
     * @param username The username for the ICE server (if applicable).
     * @param password The password for the ICE server (if applicable).
     * @param urls     The list of URLs for the ICE server.
     */
    public record IceServerInfo(String username, String password, List<String> urls) {
        public static class Builder {
            private String username = "";
            private String password = "";
            private List<String> urls = List.of();

            public Builder setUsername(String username) {
                this.username = username;
                return this;
            }

            public Builder setPassword(String password) {
                this.password = password;
                return this;
            }

            public Builder setUrls(List<String> urls) {
                this.urls = urls;
                return this;
            }

            public IceServerInfo build() {
                return new IceServerInfo(username, password, urls);
            }
        }
    }
}
