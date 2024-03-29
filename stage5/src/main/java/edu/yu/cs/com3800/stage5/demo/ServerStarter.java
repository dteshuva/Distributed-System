package edu.yu.cs.com3800.stage5.demo;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;

import edu.yu.cs.com3800.stage5.*;

public class ServerStarter {

    protected static final int[] PEER_SERVER_PORTS = {9000, 9010, 9020, 9030, 9040, 9050, 9060, 9070}; // 9000 is gateway
    protected static final int GATEWAY_HTTP_PORT = 9000;

    public static void main(String[] args) throws IOException {
        long thisId = Long.parseLong(args[0]); // node id or 0 for gateway

        // create map of peer ids to addresses
        HashMap<Long, InetSocketAddress> peerIdToAddress = new HashMap<>();
        for (int i = 0; i < PEER_SERVER_PORTS.length; i++) {
            peerIdToAddress.put((long) i, new InetSocketAddress("localhost", PEER_SERVER_PORTS[i]));
        }

        // create and start server (or gateway)
        var address = peerIdToAddress.remove(thisId);
        if (thisId == 0L) {
            GatewayPeerServerImpl gatewayPeerServer = new GatewayPeerServerImpl(GATEWAY_HTTP_PORT, 0, 0L, peerIdToAddress);
            gatewayPeerServer.start();
            new GatewayServer(GATEWAY_HTTP_PORT, gatewayPeerServer).start();
        } else {
            new ZooKeeperPeerServerImpl(address.getPort(), 0, thisId, peerIdToAddress, 0L).start();
        }
    }

}
