package edu.yu.cs.com3800.stage5.demo;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;

import edu.yu.cs.com3800.stage5.*;

public final class Stage5Killer {

    /** Gateway peer server port is index 0 */
    protected static final int[] PEER_SERVER_PORTS = {8001, 8010, 8020, 8030, 8040, 8050, 8060, 8070, 8080};
    private static final int GATEWAY_HTTP_PORT = 9000;

    /**
     * This main method starts the distributed system locally on the ports specified above
     * @throws InterruptedException
     */
    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws IOException, InterruptedException {
        //create IDs and addresses
        HashMap<Long, InetSocketAddress> peerIDtoAddress = new HashMap<>(3);
        for (int i = 0; i < PEER_SERVER_PORTS.length; i++) {
            peerIDtoAddress.put((long) i, new InetSocketAddress("localhost", PEER_SERVER_PORTS[i]));
        }

        //create and start servers and gateway
        HashMap<Long, ZooKeeperPeerServerImpl> servers = new HashMap<>(3);
        for (var entry : peerIDtoAddress.entrySet()) {
            HashMap<Long, InetSocketAddress> map = (HashMap<Long, InetSocketAddress>) peerIDtoAddress.clone();
            var address = map.remove(entry.getKey());
            if (entry.getKey() == 0L) {
                GatewayPeerServerImpl gatewayPeerServer = new GatewayPeerServerImpl(GATEWAY_HTTP_PORT, 0, 0L, map);
                gatewayPeerServer.start();
                GatewayServer gateway = new GatewayServer(GATEWAY_HTTP_PORT, gatewayPeerServer);
                new Thread(() -> gateway.start(), "Gateway").start();
            } else {
                ZooKeeperPeerServerImpl server = new ZooKeeperPeerServerImpl(address.getPort(), 0, entry.getKey(), map, 0L);
                servers.put(entry.getKey(), server);
                server.start();
            }
        }

        // Thread.sleep(10000);
        // // kill the server with ID 3
        // System.out.println("Killing server with ID 1");
        // servers.get(1L).shutdown();
    }
}
