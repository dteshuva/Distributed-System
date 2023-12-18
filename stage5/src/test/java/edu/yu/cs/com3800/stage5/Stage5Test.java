package edu.yu.cs.com3800.stage5;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.*;
import java.net.http.*;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.*;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import edu.yu.cs.com3800.*;

public class Stage5Test {

    private static final int NUM_REQUESTS = 20;

    /** Gateway peer server is index 0 */
    private static final int[] PEER_SERVER_PORTS = { 8000, 8010, 8020, 8030, 8040, 8050, 8060, 8070 };
    //   private static final int[] PEER_SERVER_PORTS = { 8000, 8010, 8070 };

    private static final int GATEWAY_HTTP_PORT = 8000;

    private final HttpClient httpClient = HttpClient.newHttpClient();
    ExecutorService executor = Executors.newCachedThreadPool();
    private static GatewayServer gateway;
    private static ArrayList<ZooKeeperPeerServer> servers;
    static GatewayPeerServerImpl gatewayPeerServer;

    @BeforeAll
    @SuppressWarnings("unchecked")
    static void createAndStartServers() throws IOException {
        // step 1: create IDs and addresses
        HashMap<Long, InetSocketAddress> peerIDtoAddress = new HashMap<>();
        for (int i = 0; i < PEER_SERVER_PORTS.length; i++) {
            peerIDtoAddress.put((long) i, new InetSocketAddress("localhost", PEER_SERVER_PORTS[i]));
        }

        // step 2: create and start servers
        servers = new ArrayList<>();
        for (long i = 1; i < PEER_SERVER_PORTS.length; i++) {
            HashMap<Long, InetSocketAddress> map = (HashMap<Long, InetSocketAddress>) peerIDtoAddress.clone();
            var address = map.remove(i);
            ZooKeeperPeerServerImpl server = new ZooKeeperPeerServerImpl(address.getPort(), 0, i, map, 0L);
            servers.add(server);
            server.start();
        }

        // step 3: create and start gateway
        HashMap<Long, InetSocketAddress> map = (HashMap<Long, InetSocketAddress>) peerIDtoAddress.clone();
        var address = map.remove(0L);
        gatewayPeerServer = new GatewayPeerServerImpl(GATEWAY_HTTP_PORT, 0, 0L, map);
        gatewayPeerServer.start();
        gateway = new GatewayServer(GATEWAY_HTTP_PORT, gatewayPeerServer);
        gateway.start();
    }

    @AfterAll
    static void shutdownServers() {
        for (ZooKeeperPeerServer server : servers) {
            server.shutdown();
        }
        gatewayPeerServer.shutdown();
        gateway.shutdown();
    }

    @Test
    void testOneRequest() throws Exception {
        String src = """
                import java.util.ArrayList;
                import java.util.Collections;
                import java.util.List;

                public class TestClass {
                    public String run() {
                        List<String> list = new ArrayList<>();
                        list.add("Hello World!");
                        return list.get(0);
                    }
                }
                """;
        String expected = "Hello World!";

        // step 1: send request to the gateway
        HttpResponse<String> r = new edu.yu.cs.com3800.stage5.Stage5Test.sendHttpRequest(src).call();
        assertEquals(200, r.statusCode());
        assertEquals(expected, r.body());
    }

    @Test
    public void testConcurrentRequests() throws InterruptedException, ExecutionException {
        String src = """
            public class HelloWorld {
                public String run() {
                    return "Hello " + "world! " + X;
                }
            }
        """;

        // step 1: send requests to the gateway
        List<Future<HttpResponse<String>>> responses = new ArrayList<>(NUM_REQUESTS);
        for (int i = 0; i < NUM_REQUESTS; i++) {
            responses.add(i, executor.submit(new edu.yu.cs.com3800.stage5.Stage5Test.sendHttpRequest(src.replace("X", ""+i))));
            //Thread.sleep(1000);
        }

        // step 2: validate responses from gateway
        for (int i = 0; i < NUM_REQUESTS; i++) {
            HttpResponse<String> r = responses.get(i).get();
            assertEquals(200, r.statusCode());
            assertEquals("Hello world! " + i, r.body());
        }
    }

    @Test
    void testBadRequests() throws InterruptedException, ExecutionException {
        // step 1: send bad requests to the gateway
        List<Future<HttpResponse<String>>> responses = new ArrayList<>(3);

        // no run method
        responses.add(executor.submit(new edu.yu.cs.com3800.stage5.Stage5Test.sendHttpRequest("""
                public class HelloWorld {
                    public static void main(String[] args) {
                        System.out.println("Hello World");
                    }
                }
            """)));

        // constructor takes arguments
        responses.add(executor.submit(new edu.yu.cs.com3800.stage5.Stage5Test.sendHttpRequest("""
            public class HelloWorld {
                public HelloWorld(int i) {
                }
                public String run() {
                    return "Hello " + "World " + (40 + 14 / 7);
                }
            }
        """)));

        responses.add(executor.submit(new edu.yu.cs.com3800.stage5.Stage5Test.sendHttpRequest("""
            public class HelloWorld {
                public String run() {
                    return "Hello World "
                }
            }
        """)));

        // step 2: validate bad responses from gateway
        for (Future<HttpResponse<String>> response : responses) {
            HttpResponse<String> r = response.get();
            assertEquals(400, r.statusCode());
            assertFalse(r.body().isBlank());
        }
    }

    @Test
    void testFailureDetection() {
        System.out.println("Running testFailureDetection");
        // step 1: kill a server
        ZooKeeperPeerServer toKill = servers.remove(0);
        if (toKill.getPeerState() == ZooKeeperPeerServer.ServerState.LEADING) {
            servers.add(toKill);
            toKill = servers.remove(0);
        }
        System.out.println("Killed server " + toKill.getUdpPort());
        toKill.shutdown();

        try {
            Thread.sleep(Gossiper.CLEANUP);
        } catch (InterruptedException e) {
            return;
        }

        for (var server : servers) {
            assertTrue(server.isPeerDead(toKill.getServerId()));
        }
        System.out.println("testFailureDetection passed");
    }

    @Test
    void testLeaderFailureDetection(){
        System.out.println("Running testLeaderFailureDetection");
        ZooKeeperPeerServer toKill = servers.remove(servers.size()-1);

        toKill.shutdown();

        try {
            Thread.sleep(Gossiper.CLEANUP);
        } catch (InterruptedException e) {
            return;
        }

        for (var server : servers) {
            assertTrue(server.isPeerDead(toKill.getServerId()));
        }
        assertTrue(servers.get(servers.size()-1).getUdpPort() != toKill.getUdpPort());
        ZooKeeperPeerServer newLeader = servers.get(servers.size()-1);
        assertEquals(newLeader.getPeerState(), ZooKeeperPeerServer.ServerState.LEADING, "Server " + newLeader.getUdpPort() + " is in state " + newLeader.getPeerState() + "\nwhile it should be a leader");
        for (var server : servers) {
            assertEquals(server.getCurrentLeader().getProposedLeaderID(), (long) newLeader.getServerId());
        }
        System.out.println("testLeaderFailureDetection passed");

    }



    private class sendHttpRequest implements Callable<HttpResponse<String>> {
        private final String src;

        private sendHttpRequest(String src) {
            this.src = src;
        }

        @Override
        public HttpResponse<String> call() throws Exception {
            URI uri = new URL("http", "localhost", GATEWAY_HTTP_PORT, "/compileandrun").toURI();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(uri)
                    .setHeader("Content-type", "text/x-java-source")
                    .POST(BodyPublishers.ofString(src))
                    .build();
            return httpClient.send(request, BodyHandlers.ofString());
        }
    }
}
