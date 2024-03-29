package edu.yu.cs.com3800.stage5;

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

import static org.junit.jupiter.api.Assertions.*;

public class Stage5Test {

    private static final int NUM_REQUESTS = 5;

    private static final int[] PEER_SERVER_PORTS = { 9000, 9010, 9020, 9030, 9040, 9050, 9060, 9070 };
 //   private static final int[] PEER_SERVER_PORTS = { 8000, 8010, 8070 };

    private static final int GATEWAY_HTTP_PORT = 8001;

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
        gatewayPeerServer = new GatewayPeerServerImpl(8000, 0, 0L, map);
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
        HttpResponse<String> r = new sendHttpRequest(src).call();
        assertEquals(200, r.statusCode());
        assertEquals(expected, r.body());
    }

    @Test
    public void testConcurrentRequests() throws Exception {
        String src = """
            public class HelloWorld {
                public String run() {
                    return "Hello " + "world! " + X;
                }
            }
        """;

        List<Future<HttpResponse<String>>> responses = new ArrayList<>(NUM_REQUESTS);
        for (int i = 0; i < NUM_REQUESTS; i++) {
            responses.add(i, executor.submit(new sendHttpRequest(src.replace("X", ""+i))));
            //Thread.sleep(1000);
        }

        for (int i = 0; i < NUM_REQUESTS; i++) {
            HttpResponse<String> r = responses.get(i).get();
            assertEquals(200, r.statusCode());
            assertEquals("Hello world! " + i, r.body());
        }

        for(ZooKeeperPeerServer server : servers){
            invokeCall("/summary", server);
            invokeCall("/verbose", server);
        }
    }

    private void invokeCall(String context, ZooKeeperPeerServer server) throws Exception {
        getLogger logger = new getLogger(context, server.getUdpPort());
        System.out.println(logger.call().body());
    }

    @Test
    void testBadRequests() throws InterruptedException, ExecutionException {

        List<Future<HttpResponse<String>>> responses = new ArrayList<>(3);


        responses.add(executor.submit(new sendHttpRequest("""
                public class HelloWorld {
                    public static void main(String[] args) {
                        System.out.println("Hello World");
                    }
                }
            """)));


        responses.add(executor.submit(new sendHttpRequest("""
            public class HelloWorld {
                public HelloWorld(int i) {
                }
                public String run() {
                    return "Hello " + "World " + (40 + 14 / 7);
                }
            }
        """)));

        responses.add(executor.submit(new sendHttpRequest("""
            public class HelloWorld {
                public String run() {
                    return "Hello World "
                }
            }
        """)));

        for (Future<HttpResponse<String>> response : responses) {
            HttpResponse<String> r = response.get();
            assertEquals(400, r.statusCode());
            assertFalse(r.body().isBlank());
        }
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

    private class getLogger implements Callable<HttpResponse<String>>{
        private final String context;
        private final int httpPort;
        private getLogger(String context, int portNum){
            this.context = context;
            httpPort = portNum;
        }

        @Override
        public HttpResponse<String> call() throws Exception {
            URI uri = new URL("http", "localhost", httpPort + 4, context).toURI();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(uri)
                    .setHeader("Content-type", "text/x-java-source")
                    .GET()
                    .build();
            return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        }

    }

}
