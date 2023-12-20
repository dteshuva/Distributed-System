package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.ZooKeeperPeerServer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.*;

public class DemoTest {
    private static final int NUM_REQUESTS = 9;

    private static final int[] PEER_SERVER_PORTS = { 8000, 8010, 8020, 8030, 8040, 8050, 8060, 8070 };
    //   private static final int[] PEER_SERVER_PORTS = { 8000, 8010, 8070 };

    private static final int GATEWAY_HTTP_PORT = 8000;

    private final HttpClient httpClient = HttpClient.newHttpClient();
    ExecutorService executor = Executors.newCachedThreadPool();
    private static GatewayServer gateway;
    private static ArrayList<ZooKeeperPeerServer> servers;
    static GatewayPeerServerImpl gatewayPeerServer;

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

    static void shutdownServers() {
        for (ZooKeeperPeerServer server : servers) {
            server.shutdown();
        }
        gatewayPeerServer.shutdown();
        gateway.shutdown();
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
                    .POST(HttpRequest.BodyPublishers.ofString(src))
                    .build();
            return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        }
    }

    public void printNodes() throws Exception {
        HttpResponse<String> r;
        while(true){
            Thread.sleep(1000);
            r = new getLeaderRequest().call();
            if(r.statusCode() == HttpURLConnection.HTTP_OK)
                break;
        }
        System.out.println(r.body());
    }



    private class getLeaderRequest implements Callable<HttpResponse<String>>{

        private getLeaderRequest(){

        }

        @Override
        public HttpResponse<String> call() throws Exception {
            URI uri = new URL("http", "localhost", GATEWAY_HTTP_PORT, "/getleader").toURI();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(uri)
                    .setHeader("Content-type", "text/x-java-source")
                    .GET()
                    .build();
            return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        }

    }

    public void testConcurrentRequests() throws InterruptedException, ExecutionException {
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
          //  System.out.println(src.replace("X", ""+i));
        }

        for (int i = 0; i < NUM_REQUESTS; i++) {
            HttpResponse<String> r = responses.get(i).get();
            System.out.println(r.statusCode());
        }
    }

    @Test
    public void main() throws Exception {

        // 2 Create a cluster of 7 nodes and one gateway, starting each in their own JVM

        createAndStartServers();

        /*
       3 Wait until the election has completed before sending any requests to the Gateway. In order to do this, you must add
        another http based service to the Gateway which can be called to ask if it has a leader or not. If the Gateway has a
        leader, it should respond with the full list of nodes and their roles (follower vs leader). Script should print out the list of
        server IDs and their roles.
         */

        printNodes();

        /*
        4 Once the gateway has a leader, send 9 client requests. Script should print out both the request and the response from
        the cluster. In other words, you wait to get all the responses and print them out. You can either write a client in java or
        use cURL.
         */

        testConcurrentRequests();

        System.out.println("Sent 9 requests");

        /*
        5 kill -9 a follower JVM, printing out which one you are killing. Wait heartbeat interval * 10 time, and then retrieve and
        display the list of nodes from the Gateway. The dead node should not be on the list
         */

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

        printNodes();

        /*
        6 kill -9 the leader JVM and then pause 1000 milliseconds. Send/display 9 more client requests to the gateway, in the
        background
         */

        toKill = servers.remove(servers.size()-1);

        toKill.shutdown();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            return;
        }

        testConcurrentRequests();

        try {
            Thread.sleep(Gossiper.FAIL);
        } catch (InterruptedException e) {
            return;
        }

        /*
        7 Wait for the Gateway to have a new leader, and then print out the node ID of the leader. Print out the responses the
        client receives from the Gateway for the 9 requests sent in step 6
         */

        System.out.println("The leader is - " + gatewayPeerServer.getCurrentLeader().getProposedLeaderID());

        // Send/display 1 more client request (in the foreground), print the response

        String src = """
            public class HelloWorld {
                public String run() {
                    return "Hello " + "world!";
                }
            }
        """;
        HttpResponse<String> r = new sendHttpRequest(src).call();

        System.out.println(r.body());

        printNodes();

        // List the paths to files containing the Gossip messages received by each node.
        System.out.println("The files containing the Gossip messages received by each node are in the ./logs path along with all other files");

        shutdownServers();
    }

}
