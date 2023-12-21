package edu.yu.cs.com3800.stage5.demo;

import edu.yu.cs.com3800.stage5.GatewayPeerServerImpl;
import edu.yu.cs.com3800.stage5.GatewayServer;
import edu.yu.cs.com3800.stage5.ZooKeeperPeerServerImpl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class DemoScript {

    private static final int CLUSTER_NUM = 8;
    private static final Long GATEWAY_ID = 0L;
    private static final int NUM_OBSERVERS = 1;
    private static final int GATEWAY_UDP_PORT = 8090;
    private static final int GATEWAY_HTTP_PORT = 8080;

    // 2x check that this is ok!
    private static final HttpClient httpClient = HttpClient.newHttpClient();

    private static final int N_THREADS = Runtime.getRuntime().availableProcessors() * 2;

    private static final URI uri1 = createURI("/gettheleader");
    private static final URI uri2 = createURI("/compileandrun");

    // 2. Create a cluster of 7 nodes and one gateway, starting each in their own JVM.
    private static final int[] clusterPorts = {8081, 8082, 8083, 8084, 8085, 8086, 8087};

    // @todo: Change me
    private static final String validClass =
            "package edu.yu.cs.com3800.stage5;\n\n" +
            "public class HelloWorld\n{\n" +
            "    public String run()\n    {\n" +
            "        return \"Hello world!\";\n" +
            "    }\n" +
            "}\n";

    private static URI createURI(String method) {
        return URI.create("http://localhost:" + GATEWAY_HTTP_PORT + method);

    }

    public static void main(String[] args) {
        System.out.println("Step 1. Build your code using mvn test, thus running your Junit tests.");

        ExecutorService threadPool = createThreadPool();

        // 2. Create a cluster of 7 nodes and one gateway, starting each in their own JVM.
        final Map<Long, Long> serverProcessMap = createAndStartServers();

        // Step 3: Check for leader election
        long leaderId = step3();

        // Step 4: Send 9 client requests
        step4(threadPool);

        // Step 5: Kill a follower and check cluster state
        step5(serverProcessMap);

        // Step 6: Kill the leader and send 9 client requests
        List<Future<Response>> futures = step6(leaderId, threadPool, serverProcessMap);

        // Step 7: Wait for new leader and check responses.
        leaderId = step7(futures);

        // Step 8: Send and display 1 more client request
        step8(threadPool);

        // Step 9: List paths to gossip message files
        step9(serverProcessMap.size());

        // Step 10: Shut down all nodes
        step10(serverProcessMap);
    }

    private static ExecutorService createThreadPool () {
        return Executors.newFixedThreadPool(N_THREADS, mew -> {
            Thread thread = new Thread(mew);
            thread.setDaemon(true);
            return thread;
        });
    }

    // 2. Create a cluster of 7 nodes and one gateway, starting each in their own JVM.
    private static Map<Long, Long> createAndStartServers() {
        System.out.println("Step 2. Creating a cluster of 7 nodes and one gateway & starting each in their own JVM.");

        Map<Long, Long> serverProcessMap = new HashMap<>();

        final String classPath = System.getProperty("java.class.path");
        final String command = "java";
        final String classpathOption = "-classpath";
        final String serverClass = "edu.yu.cs.com3800.stage5.demo.DemoScript";

        for (long serverId = 0; serverId < CLUSTER_NUM; serverId++) {
            String serverArg = String.valueOf(serverId);
            ProcessBuilder processBuilder = new ProcessBuilder(command, classpathOption, classPath, serverClass, serverArg);
            processBuilder.inheritIO(); // Ensures that IO is inherited from the current process
            try {
                Process process = processBuilder.start();
                serverProcessMap.put(serverId, process.pid());
            } catch (IOException e) {
                System.out.println("Failed to start server with ID: " + serverId);
                e.printStackTrace();
            }
        }

        // 2x check this is necessary.
        try {
            Thread.sleep(50_000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return serverProcessMap;
    }

    /**
     * 3. Wait until the election has completed before sending any requests to the Gateway.
     * In order to do this, you must add another http based service to the Gateway which can be called to ask if it has a leader or not.
     * If the Gateway has a leader, it should respond with the full list of nodes and their roles (follower vs leader).
     * Script should print out the list of server IDs and their roles.
     */
    private static long step3() {
        System.out.println("Step 3: Check for leader election.");

        // Could create helper method
        HttpResponse<String> response = checkForLeaderElection();
        String body = response.body();
        long leaderId = Long.parseLong(body.substring(0, 1));
        return leaderId;
    }

    /**
     * 4. Once the gateway has a leader, send 9 client requests.
     * Script should print out both the request and the response from the cluster.
     * In other words, you wait to get all the responses and print them out.
     * You can either write a client in java or use cURL.
     */
    private static void step4(final ExecutorService threadPool) {
        // @todo: Make sure we are printing out the request!
        System.out.println("Step 4. Sending 9 client requests.");
        List<Future<Response>> futures = sendClientRequests(9, 0, threadPool);
        printOutResponses(futures);
    }

    /**
     * 5. kill -9 a follower JVM, printing out which one you are killing.
     * Wait heartbeat interval * 10 time, and then retrieve and
     * display the list of nodes from the Gateway.
     * The dead node should not be on the list
     */
    private static void step5(final Map<Long, Long> serverProcessMap) {
        System.out.println("Step 5. Killing a follower");
        // Kill follower server 3.
        // Why?
        killServerAndCheckCluster(3L, 20_000, serverProcessMap);
    }

    /**
     * 6. kill -9 the leader JVM and then pause 1000 milliseconds.
     * Send/display 9 more client requests to the gateway, in the background.
     */
    private static List<Future<Response>> step6(final long serverIdToKill, final ExecutorService threadPool, final Map<Long, Long> serverProcessMap) {
        System.out.println("Step 6. Killing a leader and sending 9 more client requests.");
        killServerAndCheckCluster(serverIdToKill, 1_000, serverProcessMap);
        return sendClientRequests(9, 9, threadPool);
    }

    /**
     * 7. Wait for the Gateway to have a new leader, and then print out the node ID of the leader.
     * Print out the responses the client receives from the Gateway for the 9 requests sent in step 6.
     * Do not proceed to step 8 until all 9 requests have received responses.
     */
    private static long step7(List<Future<Response>> futures) {
        System.out.println("Step 7. Wait for new leader and check responses.");
        HttpResponse<String> response = waitForNewLeader();
        long leaderId = Long.parseLong(response.body().substring(0, 1));
        System.out.println("Elected new leader: " + leaderId);

        printOutResponses(futures);

        return leaderId;
    }

    /**
     * 8. Send/display 1 more client request (in the foreground), print the response
     */
    private static void step8(ExecutorService threadPool) {
        System.out.println("Step 8. Send/display 1 more client request (in the foreground), print the response.");
        sendClientRequests(1, 18, threadPool);
    }

    /**
     * 9. List the paths to files containing the Gossip messages received by each node.
     */
    private static void step9(final int serverCount) {
        System.out.println("Step 9. List the paths to files containing the Gossip messages received by each node.");

        System.out.println("Listing Gossip Message Paths for each node:");
        for (int serverId = 0; serverId < serverCount; serverId++) {
            String filePath = "/path/to/gossip/messages/node_" + serverId + ".log";
            System.out.println("Node " + serverId + ": " + filePath);
        }
    }

    /**
     * 10. Shut down all the nodes
     */
    private static void step10(final Map<Long, Long> serverProcessMap) {
        System.out.println("Step 10. Shutting down all nodes.");

        for (Long serverId : serverProcessMap.keySet()) {
            long processId = serverProcessMap.get(serverId);
            try {
                Runtime.getRuntime().exec("kill -9 " + processId);
                // ProcessHandle.of(processId).ifPresent(ProcessHandle::destroy);
                System.out.println("Node " + serverId + " with process ID " + processId + " has been shut down.");
            } catch (Exception e) {
                System.out.println("Failed to shut down node " + serverId);
                e.printStackTrace();
            }
        }

    }

    private static HttpResponse<String> checkForLeaderElection() {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(uri1)
                .GET()
                // 2x check!
                .header("Content-Type", "get-leader")
                .build();

        while (true) {
            try {
                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                if (response != null && !response.body().equals("No Leader")) {
                    System.out.println("Leader elected: " + response.body());
                    return response;
                }
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
            // 2x check that need to wait
            try {
                Thread.sleep(1000); // Wait before retrying
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    // private static HttpRequest buildHttpRequest(URI uri) {}

    private static List<Future<Response>> sendClientRequests(int numberOfRequests, int startIndex, ExecutorService threadPool) {
        List<Future<Response>> futures = new ArrayList<>();
        for (int i = startIndex; i < startIndex + numberOfRequests; i++) {
            int finalI = i;

            System.out.println(finalI + " sent message: " + i);

            // @todo: Change me
            String code = validClass.replace("world!", "world! from code version " + i);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(uri2)
                    .POST(HttpRequest.BodyPublishers.ofString(code))
                    // 2x check
                    .header("Content-Type", "text/x-java-source")
                    .build();

            Future<Response> responseFuture = threadPool.submit(() -> {
                try {
                    HttpClient httpClient = HttpClient.newHttpClient();
                    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                    return new Response(response.statusCode(), response.body(), finalI);
                } catch (IOException | InterruptedException e) {
                    System.out.println(""); // Add error message.
                    // Message
                    e.printStackTrace();
                    // Should we still return a Response object?
                    return null;
                }
            });

            futures.add(responseFuture);
        }
        return futures;
    }

    private static void printOutResponses(List<Future<Response>> futures) {
        for (Future<Response> future : futures) {
            try {
                System.out.println("Response: " + future.get());
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
    }

    private static void killServerAndCheckCluster(final long serverIdToKill, final int timout, final Map<Long, Long> serverProcessMap) {
        try {
            Runtime.getRuntime().exec("kill -9 " + serverIdToKill);
            serverProcessMap.remove(serverIdToKill);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Wait for cluster state to update
        try {
            // Adjust according to heartbeat interval
            Thread.sleep(timout);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        checkForLeaderElection();
    }

    private static HttpResponse<String> waitForNewLeader() {
        System.out.println("Waiting for a new leader to be elected.");

        HttpRequest request = HttpRequest.newBuilder()
                .uri(uri1)
                .GET()
                .header("Content-Type", "get-leader")
                .build();

        while (true) {
            try {
                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                if (response != null && !response.body().equals("No Leader")) {
                    System.out.println("New Leader Elected: " + response.body());
                    return response;
                } else {
                    // No leader yet, wait for a short period before trying again
                    Thread.sleep(1_000);
                }

            } catch (IOException | InterruptedException e) {
                System.out.println("An error occurred while checking for a new leader.");
                e.printStackTrace();
                // Optional: Decide if you want to break the loop or try again
            }
        }
    }
}















