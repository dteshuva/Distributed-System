package edu.yu.cs.com3800.stage5.demo;

import edu.yu.cs.com3800.stage5.Gossiper;

import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static edu.yu.cs.com3800.stage5.demo.NodeRunner.GATEWAY_HTTP_PORT;
import static edu.yu.cs.com3800.stage5.demo.NodeRunner.NODE_PORTS;

public class Demo {

    private static final HttpClient httpClient = HttpClient.newHttpClient();
    private static final String javaCode = """
            public class HelloWorld {
                public String run() {
                    return "Hello world! X";
                }
            }
            """;

    private static final ExecutorService executor = Executors.newCachedThreadPool();


    public static void main(String[] args) throws Exception {

        // 2. Create a cluster of 7 nodes and one gateway, starting each in their own JVM
        Process[] nodes = new Process[8];
        for (int nodeId = 0; nodeId < 8; nodeId++) {
            nodes[nodeId] = new ProcessBuilder("java", "-cp", "target/classes", "edu.yu.cs.com3800.stage5.demo.NodeRunner", Integer.toString(nodeId)).inheritIO().start();
        }
        Thread.sleep(2000);

        // 3. Wait until the election has completed before sending any requests to the Gateway
        OptionalInt leaderId;
        while ((leaderId = getLeaderFromGateway()).isEmpty()) {
            Thread.sleep(250);
        }

        // 4. Once the gateway has a leader, send 9 client requests.
        // Script should print out both the request and the response from
        // the cluster. In other words, you wait to get all the responses and print them out.
        List<Future<HttpResponse<String>>> responses = new ArrayList<>(9);
        responses.add(0, null);
        for (int i = 1; i <= 9; i++) {
            String request = javaCode.replace("X", Integer.toString(i));
            responses.add(i, executor.submit(new sendCompileAndRun(request)));
            System.out.println(request + "\n");
        }

        for (int i = 1; i <= 9; i++) {
            var response = responses.get(i).get();
            System.out.println("Status code: " + response.statusCode() + "\n" + response.body() +"\n");
        }

        // 5. kill -9 a follower JVM, printing out which one you are killing.
        int toKill = leaderId.getAsInt() == 2 ? 3 : 2;
        System.out.println("Killing follower with ID " + toKill + "\n");
        nodes[toKill].destroyForcibly();
        Thread.sleep(Gossiper.CLEANUP);
        getLeaderFromGateway();

        // 6. kill -9 the leader JVM and then pause 1000 milliseconds.
        toKill = leaderId.getAsInt();
        System.out.println("Killing leader with ID " + toKill + "\n");
        nodes[toKill].destroyForcibly();
        Thread.sleep(1000);

        // Send/display 9 more client requests to the gateway, in the background
        for (int i = 10; i <= 18; i++) {
            String request = javaCode.replace("X", Integer.toString(i));
            responses.add(i, executor.submit(new sendCompileAndRun(request)));
            System.out.println(request + "\n");
        }

        // 7. Wait for the Gateway to have a new leader, and then print out the node ID of the leader.
        leaderId = null;
        Thread.sleep(Gossiper.CLEANUP); // wait for the gateway to notice it is dead (it wont have a new leader if it didn't notice yet)
        while ((leaderId = getLeaderFromGateway()).isEmpty()) {
            Thread.sleep(250);
        }

        // Print out the responses the client receives from the Gateway for the 9 requests sent in step 6.
        for (int i = 10; i <= 18; i++) {
            HttpResponse<String> response = responses.get(i).get();
            System.out.println("Status code: " + response.statusCode() + "\n" + response.body() +"\n");
        }

        // 8. Send/display 1 more client request (in the foreground), print the response
        String request = javaCode.replace("X", "19");
        System.out.println(request + "\n");
        HttpResponse<String> response = new sendCompileAndRun(request).call();
        System.out.println("Status code: " + response.statusCode() + "\n" + response.body() +"\n");

        // 9. List the paths to files containing the Gossip messages received by each node.
        String summaryLoggerName;
        String verboseLoggerName;

        for(int port : NODE_PORTS){

            summaryLoggerName = "Summary-logger-" + Gossiper.class.getCanonicalName()  + "-on-server-with-udpPort-" + port;
            System.out.println("./logs/" + summaryLoggerName + ".log");
            verboseLoggerName = "Verbose-logger-" + Gossiper.class.getCanonicalName()  + "-on-server-with-udpPort-" + port;
            System.out.println("./logs/" + verboseLoggerName + ".log");
        }

        // 10. Shut down all the nodes
        for (int nodeId = 0; nodeId < 8; nodeId++) {
            nodes[nodeId].destroy();
        }
    }

    private static class sendCompileAndRun implements Callable<HttpResponse<String>> {
        private final String src;

        private sendCompileAndRun(String src) {
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


    private static OptionalInt getLeaderFromGateway() throws Exception {
        System.out.println("Asking gateway for leader:");
        URI uri = new URL("http", "localhost", GATEWAY_HTTP_PORT, "/getleader").toURI();
        HttpRequest request = HttpRequest.newBuilder(uri).build();
        HttpResponse<String> response = httpClient.send(request, BodyHandlers.ofString());
        if (response.statusCode() == 200) {
            System.out.println(response.body());
            String[] lines = response.body().split("\n");
            for (String line : lines) {
                if (line.contains("LEADER")) {
                    String[] parts = line.split(" - ");
                    return OptionalInt.of(Integer.parseInt(parts[0].trim()));
                }
            }
        }
        return OptionalInt.empty();
    }

}