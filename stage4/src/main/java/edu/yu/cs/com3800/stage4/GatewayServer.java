package edu.yu.cs.com3800.stage4;
import edu.yu.cs.com3800.LoggingServer;
import java.io.*;
import java.net.*;
import com.sun.net.httpserver.*;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;

import java.lang.*;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.Executors;


public class GatewayServer implements LoggingServer {
    private int myPort;
    private static Logger logger;
    private ZooKeeperPeerServerImpl observerPeer;
    private final InetSocketAddress gatewayAddress;
    private ThreadPoolExecutor executor;
    private AtomicLong request;
    private Map<Long,InetSocketAddress> requestToClient; // may want to make it parallel
    HttpServer server;
    Map<Long, InetSocketAddress> peerIDtoAddress;
    // int myPort, long peerEpoch, Long id, Map<Long, InetSocketAddress> peerIDtoAddress

    public GatewayServer(int httpPort, GatewayPeerServerImpl gatwayPeerServermpl) {
        this.logger = initializeLogging(GatewayServer.class.getCanonicalName() + "on port " + this.myPort);
        this.myPort = httpPort;
        this.observerPeer = gatwayPeerServermpl;
        this.gatewayAddress = this.observerPeer.getAddress();
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        this.peerIDtoAddress = this.observerPeer.getMap();
        // not sure
        this.request = new AtomicLong(0);
        try {
            this.server = HttpServer.create(new InetSocketAddress(this.myPort), 0);
        } catch (IOException e) {
            e.printStackTrace();
        }
        int poolSize = Runtime.getRuntime().availableProcessors() * 2;
        //int poolSize = 50;
        this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(
                poolSize,
                r -> {
                    Thread thread = new Thread(r);
                    thread.setDaemon(true); // Set the thread as a daemon thread
                    return thread;
                }
        );
        server.createContext("/compileandrun", new GatewayServer.CompileAndRunHandler());
        this.server.setExecutor(executor);
        logger.info("GatewayServer started at port " + this.myPort);

    }

    class CompileAndRunHandler implements HttpHandler {

        public void handle(HttpExchange exchange) throws IOException {

            if ("POST".equals(exchange.getRequestMethod())
                    && "text/x-java-source".equals(exchange.getRequestHeaders().getFirst("Content-Type"))) {
                // in ClientImpl content type has to be called Content-Type
                InetSocketAddress leaderAddress = peerIDtoAddress.get(observerPeer.getCurrentLeader().getProposedLeaderID());
                InputStream requestBody = exchange.getRequestBody();
                byte[] sourceCodeBytes = requestBody.readAllBytes();

                try(Socket socketToLeader = new Socket(leaderAddress.getHostName(), leaderAddress.getPort() +2)){
                    // probably need to add request to map
                    Message workToDo = new Message(Message.MessageType.WORK, sourceCodeBytes, gatewayAddress.getHostString(), gatewayAddress.getPort(), leaderAddress.getHostString(), leaderAddress.getPort() + 2, request.getAndIncrement());
                    socketToLeader.getOutputStream().write(workToDo.getNetworkPayload());

                    // Wait for the response from the leader

                    byte[] response = Util.readAllBytesFromNetwork(socketToLeader.getInputStream());
                    Message msgFromLeader = new Message(response);
                    logger.fine("Received response from leader: " + msgFromLeader);
                    byte[] resp = msgFromLeader.getMessageContents();

                    // Code compiled
                    if(!msgFromLeader.getErrorOccurred()){
                        exchange.sendResponseHeaders(200, resp.length);
                        try (OutputStream os = exchange.getResponseBody()) {
                            os.write(resp);
                        }
                    }
                    // Code didn't compile
                    else{
                        exchange.sendResponseHeaders(400, resp.length);
                        try (OutputStream os = exchange.getResponseBody()) {
                            os.write(resp);
                        }
                    }
                    logger.fine("Request ID " + msgFromLeader.getRequestID() + " executed successfully");

                }
                catch (IOException e) {
                    logger.log(Level.SEVERE, "Error communicating with worker", e);
                    // Handle reassignment or retry logic here
                }
            } else {
                // im not sure what to do in this case
                logger.warning("Invalid request: either the request method was not POST or the content type was not text/x-java-source");
                String response = "Invalid request";
                exchange.sendResponseHeaders(400, response.length());
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(response.getBytes());
                }
            }
        }

    }

    public void start(){
        //  this.observerPeer.start();
        this.server.start();
    }

    public void shutdown(){
        //    this.observerPeer.shutdown();
        this.server.stop(0);
    }
}