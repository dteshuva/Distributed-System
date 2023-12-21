package edu.yu.cs.com3800.stage5;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sun.net.httpserver.*;

import edu.yu.cs.com3800.*;

public class GatewayServer implements LoggingServer {

    private final int myPort;
    private final HttpServer httpServer;
    private final ZooKeeperPeerServerImpl observerPeer;
    private final InetSocketAddress gatewayAddress;
    private final Map<Long, InetSocketAddress> peerIDtoAddress;
    private final Logger logger;
    private final AtomicLong request;

    public GatewayServer(int HTTPServerPort, GatewayPeerServerImpl gatewayPeerServer) throws IOException {
        this.observerPeer = gatewayPeerServer;
        this.gatewayAddress = this.observerPeer.getAddress();
        this.myPort = HTTPServerPort;
        this.peerIDtoAddress = this.observerPeer.getMap();
        this.request = new AtomicLong(0);
        this.logger = initializeLogging(GatewayServer.class.getCanonicalName() + "-on-HTTP-port-" + myPort);

        // Set up the server
        int poolSize = Runtime.getRuntime().availableProcessors() * 2;
        httpServer = HttpServer.create(new InetSocketAddress(HTTPServerPort), 0);
        httpServer.setExecutor(Executors.newFixedThreadPool(poolSize, r -> {
            Thread t = Executors.defaultThreadFactory().newThread(r);
            t.setDaemon(true);
            return t;
        }));
        httpServer.createContext("/compileandrun", new CompileAndRunHandler());
        httpServer.createContext("/getleader", new GetLeader());

        logger.info("GatewayServer started at port " + this.myPort);
    }
    private class GetLeader implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!exchange.getRequestMethod().equals("GET")) {
                String response = "Only GET method is allowed";
                exchange.getResponseHeaders().add("Content-Type", "text/html");
                sendResponse(exchange, HttpURLConnection.HTTP_BAD_METHOD, response);
                return;
            }

            if(observerPeer.getCurrentLeader() == null){
                sendResponse(exchange, HttpURLConnection.HTTP_NO_CONTENT, "");
            }

            StringBuilder st = new StringBuilder();
            for(Long id : peerIDtoAddress.keySet()){
                if(observerPeer.isPeerDead(id))
                    continue;
                if(id == observerPeer.getCurrentLeader().getProposedLeaderID()){
                    st.append(id).append(" - LEADER\n");
                }
                else{
                    st.append(id).append(" - FOLLOWER\n");
                }
            }
            sendResponse(exchange, 200, String.valueOf(st));
        }
    }
    private class CompileAndRunHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {

            if(!checkRequest(exchange)){ return; }

            InputStream requestBody = exchange.getRequestBody();
            byte[] sourceCodeBytes = requestBody.readAllBytes();
            Message msgFromLeader = null;

            boolean isCompleted = false;
            // Label the outer while loop
            outerLoop:
            while (!isCompleted) {
                Vote leader;
                // don't proceed to complete the request until there is a leader
                while ((leader = observerPeer.getCurrentLeader()) == null) {
                    try {
                        Thread.sleep(250);
                    } catch (InterruptedException e) {
                        return;
                    }
                }
                InetSocketAddress leaderAddress = peerIDtoAddress.get(leader.getProposedLeaderID());

                try {
                    Socket socketToLeader = new Socket(leaderAddress.getHostString(), leaderAddress.getPort()+2);

                    Message workToDo = new Message(Message.MessageType.WORK, sourceCodeBytes, gatewayAddress.getHostString(), gatewayAddress.getPort(), leaderAddress.getHostString(), leaderAddress.getPort(), request.getAndIncrement());

                    socketToLeader.getOutputStream().write(workToDo.getNetworkPayload());
                    logger.fine("Message sent to leader with id: " + workToDo.getRequestID());

                    InputStream in = socketToLeader.getInputStream();
                    while (in.available() == 0) {
                        Thread.sleep(500);
                        if(observerPeer.isPeerDead(observerPeer.getCurrentLeader().getProposedLeaderID())){
                            logger.info("Leader has been reported as failed. Trying again");
                            // leader is dead so I will start again to set
                            // a tcp connection with a new leader
                            continue outerLoop;
                        }
                    }
                    byte[] response = Util.readAllBytes(in);
                    msgFromLeader = new Message(response);
                    logger.fine("Response received from leader:\n" + msgFromLeader);
                    socketToLeader.close();
                } catch (ConnectException e) {
                    logger.fine("Gateway couldn't establish a connection with the leader");
                    try {
						Thread.sleep(Gossiper.GOSSIP * 4);
					} catch (InterruptedException e1) {
						return;
					}
                    continue;
                } catch (IOException e) {
                    logger.log(Level.SEVERE, "IOException: ", e);
                } catch (InterruptedException e) {
                    break;
                }
                /*
                    Failed leader, so I'm going to ignore the response and wait for a new leader to be elected to
                    execute the request again
                 */
                 if(!observerPeer.isPeerDead(observerPeer.getCurrentLeader().getProposedLeaderID()))
                     isCompleted = true;
            }

            byte[] resp = msgFromLeader.getMessageContents();
            if (!msgFromLeader.getErrorOccurred()) {
                exchange.sendResponseHeaders(200, resp.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(resp);
                }
            } else {
                // code didn't compile
                exchange.sendResponseHeaders(400, resp.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(resp);
                }
            }
            logger.fine("Request ID " + msgFromLeader.getRequestID() + " executed successfully");
        }

        private boolean checkRequest(HttpExchange exchange) throws IOException {
            if (!exchange.getRequestMethod().equals("POST")) {
                String response = "Only POST method is allowed";
                exchange.getResponseHeaders().add("Content-Type", "text/html");
                sendResponse(exchange, HttpURLConnection.HTTP_BAD_METHOD, response);
                return false;
            }

            if (!"text/x-java-source".equals(exchange.getRequestHeaders().getFirst("Content-Type"))) {
                String response = "Content type must be java";
                sendResponse(exchange, HttpURLConnection.HTTP_BAD_REQUEST, response);
                return false;
            }
            return true;
        }
    }

    private static void sendResponse(HttpExchange exchange, int code, String body) throws IOException {
        exchange.sendResponseHeaders(code, body.length());
        exchange.getResponseBody().write(body.getBytes());
        exchange.close();
    }

    public void start() {
        httpServer.start();
    }

    public void shutdown() {
        httpServer.stop(0);
    }

}
