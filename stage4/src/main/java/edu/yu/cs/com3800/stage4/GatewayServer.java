package edu.yu.cs.com3800.stage4;

import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sun.net.httpserver.*;

import edu.yu.cs.com3800.*;
import edu.yu.cs.com3800.Message.MessageType;

public class GatewayServer implements LoggingServer {

    private static final int THREAD_POOL_SIZE = 8;

    private final int httpPort;
    private final HttpServer httpServer;
    private final GatewayPeerServerImpl peerServer;
    private final Map<Long, InetSocketAddress> peerIDtoAddress;
    private final Logger logger;
    private final AtomicLong nextRequestId;

    static ThreadLocal<Logger> requestHandlerLogger = ThreadLocal.withInitial(() -> {
        String name = "HttpRequestHandler-" + Thread.currentThread().getId();
        return LoggingServer.createLogger(name, name, true);
    });

    public GatewayServer(int HTTPServerPort, GatewayPeerServerImpl gatewayPeerServer) throws IOException {
        this.peerServer = gatewayPeerServer;
        this.httpPort = HTTPServerPort;
        this.peerIDtoAddress = this.peerServer.getMap();
        this.nextRequestId = new AtomicLong(0);
        this.logger = initializeLogging(edu.yu.cs.com3800.stage4.GatewayServer.class.getCanonicalName() + "-on-HTTP-port-" + httpPort);

        // Set up the server
        httpServer = HttpServer.create(new InetSocketAddress(HTTPServerPort), 0);
        httpServer.setExecutor(Executors.newFixedThreadPool(THREAD_POOL_SIZE, r -> {
            // Use a custom thread factory that creates daemon threads
            Thread t = Executors.defaultThreadFactory().newThread(r);
            t.setDaemon(true);
            return t;
        }));
        httpServer.createContext("/compileandrun", new edu.yu.cs.com3800.stage4.GatewayServer.CompileAndRunHandler());
    }

    private class CompileAndRunHandler implements HttpHandler {
        // synchronously communicate with the master/leader over TCP to submit the client request and get a response that it will then return to the client.
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            var logger = requestHandlerLogger.get();
            logger.fine("/compileandrun recieved HTTP " + exchange.getRequestMethod() + " request from " + exchange.getRemoteAddress());

            // check request method
            if (!exchange.getRequestMethod().equalsIgnoreCase("POST")) {
                // if the method is not POST, send a 405 response
                String response = "<h1>405 Method Not Allowed</h1>";
                exchange.getResponseHeaders().add("Content-Type", "text/html");
                sendResponse(exchange, HttpURLConnection.HTTP_BAD_METHOD, response);
                return;
            }

            if (!"text/x-java-source".equals(exchange.getRequestHeaders().getFirst("Content-Type"))) {
                // if the content type isn't correct
                sendResponse(exchange, HttpURLConnection.HTTP_BAD_REQUEST, "");
                return;
            }

            byte[] requestBody = exchange.getRequestBody().readAllBytes();
            long requestId = nextRequestId.getAndIncrement();
            Message responseMsg = null;

            // Send the java code as a work message to the leader
            boolean isCompleted = false;
            // loop until we complete the request and have a response to send back to the client
            while (!isCompleted) {
                Vote leader;
                while ((leader = peerServer.getCurrentLeader()) == null) {
                    // wait for leader to be elected
                    try {
                        Thread.sleep(250);
                    } catch (InterruptedException e) {
                        return;
                    }
                }
                InetSocketAddress leaderAddress = peerIDtoAddress.get(leader.getProposedLeaderID());
                Message msgToLeader = new Message(MessageType.WORK,
                        requestBody,
                        peerServer.getAddress().getHostString(),
                        peerServer.getUdpPort(),
                        leaderAddress.getHostString(),
                        leaderAddress.getPort(),
                        requestId);
                try {
                    logger.finer("Connecting to leader at host " + msgToLeader.getReceiverHost() + " and port " + msgToLeader.getReceiverPort());
                    Socket socket = new Socket(leaderAddress.getHostString(), leaderAddress.getPort()+2);
                    logger.finer("Successfully connected to leader at host " + msgToLeader.getReceiverHost() + " and port " + msgToLeader.getReceiverPort());
                    socket.getOutputStream().write(msgToLeader.getNetworkPayload());
                    logger.fine("Message sent to leader:\n" + msgToLeader.toString());

                    // wait for response from leader
                    InputStream in = socket.getInputStream();
                    while (in.available() == 0) {
                        Thread.sleep(500);
                    }
                    byte[] response = Util.readAllBytes(in);
                    responseMsg = new Message(response);
                    logger.fine("Response received from leader:\n" + responseMsg);
                    socket.close();
                } catch (ConnectException e) {
                    logger.fine("Unable to connect to leader at host " + msgToLeader.getReceiverHost() + " and port " + msgToLeader.getReceiverPort());
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e1) {
                        return;
                    }
                    continue;
                } catch (IOException e) {
                    logger.log(Level.SEVERE, "IOException occured in gateway", e);
                } catch (InterruptedException e) {
                    break;
                }
                isCompleted = true;
            }

            if (responseMsg.getErrorOccurred()) {
                sendResponse(exchange, HttpURLConnection.HTTP_BAD_REQUEST, new String(responseMsg.getMessageContents()));
            } else {
                sendResponse(exchange, HttpURLConnection.HTTP_OK, new String(responseMsg.getMessageContents()));
            }
        }

        private static void sendResponse(HttpExchange exchange, int code, String body) throws IOException {
            exchange.sendResponseHeaders(code, body.length());
            exchange.getResponseBody().write(body.getBytes());
            requestHandlerLogger.get().fine(String.format("""
                    Responded to %s;
                    Code: %d
                    Body: %s""", exchange.getRemoteAddress(), code, body));
            exchange.close();
        }
    }



    public void start() {
        logger.info("Starting gateway server...");
        //     peerServer.start();
        httpServer.start();
        logger.severe("Gateway HTTP server started on port " + httpPort);
    }

    public void shutdown() {
        httpServer.stop(0);
        //    peerServer.shutdown();
        logger.severe("Gateway server stopped");
    }

}
