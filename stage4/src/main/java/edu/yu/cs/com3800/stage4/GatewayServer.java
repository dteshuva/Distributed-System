package edu.yu.cs.com3800.stage4;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
        logger.info("GatewayServer started at port " + this.myPort);
    }

    private class CompileAndRunHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!exchange.getRequestMethod().equals("POST")) {
                String response = "Only POST method is allowed";
                exchange.getResponseHeaders().add("Content-Type", "text/html");
                sendResponse(exchange, HttpURLConnection.HTTP_BAD_METHOD, response);
                return;
            }

            if (!"text/x-java-source".equals(exchange.getRequestHeaders().getFirst("Content-Type"))) {
                String response = "Content must be a java file";
                sendResponse(exchange, HttpURLConnection.HTTP_BAD_REQUEST, response);
                return;
            }

            InputStream requestBody = exchange.getRequestBody();
            byte[] sourceCodeBytes = requestBody.readAllBytes();
            long requestId = request.getAndIncrement();
            Message msgFromLeader = null;

            boolean isCompleted = false;

            while (!isCompleted) {
                Vote leader;
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
                    }
                    byte[] response = Util.readAllBytes(in);
                    msgFromLeader = new Message(response);
                    logger.fine("Response received from leader:\n" + msgFromLeader);
                    socketToLeader.close();
                } catch (ConnectException e) {
                    logger.fine("Unable to connect to leader");
                    try {
						Thread.sleep(3000);
					} catch (InterruptedException e1) {
						return;
					}
                    continue;
                } catch (IOException e) {
                    logger.log(Level.SEVERE, "IOException occurred in gateway", e);
                } catch (InterruptedException e) {
                    break;
                }
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

        private static void sendResponse(HttpExchange exchange, int code, String body) throws IOException {
            exchange.sendResponseHeaders(code, body.length());
            exchange.getResponseBody().write(body.getBytes());
            exchange.close();
        }
    }



    public void start() {
        httpServer.start();
    }

    public void shutdown() {
        httpServer.stop(0);
    }

}
