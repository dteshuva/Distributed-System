package edu.yu.cs.com3800.stage5;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.*;

import com.sun.net.httpserver.*;

import edu.yu.cs.com3800.*;
import edu.yu.cs.com3800.Message.MessageType;

public class Gossiper extends Thread implements LoggingServer {

    public static final int GOSSIP = 3000;
    public static final int FAIL = GOSSIP * 10;
    public static final int CLEANUP = FAIL * 2;

    private final ZooKeeperPeerServer peerServer;
    private final LinkedBlockingQueue<Message> incomingMessages;
    private final Long id;
    private final Logger logger;
    private final Logger summaryLogger;
    private final Logger verboseLogger;
    private long heartbeatCounter = 0;
    private Map<Long, HeartbeatData> heartbeatTable = new HashMap<>();
    private final HttpServer httpServer;

    public Gossiper(ZooKeeperPeerServerImpl peerServer, LinkedBlockingQueue<Message> incomingMessages) throws IOException {
        this.peerServer = peerServer;
        this.id = peerServer.getServerId();
        this.incomingMessages = incomingMessages;
        this.setDaemon(true);
        int port = peerServer.getUdpPort();
        setName("Gossiper-udpPort-" + port);
        this.logger = initializeLogging(Gossiper.class.getCanonicalName() + "-on-server-with-udpPort-" + port);

        // Set up summary logger
        this.summaryLogger = initializeLogging("Summary-logger-" + Gossiper.class.getCanonicalName()  + "-on-server-with-udpPort-" + port, false);

        // Set up verbose logger
        this.verboseLogger = initializeLogging("Verbose-logger-" + Gossiper.class.getCanonicalName()  + "-on-server-with-udpPort-" + port, false);

        // Set up http service
        httpServer = HttpServer.create(new InetSocketAddress(port + 4), 0);
        httpServer.createContext("/summary", new getLogHandler("./logs/Summary-logger-Gossip-on-server-with-udpPort-" + port + ".log"));
        httpServer.createContext("/verbose", new getLogHandler("./logs/Verbose-logger-Gossip-on-server-with-udpPort-" + port + ".log"));
        httpServer.start();
    }


    private class getLogHandler implements HttpHandler {
        private final Path logFile;

        public getLogHandler(String path) {
            this.logFile = Path.of(path);
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            logger.fine("Received request from " + exchange.getRemoteAddress());
            // check request method
            if (!exchange.getRequestMethod().equals("GET")) {
                // if the method is not GET, send a 405 response
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_METHOD, 0);
                exchange.close();
                logger.log(Level.SEVERE,"405: Method Not Allowed.");
                return;
            }

            byte[] response = Files.readAllBytes(logFile);

            exchange.getResponseHeaders().add("Content-Type", "text/x-log");
            exchange.sendResponseHeaders(200, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
            logger.fine("Responded to " + exchange.getRemoteAddress() + "successfully");
        }
    }

    public void shutdown() {
        httpServer.stop(0);
        interrupt();
    }

    @Override
    public void run() {
        this.logger.fine("Starting Gossiper thread");
        while (!this.isInterrupted()) {
            long currentTime = System.currentTimeMillis();
            // increment our own heartbeat counter
            heartbeatTable.put(id, new HeartbeatData(heartbeatCounter++, currentTime));

            // Step 1) merge in to its records all new heartbeats / gossip info that the UDP receiver has
            updateTable(currentTime);

            // Step 2) check for failures, using the records it has
            checkForFailures(currentTime);

            // Step 3) clean up old failures that have reached cleanup time
            cleanUpFailures(currentTime);

            // Step 4) gossip to a random peer
            sendGossip();

            // Step 5) sleep for the heartbeat/gossip interval
            try {
                Thread.sleep(GOSSIP);
            } catch (InterruptedException e) {
                this.logger.log(Level.SEVERE,"Gossiper thread interrupted");
                break;
            }
        }
        this.logger.info("Exiting Gossiper.run()");
    }

    private void updateTable(long currentTime) {
        Queue<Message> otherMessages = new LinkedList<>();
        Message m = null;
        while ((m = incomingMessages.poll()) != null) { // For each received gossip message in the queue
            if (m.getMessageType() == MessageType.GOSSIP) {
                // deserialize the gossip message
                HashMap<Long,HeartbeatData> receivedTable = deserializeHeartbeatTable(m.getMessageContents());
                String sender = m.getSenderHost() + ":" + m.getSenderPort();
                logger.finest("Received heartbeat table from " + sender);
                saveToVerboseLog(sender, receivedTable, currentTime);
                // merge the received table into our own
                for (Map.Entry<Long, HeartbeatData> newTableEntry : receivedTable.entrySet()) {
                    long receivedId = newTableEntry.getKey();
                    long receivedHeartbeat = newTableEntry.getValue().heartbeatCounter();
                    // If the peer is alive and not in the table - update it
                    // if it is in the table - update the heartbeat if it's newer
                    if (!peerServer.isPeerDead(receivedId) && (!heartbeatTable.containsKey(receivedId) || receivedHeartbeat > heartbeatTable.get(receivedId).heartbeatCounter())) {
                        heartbeatTable.put(receivedId, new HeartbeatData(receivedHeartbeat, currentTime)); // Add it
                        this.summaryLogger.info(id + ": updated " + receivedId + "'s heartbeat sequence to " + receivedHeartbeat + " based on message from " + sender + " at node time " + currentTime);
                    }
                }
            } else {
                // Im not sure if im going to need it
                otherMessages.add(m);
            }
        }
        incomingMessages.addAll(otherMessages);
    }

    private void checkForFailures(long currentTime) {
        for (Map.Entry<Long, HeartbeatData> entry : heartbeatTable.entrySet()) {
            if (peerServer.isPeerDead(entry.getKey())) {
                continue;
            }
            if (currentTime - entry.getValue().time() > FAIL) {
                this.summaryLogger.info(id + ": no heartbeat from server " + entry.getKey() + " - SERVER FAILED");
                System.out.println(id + ": no heartbeat from server " + entry.getKey() + " - SERVER FAILED");
                peerServer.reportFailedPeer(entry.getKey());
            }
        }
    }
// not sure if good
    private void cleanUpFailures(long currentTime) {
        heartbeatTable.entrySet().removeIf(entry -> currentTime - entry.getValue().time() > CLEANUP);
    }

    private void sendGossip() {
        InetSocketAddress randomPeer = peerServer.getRandomPeer();
        peerServer.sendMessage(MessageType.GOSSIP, serializeHeartbeatTable(), randomPeer);
        this.logger.fine("Sent gossip message to " + randomPeer);
    }

    private record HeartbeatData(long heartbeatCounter, long time) implements Serializable {}

    private byte[] serializeHeartbeatTable() {
        // remove dead peers from the table before sending
        var tableToSend = new HashMap<>(heartbeatTable);
        for (long id : heartbeatTable.keySet()) {
            if (peerServer.isPeerDead(id)) {
                tableToSend.remove(id);
            }
        }
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            new ObjectOutputStream(baos).writeObject(tableToSend);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error serializing heartbeat table", e);
        }
        return baos.toByteArray();
    }

    @SuppressWarnings("unchecked")
    private HashMap<Long, HeartbeatData> deserializeHeartbeatTable(byte[] data) {
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        try {
            return (HashMap<Long, HeartbeatData>) new java.io.ObjectInputStream(bais).readObject();
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error deserializing heartbeat table", e);
        }
        return null;
    }

    private void saveToVerboseLog(String sender, HashMap<Long, HeartbeatData> receivedTable, long currentTime) {
        String time = new SimpleDateFormat("MM/dd/yyyy, HH:mm:ss.S").format(new Date(currentTime));
        this.verboseLogger.info("Message from " + sender + " received at " + time + ":");
        this.verboseLogger.info(receivedTable.toString());
    }

}
