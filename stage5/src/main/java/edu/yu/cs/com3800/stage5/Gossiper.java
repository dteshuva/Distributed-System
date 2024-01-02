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

    private final ZooKeeperPeerServerImpl peerServer;
    private final LinkedBlockingQueue<Message> incomingMessages;
    private final Long id;
    private final Logger logger;
    private final Logger summaryLogger;
    private final Logger verboseLogger;
    private long heartbeatCounter = 0;
    private Map<Long, HeartBeat> heartbeatTable = new HashMap<>();
    private final HttpServer httpServer;

    public Gossiper(ZooKeeperPeerServerImpl peerServer, LinkedBlockingQueue<Message> incomingMessages) throws IOException {
        this.peerServer = peerServer;
        this.id = peerServer.getServerId();
        this.incomingMessages = incomingMessages;
        this.setDaemon(true);
        int port = peerServer.getUdpPort();
        setName("Gossiper-udpPort-" + port);
        this.logger = initializeLogging(Gossiper.class.getCanonicalName() + "-on-server-with-udpPort-" + port);
        String summaryLoggerName = "Summary-logger-" + Gossiper.class.getCanonicalName()  + "-on-server-with-udpPort-" + port;
        this.summaryLogger = initializeLogging(summaryLoggerName, false);
        String verboseLoggerName = "Verbose-logger-" + Gossiper.class.getCanonicalName()  + "-on-server-with-udpPort-" + port;
        this.verboseLogger = initializeLogging(verboseLoggerName, false);

        httpServer = HttpServer.create(new InetSocketAddress(port + 4), 0);
        httpServer.createContext("/summary", new getLogHandler("./logs/" + summaryLoggerName + ".log"));
        httpServer.createContext("/verbose", new getLogHandler("./logs/" + verboseLoggerName + ".log"));
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
                logger.log(Level.SEVERE,"Bad Method");
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
        Map<Long,InetSocketAddress> map = this.peerServer.getMap();
        // update the table and add all nodes to it
        for(Long id : map.keySet()){
            heartbeatTable.put(id, new HeartBeat(heartbeatCounter, System.currentTimeMillis()));
        }
        while (!this.isInterrupted()) {
            long currentTime = System.currentTimeMillis();
            // Every round - update my own heartbeat
            heartbeatTable.put(id, new HeartBeat(heartbeatCounter++, currentTime));

            try {
                // Get gossip messages off the queue and update
                // the table accordingly
                updateTable(currentTime);
            } catch (IOException | ClassNotFoundException e) {
                this.logger.log(Level.SEVERE, "Encountered a problem updating the heartbeat table");
            }
            // Check all entries of the heartbeat table and check if there are
            // nodes that failed
            checkFailures(currentTime);

            // If a node failed and CLEANUP milliseconds passed - delete
            // from the heartbeat table
            checkCleanUp(currentTime);

            try {
                // Send gossip to a random server
                sendGossip();
            } catch (IOException e) {
                this.logger.log(Level.SEVERE, "Failed to gossip");
            }

            try {
                Thread.sleep(GOSSIP);
            } catch (InterruptedException e) {
                this.logger.log(Level.SEVERE,"Gossiper thread interrupted");
                break;
            }
        }
        this.logger.info("Exiting Gossiper.run()");
    }

    private void updateTable(long currentTime) throws IOException, ClassNotFoundException {
        Queue<Message> otherMessages = new LinkedList<>();
        Message m = null;
        while ((m = incomingMessages.poll()) != null) {
            if (m.getMessageType() == MessageType.GOSSIP) {
                // Deserialize the table from the received gossip message
                HashMap<Long, HeartBeat> receivedTable = deserializeHeartbeatTable(m.getMessageContents());

                Date date = new Date(currentTime);
                SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String dateString = formatter.format(date);

                this.verboseLogger.fine("Gossip from " + m.getSenderHost() + "/" + m.getSenderPort() + " received at " + dateString + ":\n" + receivedTable.toString());

                // Iterate all the entries of the received table, and update my table
                // accordingly while ignoring messages from\about dead peers
                for (Map.Entry<Long, HeartBeat> newTableEntry : receivedTable.entrySet()) {
                    long receivedId = newTableEntry.getKey();
                    long receivedHeartbeat = newTableEntry.getValue().heartbeatCounter();

                    if (!peerServer.isPeerDead(receivedId) && (!heartbeatTable.containsKey(receivedId) || receivedHeartbeat > heartbeatTable.get(receivedId).heartbeatCounter())) {
                        heartbeatTable.put(receivedId, new HeartBeat(receivedHeartbeat, currentTime)); // Add it
                        this.summaryLogger.fine(id + ": updated " + receivedId + "'s heartbeat sequence to " + receivedHeartbeat + " based on message from " + m.getSenderPort() + " at node time " + currentTime);
                    }
                }
            } else {
                // If a message is not gossip add it back to the blocking queue so it will be processed by a different
                // thread
                otherMessages.add(m);
            }
        }
        incomingMessages.addAll(otherMessages);
    }

    private void checkFailures(long currentTime) {
        // Iterate all the entries of the heartbeat table, if didn't receive a heartbeat for
        // for longer than FAIL milliseconds declare the node as failed
        for (Map.Entry<Long, HeartBeat> entry : heartbeatTable.entrySet()) {
            if (peerServer.isPeerDead(entry.getKey())) {
                continue;
            }
            if (currentTime - entry.getValue().time() > FAIL) {
                this.summaryLogger.fine(id + ": no heartbeat from server " + entry.getKey() + " - SERVER FAILED");
                System.out.println(id + ": no heartbeat from server " + entry.getKey() + " - SERVER FAILED");
                peerServer.reportFailedPeer(entry.getKey());
            }
        }
    }

    private void checkCleanUp(long currentTime) {
        //  Delete from the table after failed nodes after CLEANUP milliseconds
        heartbeatTable.entrySet().removeIf(entry -> currentTime - entry.getValue().time() > CLEANUP);
    }

    private void sendGossip() throws IOException {
        // Pick a random peer
        InetSocketAddress randomPeer = peerServer.getRandomPeer();
        if(randomPeer == null)
            return;
        // Send gossip to it
        peerServer.sendMessage(MessageType.GOSSIP, serializeHeartbeatTable(), randomPeer);
        this.logger.fine("Sent gossip message to " + randomPeer);
    }



    private byte[] serializeHeartbeatTable() throws IOException {
        HashMap<Long, HeartBeat> tableToSend = new HashMap<>(heartbeatTable);
        // if a peer is dead there's no point to send it so it will be removed
        // from the table
        for (long id : heartbeatTable.keySet()) {
            if (peerServer.isPeerDead(id)) {
                tableToSend.remove(id);
            }
        }

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
        objectOutputStream.writeObject(tableToSend);
        objectOutputStream.close();

        return byteArrayOutputStream.toByteArray();
    }

    @SuppressWarnings("unchecked")
    private HashMap<Long, HeartBeat> deserializeHeartbeatTable(byte[] data) throws IOException, ClassNotFoundException {

        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
        ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
        Map<?, ?> map = (Map<?, ?>) objectInputStream.readObject();
        objectInputStream.close();

        return (HashMap<Long, HeartBeat>) map;
    }


    public void switchState(){
        this.summaryLogger.fine(this.peerServer.getServerId()+": switching from FOLLOWING to LOOKING");
        System.out.println(this.peerServer.getServerId()+": switching from FOLLOWING to LOOKING");
    }

    private record HeartBeat(long heartbeatCounter, long time) implements Serializable {

    }
}