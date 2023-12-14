package edu.yu.cs.com3800.stage4;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.yu.cs.com3800.*;
import edu.yu.cs.com3800.Message.MessageType;

public class RoundRobinLeader extends Thread implements LoggingServer {

    private static final int THREAD_POOL_SIZE = 16;

    private final ZooKeeperPeerServer peerServer;
    private ServerSocket tcpServer;
    private final List<InetSocketAddress> workers;
    private int nextWorkerIndex = 0;
    private Logger logger;
    private final Map<Long, Message> completedWork = new HashMap<>();
    private final ConcurrentMap<InetSocketAddress, Set<RequestCompleter>> assignedWork = new ConcurrentHashMap<>();
    private ThreadPoolExecutor threadPool;

    public RoundRobinLeader(ZooKeeperPeerServer peerServer, List<InetSocketAddress> workers) throws IOException {
        this.peerServer = peerServer;
        this.workers = workers;
        this.setDaemon(true);
        setName("RoundRobinLeader-udpPort-" + peerServer.getUdpPort());
    }

    public void shutdown() {
        if (tcpServer != null && !tcpServer.isClosed()) {
            try { tcpServer.close(); } catch (IOException e) {}
        }
        interrupt();
    }

    @Override
    public void run() {
        this.logger = initializeLogging(RoundRobinLeader.class.getCanonicalName() + "-on-port-" + peerServer.getUdpPort());
        this.logger.info("Begining role as leader");

        try {
            this.tcpServer = new ServerSocket(peerServer.getUdpPort() + 2);
        } catch (IOException e) {
            this.logger.log(Level.SEVERE, "Error opening socket", e);
            return;
        }
        threadPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(THREAD_POOL_SIZE, r -> {
            // Use a custom thread factory that creates daemon threads
            Thread t = Executors.defaultThreadFactory().newThread(r);
            t.setDaemon(true);
            return t;
        });
        try { gatherCompletedWork(); } catch (IOException e) {} // Gather any completed work from other nodes
        this.logger.fine("Finished gathering completed work from followers - going into leader mode");
        while (!this.isInterrupted()) {
            Socket socket = null;
            Message msgFromGateway = null;
            try {
                // accept a tcp connection from the gateway
                socket = tcpServer.accept();
                this.logger.fine("Accepted TCP connection from " + socket.getInetAddress());
                // read the message from the connection
                byte[] received = Util.readAllBytesFromNetwork(socket.getInputStream());
                msgFromGateway = new Message(received);
                this.logger.fine("Received message from gateway: " + msgFromGateway);
            } catch (SocketException e) {
                this.logger.info("Socket closed while waiting for connection, exiting");
                break;
            } catch (IOException e) {
                this.logger.log(Level.SEVERE, "I/O error occured while connecting to gateway", e);
            }
            if (socket == null || msgFromGateway == null) continue;
            if (completedWork.containsKey(msgFromGateway.getRequestID())) { // If we have already completed this request
                sendCompletedWork(msgFromGateway, socket);
            } else {
                this.logger.fine("Request ID " + msgFromGateway.getRequestID() + " not yet completed.");
                assignWorkToPeer(msgFromGateway, socket);
            }
        }
        threadPool.shutdownNow();
        this.logger.log(Level.SEVERE, "Exiting RoundRobinLeader.run()");
    }

    /**
     * Gathers any work that it, or other nodes, completed as workers after the
     * previous leader died. Eventually the gateway will send a request with the
     * same request ID to the leader, and the leader will simply reply with the
     * already-completed work instead of having a follower redo the work from scratch.
     */
    private void gatherCompletedWork() throws IOException {
        this.logger.fine("Gathering completed work from workers");
        for (InetSocketAddress worker : workers) {
            logger.fine("Requesting completed work from worker " + worker);
            // ask worker for last work
            Message request = new Message(MessageType.NEW_LEADER_GETTING_LAST_WORK,
                    "No Message Content".getBytes(),
                    peerServer.getAddress().getHostString(),
                    peerServer.getUdpPort(),
                    worker.getHostString(),
                    worker.getPort());
            Socket connectionToWorker = new Socket(worker.getHostString(), worker.getPort() + 2);
            connectionToWorker.getOutputStream().write(request.getNetworkPayload());
            // get response from worker
            InputStream in = connectionToWorker.getInputStream();
            while (in.available() == 0) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    connectionToWorker.close();
                    return;
                }
            }
            byte[] fromWorker = Util.readAllBytes(in);
            connectionToWorker.close();
            Message msgFromWorker = new Message(fromWorker);
            if (msgFromWorker.getRequestID() > -1) {
                logger.finer("Received completed work from worker: " + msgFromWorker);
                this.completedWork.put(msgFromWorker.getRequestID(), msgFromWorker);
            } else {
                logger.finer("Worker " + worker + " has no completed work");
            }
        }
    }

    private void sendCompletedWork(Message msgFromGateway, Socket socket) {
        this.logger.fine("Request ID " + msgFromGateway.getRequestID() + " already completed, sending result to gateway");
        Message completedWork = this.completedWork.remove(msgFromGateway.getRequestID());
        Message toSend = new Message(MessageType.COMPLETED_WORK,
                completedWork.getMessageContents(),
                peerServer.getAddress().getHostString(),
                peerServer.getUdpPort(),
                msgFromGateway.getSenderHost(),
                msgFromGateway.getSenderPort(),
                completedWork.getRequestID(),
                completedWork.getErrorOccurred());
        try {
            socket.getOutputStream().write(toSend.getNetworkPayload());
        } catch (IOException e) {
            this.logger.log(Level.SEVERE, "Error sending completed work ID " + msgFromGateway.getRequestID() + " to gateway", e);
        }
    }

    private synchronized void assignWorkToPeer(Message msgFromGateway, Socket socket) {
        InetSocketAddress peer = workers.get(nextWorkerIndex);
        nextWorkerIndex = (nextWorkerIndex + 1) % workers.size(); // Increment nextWorkerIndex (round robin)
        assignedWork.putIfAbsent(peer, Collections.synchronizedSet(new HashSet<RequestCompleter>()));

        RequestCompleter work = new RequestCompleter(msgFromGateway, socket, peer);
        assignedWork.get(peer).add(work);
        threadPool.execute(work);
        this.logger.fine("Request ID " + msgFromGateway.getRequestID() + " Assigned to worker " + peer);
    }

    private synchronized void reassignWork(InetSocketAddress failedPeer) {
        this.logger.finest("Reassigning work from failed peer " + failedPeer);
        Set<RequestCompleter> work = assignedWork.remove(failedPeer);
        if (work == null) return;
        for (RequestCompleter w : work) {
            w.cancel();
            assignWorkToPeer(w.msgFromGateway, w.connectionToGateway);
        }
    }

    public void reportFailedPeer(InetSocketAddress failedPeer) {
        synchronized (this) {
            this.workers.remove(failedPeer);
            if (nextWorkerIndex >= this.workers.size()) nextWorkerIndex = 0;
        }
        if (isAlive()) {
            this.logger.fine("Peer " + failedPeer + " reported as failed");
            // reassign any client request work it had given the dead node to a different node
            reassignWork(failedPeer);
        }
    }

    private class RequestCompleter implements Runnable {

        private final Socket connectionToGateway;
        private final Message msgFromGateway;
        private final InetSocketAddress worker;
        private volatile boolean cancelled = false;

        public RequestCompleter(Message message, Socket socket, InetSocketAddress worker) {
            this.connectionToGateway = socket;
            this.msgFromGateway = message;
            this.worker = worker;
        }

        public void cancel() {
            cancelled = true;
        }

        @Override
        public void run() {
            try {
                // send message to the worker
                Message toSend = new Message(MessageType.WORK,
                        msgFromGateway.getMessageContents(),
                        peerServer.getAddress().getHostString(),
                        peerServer.getUdpPort(),
                        worker.getHostString(),
                        worker.getPort(),
                        msgFromGateway.getRequestID());
                Socket connectionToWorker = new Socket(worker.getHostString(), worker.getPort() + 2);
                connectionToWorker.getOutputStream().write(toSend.getNetworkPayload());

                // Receive response from worker
                InputStream in = connectionToWorker.getInputStream();
                while (in.available() == 0) {
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        connectionToWorker.close();
                        return;
                    }
                    if (cancelled) {
                        connectionToWorker.close();
                        return;
                    }
                }
                byte[] fromWorker = Util.readAllBytes(in);
                connectionToWorker.close();
                Message msgFromWorker = new Message(fromWorker);
                logger.finer("Received message from worker: " + msgFromWorker);

                // Send response to gateway
                toSend = new Message(MessageType.COMPLETED_WORK,
                        msgFromWorker.getMessageContents(),
                        peerServer.getAddress().getHostString(),
                        peerServer.getUdpPort(),
                        msgFromGateway.getSenderHost(),
                        msgFromGateway.getSenderPort(),
                        msgFromWorker.getRequestID(),
                        msgFromWorker.getErrorOccurred());
                connectionToGateway.getOutputStream().write(toSend.getNetworkPayload());
            } catch (IOException e) {}
            assignedWork.get(worker).remove(this);
        }
    }

}
