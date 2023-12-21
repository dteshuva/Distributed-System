package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;
import edu.yu.cs.com3800.ZooKeeperPeerServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RoundRobinLeader extends Thread implements LoggingServer {

    private final int myPort;
    private Logger logger;
    private final ZooKeeperPeerServerImpl peerServer;
    private final Queue<Long> roundRobin; // make sure not to add gateway server id
    private ServerSocket tcpServer;
    private final ExecutorService requestHandlerPool;
    //  private Map<Long,InetSocketAddress> requestToClient; // move from here
    private Map<Long, Message> completedWork;

    private Map<Long, List<Message>> sentWork;
    private Map<Long,Socket> requestToConnection;

    public RoundRobinLeader(ZooKeeperPeerServerImpl peerServer, Map<Long, InetSocketAddress> peerIDtoAddress) {
        //    this.request = 0;
        //  this.requestToClient = new HashMap<>();
        this.myPort = peerServer.getUdpPort();
        this.peerServer = peerServer;
        this.roundRobin = new LinkedList<>();
        this.completedWork = new HashMap<>();
        this.sentWork = new HashMap<>();
        this.requestToConnection = new HashMap<>();
        int threadPoolSize = Runtime.getRuntime().availableProcessors() * 2;
        ThreadFactory daemonThreadFactory = new ThreadFactory() {
            private final AtomicInteger threadNumber = new AtomicInteger(1);

            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setDaemon(true);
                thread.setName("DaemonThread-" + threadNumber.getAndIncrement());
                return thread;
            }
        };

        this.requestHandlerPool = Executors.newFixedThreadPool(threadPoolSize, daemonThreadFactory);
        this.setDaemon(true);
        setName("JavaRunnerFollower-port-" + this.myPort);
        long myId = this.peerServer.getServerId();
        // need to make sure gateway server isn't added - either make sure the map passed doesn't contain its id
        // or make sure to pass its id so I know to delete  or ignore it
        for(long id : peerIDtoAddress.keySet()){
            if(id != myId){
                this.roundRobin.add(id);
                this.sentWork.put(id,new ArrayList<>());
            }
        }
    }

    public void shutdown() {
        if(this.tcpServer != null && !this.tcpServer.isClosed()) {
            try {
                this.tcpServer.close();
            } catch (IOException e) {
                this.logger.log(Level.SEVERE,"Failed closing tcp server");
            }
        }
        if(!this.requestHandlerPool.isShutdown()){
            this.requestHandlerPool.shutdownNow();
        }
        interrupt();
    }

    @Override
    public void run() {
        if(this.logger == null){
            this.logger = initializeLogging(RoundRobinLeader.class.getCanonicalName() + "-on-server-with-udpPort-" + this.myPort);
        }
        this.logger.info("Server with port " + this.myPort + " is beginning role as leader");
        // ... existing initialization code ...
        try {
            // Check if the tcpServer is not null and is open
            if (this.tcpServer != null && !this.tcpServer.isClosed()) {
                // Close the existing tcpServer
                this.tcpServer.close();
            }
            // Create a new ServerSocket
            this.tcpServer = new ServerSocket(this.myPort + 2);
        } catch (IOException e) {
            this.logger.log(Level.SEVERE, "Encountered a problem opening tcp server", e);
            return;
        }

        Message completedMessage = peerServer.getLastWork();
        if(completedMessage != null){
            completedWork.put(completedMessage.getRequestID(), completedMessage);
        }

        // Send requests to all workers to gather work completed for
        // previous leader
        try {
            for(int i = 1; i<= roundRobin.size(); i++){
                this.requestHandlerPool.submit(this::requestForDoneWork);
                Thread.sleep(500);
            }
        } catch (InterruptedException e) {
            this.logger.log(Level.SEVERE,"There has been a problem to gather completed work from all other nodes");
        }
        // Main loop
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            this.logger.log(Level.SEVERE,"Interrupted exception: ",e);
        }
        while (!this.isInterrupted()) {
            try {
                // Accept a TCP connection from the gateway
                // Has danger to be bottleneck
                Socket socketFromGateway = null;
                if(tcpServer != null && !tcpServer.isClosed()){
                    socketFromGateway = tcpServer.accept();
                }

                this.logger.fine("Accepted TCP connection from " + socketFromGateway.getInetAddress());

                // Submit the request handling to the thread pool
                Socket finalSocketFromGateway = socketFromGateway;

                this.requestHandlerPool.submit(() -> handleRequest(finalSocketFromGateway));
            } catch (SocketException e){
                this.logger.info("Socket closed while waiting for connection");
                break;
            }
            catch (IOException e) {
                this.logger.log(Level.SEVERE, "I/O error occurred", e);
                if (Thread.currentThread().isInterrupted()) {
                    this.logger.log(Level.SEVERE, "Thread was interrupted in the middle");
                    break; // Exit loop if interrupted
                }
            }

        }

        // Clean up
        try {
            this.requestHandlerPool.shutdown();
            if (!this.requestHandlerPool.awaitTermination(60, TimeUnit.SECONDS)) {
                this.requestHandlerPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            this.requestHandlerPool.shutdownNow();
        }
        this.logger.info("Exiting RoundRobinLeader.run()");
    }

    private void handleRequest(Socket socketFromGateway) {
        try {
            // Read the message from the gateway
            byte[] received = Util.readAllBytesFromNetwork(socketFromGateway.getInputStream());
            Message msgFromGateway = new Message(received);
            this.logger.fine("Received message from gateway: " + msgFromGateway);

            if(this.completedWork.containsKey(msgFromGateway.getRequestID())){
                Message msgFromWorker = this.completedWork.remove(msgFromGateway.getRequestID());
                socketFromGateway.getOutputStream().write(msgFromWorker.getNetworkPayload());
                this.logger.fine("Work has been done by a worker, there is no need to reassign this work");
            }
            // Assign work to a worker using a TCP connection
            // TCP port = UDP port + 2
            InetSocketAddress workerAddress = getNextWorkerAddress();
            this.requestToConnection.put(msgFromGateway.getRequestID(), socketFromGateway);
            this.logger.info("Attempting to assign work to node with port " + (workerAddress.getPort()));
            // Thread.sleep(1000);
            try (Socket socketToWorker = new Socket(workerAddress.getHostString(), workerAddress.getPort() + 2)) {
                // Send the task to the worker
                socketToWorker.getOutputStream().write(msgFromGateway.getNetworkPayload());

                // Wait for the response from the worker
                byte[] response = Util.readAllBytesFromNetwork(socketToWorker.getInputStream());
                Message msgFromWorker = new Message(response);
                if(peerServer.isPeerDead(workerAddress)){
                    // worker died in the meantime so will reassign the work
                    handleReassign(msgFromGateway);
                }
                this.logger.fine("Received response from worker: " + msgFromWorker);

                // Send the worker's response back to the gateway
                socketFromGateway.getOutputStream().write(msgFromWorker.getNetworkPayload());
            } catch (IOException e) {
                this.logger.log(Level.SEVERE, "Error communicating with worker", e);
                handleReassign(msgFromGateway);
                // Handle reassignment
            }
            this.requestToConnection.remove(msgFromGateway.getRequestID());
        } catch (IOException e) {
            this.logger.log(Level.SEVERE, "Error reading from gateway", e);
        }
    }


    private synchronized InetSocketAddress getNextWorkerAddress() {
        // makae sure gatewayserver isnt here
        if(this.roundRobin.isEmpty())
            return null;
        long id = this.roundRobin.poll();
        InetSocketAddress nextWorker = this.peerServer.getPeerByID(id);
        this.roundRobin.add(id);
        return nextWorker;
    }

    public void requestForDoneWork(){
        InetSocketAddress workerAddress = getNextWorkerAddress();
        this.logger.info("Attempting to assign work to node with port " + (workerAddress.getPort()));
        // Thread.sleep(1000);
        try (Socket socketToWorker = new Socket(workerAddress.getHostString(), workerAddress.getPort() + 2)) {
            // Send the task to the worker
            Message getWork = new Message(Message.MessageType.NEW_LEADER_GETTING_LAST_WORK, "".getBytes(),this.peerServer.getAddress().getHostString(),this.myPort, workerAddress.getHostString(), workerAddress.getPort(), -1);
            socketToWorker.getOutputStream().write(getWork.getNetworkPayload());

            // Wait for the response from the worker
            byte[] response = Util.readAllBytesFromNetwork(socketToWorker.getInputStream());
            Message msgFromWorker = new Message(response);
            this.logger.fine("Received response from worker: " + msgFromWorker);

            // Put the completed work on the queue - if received one
            if(msgFromWorker.getRequestID() != -1)
                this.completedWork.put(msgFromWorker.getRequestID(), msgFromWorker);

        } catch (IOException e) {
            this.logger.log(Level.SEVERE, "Error communicating with worker", e);
            // Handle reassignment or retry logic here
        }
    }

    public void deleteDeadWorker(Long id){
        List<Message> workToDo = this.sentWork.remove(id);
        this.roundRobin.remove(id);
        if(workToDo == null)
            return;
        for(Message w : workToDo){
            this.requestHandlerPool.submit(()->handleReassign(w));
        }

    }

    public void handleReassign(Message work){
        InetSocketAddress workerAddress = getNextWorkerAddress();
        this.logger.info("Attempting to reassign work to node with port " + (workerAddress.getPort()));
        // Thread.sleep(1000);
        try (Socket socketToWorker = new Socket(workerAddress.getHostString(), workerAddress.getPort() + 2)) {
            // Send the task to the worker
            socketToWorker.getOutputStream().write(work.getNetworkPayload());

            // Wait for the response from the worker
            byte[] response = Util.readAllBytesFromNetwork(socketToWorker.getInputStream());
            Message msgFromWorker = new Message(response);
            this.logger.fine("Received response from worker: " + msgFromWorker);

            // Send the worker's response back to the gateway
            Socket socket = this.requestToConnection.remove(msgFromWorker.getRequestID());
            socket.getOutputStream().write(msgFromWorker.getNetworkPayload());
        } catch (IOException e) {
            this.logger.log(Level.SEVERE, "Error communicating with worker", e);
            handleReassign(work);
        }
    }

}