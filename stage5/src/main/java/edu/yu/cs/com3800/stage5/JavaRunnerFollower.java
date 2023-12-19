package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class JavaRunnerFollower extends Thread implements LoggingServer {

    private final int myPort;
    private Logger logger;
    private ServerSocket serverSocket;
    private ZooKeeperPeerServer peerServer;
    private Queue<Message> messageQueue;


    public JavaRunnerFollower(ZooKeeperPeerServer peerServer, int myPort) {
        this.myPort = myPort;
        this.peerServer = peerServer;
        this.messageQueue = new LinkedList<>();
        this.setDaemon(true);
        setName("JavaRunnerFollower-port-" + this.myPort);
    }

    public void shutdown() {
        interrupt();
        try {
            if (this.serverSocket != null && !this.serverSocket.isClosed()) {
                this.serverSocket.close();
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Error closing server socket", e);
        }
    }

    @Override
    public void run() {
        if (this.logger == null) {
            this.logger = initializeLogging(JavaRunnerFollower.class.getCanonicalName() + "-on-server-with-udpPort-" + this.myPort);
        }

        try {
            this.serverSocket = new ServerSocket(this.myPort + 2);
        } catch (IOException e) {
            this.logger.log(Level.SEVERE, "Error opening server socket", e);
            return;
        }

        while (!this.isInterrupted()) {
            Socket socket = null;
            try{
                socket = serverSocket.accept();
            } catch (SocketException e) {
                this.logger.info("Socket closed while waiting for a connection.");
                break;
            } catch (IOException e) {
                this.logger.log(Level.SEVERE, "I/O error occurred while waiting for a connection.", e);
            }
            if(socket == null)
                continue;

            byte[] received = null;
            try {
                received = Util.readAllBytesFromNetwork(socket.getInputStream());
            } catch (IOException e) {
                this.logger.log(Level.SEVERE, "I/O error occurred while reading from connection.", e);
            }
            Message message = new Message(received);
            InetSocketAddress sender = new InetSocketAddress(message.getSenderHost(), message.getSenderPort());
            if(this.peerServer.isPeerDead(sender)){
                this.logger.info("Leader has been reported as failed so message is ignored");
                continue;
            }
            this.logger.fine("Received message\n" + message);

            if(message.getMessageType() == Message.MessageType.WORK){
                processAndRespondToMessage(socket, message);
            }
            else if(message.getMessageType() == Message.MessageType.NEW_LEADER_GETTING_LAST_WORK){
                Message response;
                if(messageQueue.isEmpty()){
                    String result = "";
                    response = new Message(Message.MessageType.COMPLETED_WORK, result.getBytes(), message.getReceiverHost(), message.getReceiverPort(), message.getSenderHost(), message.getSenderPort(), message.getRequestID());
                }
                else{
                    response = messageQueue.poll();
                }

                try {
                    socket.getOutputStream().write(response.getNetworkPayload());
                    this.logger.fine("Responded to " + socket.getInetAddress() + " with last response");
                } catch (IOException e) {
                    this.logger.log(Level.SEVERE, "I/O error occurred while sending response", e);
                }
            }
        }

        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException e) {
            this.logger.log(Level.SEVERE, "Error closing server socket", e);
        } // until here
        this.logger.log(Level.SEVERE, "Exiting JavaRunnerFollower.run()");
    }

    private void processAndRespondToMessage(Socket socket, Message message) {
        Message response;
        String result;
        InetSocketAddress sender = new InetSocketAddress(message.getSenderHost(), message.getSenderPort());
        boolean errorOccurred = false;

        try {
            JavaRunner javaRunner = new JavaRunner();
            result = javaRunner.compileAndRun(new ByteArrayInputStream(message.getMessageContents()));
            if (result == null) {
                result = "";
            }
            this.logger.fine("Successfully compiled and ran request ID: " + message.getRequestID());
        } catch (Exception e) {
            result = e.getMessage() + '\n' + Util.getStackTrace(e);
            errorOccurred = true;
            this.logger.fine("Failed to compile and run:\n\tRequest ID: " + message.getRequestID() + "\n\tError:" + e.getMessage());
        }

        // Respond with the result
        // If the leader died before sending back the response the message will be queued
        response = new Message(Message.MessageType.COMPLETED_WORK, result.getBytes(), message.getReceiverHost(), message.getReceiverPort(), message.getSenderHost(), message.getSenderPort(), message.getRequestID(), errorOccurred);
        if(this.peerServer.isPeerDead(sender)){
            this.messageQueue.offer(response);
            this.logger.info("Leader has died. Queuing the response to be sent to new leader");
            return;
        }
        try {
            socket.getOutputStream().write(response.getNetworkPayload());
            this.logger.fine("Responded to " + socket.getInetAddress() + "\n\tResult: " + result);
        } catch (IOException e) {
            this.logger.log(Level.SEVERE, "I/O error occurred while sending response", e);
        }
    }

    public Message lastWork(){
        if(this.messageQueue.isEmpty())
            return null;
        return this.messageQueue.poll();
    }


}