package edu.yu.cs.com3800.stage4;

import edu.yu.cs.com3800.JavaRunner;
import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class JavaRunnerFollower extends Thread implements LoggingServer {

    private final int myPort;
    private Logger logger;
    private ServerSocket serverSocket;


    public JavaRunnerFollower(int myPort) {
        this.myPort = myPort;
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
            this.logger = initializeLogging(edu.yu.cs.com3800.stage4.JavaRunnerFollower.class.getCanonicalName() + "-on-server-with-udpPort-" + this.myPort);
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
                this.logger.log(Level.SEVERE, "I/O error occured while waiting for a connection.", e);
            }
            if(socket == null)
                continue;

            byte[] received = null;
            try {
                received = Util.readAllBytesFromNetwork(socket.getInputStream());
            } catch (IOException e) {
                this.logger.log(Level.SEVERE, "I/O error occured while reading from connection.", e);
            }
            Message message = new Message(received);
            InetSocketAddress sender = new InetSocketAddress(message.getSenderHost(), message.getSenderPort());
            this.logger.fine("Received message\n" + message);

            Message response;

            String result;
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
            response = new Message(Message.MessageType.COMPLETED_WORK, result.getBytes(), message.getReceiverHost(), message.getReceiverPort(), message.getSenderHost(), message.getSenderPort(), message.getRequestID(), errorOccurred);

            try {
                socket.getOutputStream().write(response.getNetworkPayload());
                this.logger.fine("Responded to " + socket.getInetAddress() + "\n\tResult: " + result);
            } catch (IOException e) {
                this.logger.log(Level.SEVERE, "I/O error occured while sending response", e);
            }
        }

        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException e) {
            this.logger.log(Level.SEVERE, "Error closing server socket", e);
        }
        this.logger.log(Level.SEVERE, "Exiting JavaRunnerFollower.run()");
    }

}