package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.*;
import edu.yu.cs.com3800.Message.MessageType;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ZooKeeperPeerServerImpl extends Thread implements ZooKeeperPeerServer {
    private final InetSocketAddress myAddress;
    private final int myPort;
    private ServerState state;
    private volatile boolean shutdown;
    private LinkedBlockingQueue<Message> outgoingMessages;
    private LinkedBlockingQueue<Message> incomingMessages;
    private final Long id;
    private long peerEpoch;
    private volatile Vote currentLeader = null;
    private Map<Long, InetSocketAddress> peerIDtoAddress;
    private final Long gatewayID;
    private final Set<InetSocketAddress> deadPeers = Collections.synchronizedSet(new HashSet<InetSocketAddress>());
    private final List<InetSocketAddress> livePeers;

    // Worker Threads
    private UDPMessageSender senderWorker;
    private UDPMessageReceiver receiverWorker;
    private JavaRunnerFollower javaRunnerFollower = null;
    private RoundRobinLeader roundRobinLeader = null;

    private Logger logger;

    public ZooKeeperPeerServerImpl(int myPort, long peerEpoch, Long serverID, Map<Long, InetSocketAddress> peerIDtoAddress, Long gatewayID) {
        //code here...
        this.myAddress = new InetSocketAddress("localhost", myPort);
        this.myPort = myPort;
        this.state = ServerState.LOOKING;
        this.outgoingMessages = new LinkedBlockingQueue<>();
        this.incomingMessages = new LinkedBlockingQueue<>();
        this.id = serverID;
        this.gatewayID = gatewayID;
        this.peerEpoch = peerEpoch;
        this.peerIDtoAddress = peerIDtoAddress;
        this.peerIDtoAddress.remove(this.id);
        this.peerIDtoAddress.remove(gatewayID);
        // In this stage all nodes are alive basically
        livePeers = Collections.synchronizedList(new ArrayList<InetSocketAddress>(peerIDtoAddress.values()));
        setName("ZooKeeperPeerServerImpl-udpPort-" + this.myPort);
        this.logger = initializeLogging(ZooKeeperPeerServerImpl.class.getCanonicalName() + "-on-server-with-udpPort-" + this.myPort);
    }

    @Override
    public void shutdown() {
        this.shutdown = true;
        this.senderWorker.shutdown();
        this.receiverWorker.shutdown();
        this.javaRunnerFollower.shutdown();
        this.roundRobinLeader.shutdown();
    }

    @Override
    public synchronized void setCurrentLeader(Vote v) throws IOException {
        this.currentLeader = v;
        this.peerEpoch = v.getPeerEpoch();
    }

    @Override
    public synchronized Vote getCurrentLeader() {
        return this.currentLeader;
    }

    @Override
    public void sendMessage(MessageType type, byte[] messageContents, InetSocketAddress target) throws IllegalArgumentException {
        // fill in
        Message message = new Message(type, messageContents, this.myAddress.getHostString(), this.myPort, target.getHostString(), target.getPort());
        try {
            this.outgoingMessages.put(message);
        } catch (InterruptedException e) {
            this.logger.log(Level.WARNING,"failed to send a message", e);
        }
    }

    @Override
    public void sendBroadcast(MessageType type, byte[] messageContents) {
        for(InetSocketAddress target : this.peerIDtoAddress.values()){
            sendMessage(type, messageContents, target);
        }
    }

    @Override
    public synchronized ServerState getPeerState() {
        return this.state;
    }

    @Override
    public synchronized void setPeerState(ServerState newState) {
        if(this.state == ServerState.OBSERVER)
            return;
        this.state = newState;
    }

    @Override
    public Long getServerId() {
        return this.id;
    }

    @Override
    public long getPeerEpoch() {
        return this.peerEpoch;
    }

    @Override
    public InetSocketAddress getAddress() {
        return this.myAddress;
    }

    @Override
    public int getUdpPort() {
        return this.myPort;
    }

    @Override
    public InetSocketAddress getPeerByID(long peerId) {
        return peerIDtoAddress.get(peerId);
    }

    @Override
    public int getQuorumSize() {
        return (livePeers.size() / 2) + 1 - 1;
    }

    // More relevant for stage5
    @Override
    public void reportFailedPeer(long peerID) {
        this.logger.info("Peer " + peerID + " has been reported as failed");
        InetSocketAddress failedPeer = peerIDtoAddress.get(peerID);
        deadPeers.add(failedPeer);
        livePeers.remove(failedPeer);

        if (peerID == currentLeader.getProposedLeaderID()) {
            peerEpoch++;
            currentLeader = null;
            setPeerState(ServerState.LOOKING);
        }
    }
    // More relevant for stage5
    @Override
    public boolean isPeerDead(InetSocketAddress address) {
        return deadPeers.contains(address);
    }
    // More relevant for stage5
    @Override
    public boolean isPeerDead(long peerID) {
        return deadPeers.contains(peerIDtoAddress.get(peerID));
    }

    public Map<Long, InetSocketAddress> getMap(){ return this.peerIDtoAddress; }

    @Override
    public void run() {
        try {
            // step 1: create thread that sends udp messages
            senderWorker = new UDPMessageSender(this.outgoingMessages, this.myPort);
            // step 2: create thread that listens for udp messages sent to this server
            receiverWorker = new UDPMessageReceiver(this.incomingMessages, this.myAddress, this.myPort, this);


            // step 3: create follower thread
            javaRunnerFollower = new JavaRunnerFollower(this.getUdpPort());
            // step 4: create leader thread
            roundRobinLeader = new RoundRobinLeader(this, this.peerIDtoAddress);

            senderWorker.start();
            receiverWorker.start();
        } catch (IOException e) {
            this.logger.log(Level.SEVERE, "Failed to start worker threads", e);
            return;
        }
        boolean leaderStarted = false;
        boolean followerStarted = false;
        try {
            // step 5: main server loop
            while (!this.shutdown) {
                switch (getPeerState()) {
                    case LOOKING:
                        // start leader election, set leader to the election winner
                        this.logger.fine("Server "+this.id+" starting leader election");
                        Vote newLeader = new ZooKeeperLeaderElection(this, incomingMessages).lookForLeader();
                        setCurrentLeader(newLeader);
                        break;
                    case FOLLOWING:
                        if (!followerStarted) {
                            this.logger.fine("Starting role as Follower...");
                            javaRunnerFollower.start();
                            followerStarted = true;
                        }
                        break;
                    case LEADING:
                        if (!leaderStarted) {
                            this.logger.fine("Starting role as Leader...");
                            roundRobinLeader.start();
                            leaderStarted = true;
                        }
                        break;
                    case OBSERVER:
                        if (currentLeader == null) {
                            this.logger.fine("Server "+this.id+" starting leader election as an observer");
                            this.currentLeader = new ZooKeeperLeaderElection(this, incomingMessages).lookForLeader();
                        }
                }
            }
        } catch (Exception e) {
            this.logger.log(Level.WARNING,"failed to elect a leader", e);
            return;
        }
        this.logger.log(Level.SEVERE,"Exiting ZooKeeperPeerServerImpl.run()");
    }
}
