package edu.yu.cs.com3800.stage4;

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
    private final int udpPort;
    private ServerState state;
    private volatile boolean shutdown;
    private LinkedBlockingQueue<Message> outgoingMessages;
    private LinkedBlockingQueue<Message> incomingMessages;
    private final Long id;
    private long peerEpoch;
    private volatile Vote currentLeader;
    private Map<Long, InetSocketAddress> peerIDtoAddress;
    private final int numberOfObservers;
    private final Long gatewayID;
    private final Set<InetSocketAddress> deadPeers = Collections.synchronizedSet(new HashSet<InetSocketAddress>());
    private final List<InetSocketAddress> livePeers;

    // Worker Threads
    private UDPMessageSender senderWorker;
    private UDPMessageReceiver receiverWorker;
    private JavaRunnerFollower javaRunnerFollower;
    private RoundRobinLeader roundRobinLeader;

    private Logger logger;

    public ZooKeeperPeerServerImpl(int udpPort, long peerEpoch, Long serverID, Map<Long, InetSocketAddress> peerIDtoAddress, Long gatewayID) {
        this.myAddress = new InetSocketAddress("localhost", udpPort);
        this.udpPort = udpPort;
        this.state = ServerState.LOOKING;
        this.outgoingMessages = new LinkedBlockingQueue<>();
        this.incomingMessages = new LinkedBlockingQueue<>();
        this.id = serverID;
        this.numberOfObservers = 1;
        this.gatewayID = gatewayID;
        this.peerEpoch = peerEpoch;
        this.peerIDtoAddress = peerIDtoAddress;
        this.peerIDtoAddress.remove(this.id);
        livePeers = Collections.synchronizedList(new ArrayList<InetSocketAddress>(peerIDtoAddress.values()));
        setName("ZooKeeperPeerServerImpl-udpPort-" + this.udpPort);
        this.logger = initializeLogging(ZooKeeperPeerServerImpl.class.getCanonicalName() + "-on-server-with-udpPort-" + this.udpPort);
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
    public void run() {
        try {
            // step 1: create thread that sends udp messages
            senderWorker = new UDPMessageSender(this.outgoingMessages, this.udpPort);
            // step 2: create thread that listens for udp messages sent to this server
            receiverWorker = new UDPMessageReceiver(this.incomingMessages, this.myAddress, this.udpPort, this);
            // step 3: create gossiper thread

            // step 4: create follower thread
            javaRunnerFollower = new JavaRunnerFollower(this.getUdpPort());
            // step 5: create leader thread
            List<InetSocketAddress> workers;
            synchronized (livePeers) {workers = new ArrayList<>(livePeers);}
            workers.remove(peerIDtoAddress.get(gatewayID));
            this.peerIDtoAddress.remove(gatewayID);
            roundRobinLeader = new RoundRobinLeader(this, this.peerIDtoAddress);

            // start helper threads
            senderWorker.start();
            receiverWorker.start();
        } catch (IOException e) {
            this.logger.log(Level.SEVERE, "Failed to start worker threads", e);
            return;
        }
        boolean leaderStarted = false;
        boolean followerStarted = false;
        try {
            // step 3: main server loop
            while (!this.shutdown) {
                switch (getPeerState()) {
                    case LOOKING:
                        // start leader election, set leader to the election winner
                        this.logger.fine("Starting Leader Election...");
                        Vote newLeader = new ZooKeeperLeaderElection(this, incomingMessages).lookForLeader();
                        setCurrentLeader(newLeader);
                        this.logger.fine("Election completed.\nResult: " + newLeader.toString());
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
                            // start leader election, set leader to the election winner
                            this.logger.fine("Starting Leader Election as Observer...");
                            Vote newLeader2 = new ZooKeeperLeaderElection(this, incomingMessages).lookForLeader();
                            setCurrentLeader(newLeader2);
                            this.logger.fine("Election completed.\nResult: " + newLeader2.toString());
                        }
                }
            }
        } catch (Exception e) {
            this.logger.log(Level.WARNING, "Exception caught while server was running", e);
            return;
        }
        this.logger.log(Level.SEVERE,"Exiting ZooKeeperPeerServerImpl.run()");
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
        Message msg = new Message(type, messageContents, this.myAddress.getHostString(), this.udpPort, target.getHostString(), target.getPort());
        this.outgoingMessages.offer(msg);
    }

    @Override
    public void sendBroadcast(MessageType type, byte[] messageContents) {
        synchronized (livePeers) {
            for(InetSocketAddress peer : livePeers) {
                Message msg = new Message(type, messageContents, this.myAddress.getHostString(), this.udpPort, peer.getHostString(), peer.getPort());
                this.outgoingMessages.offer(msg);
            }
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
        return peerEpoch;
    }

    @Override
    public InetSocketAddress getAddress() {
        return this.myAddress;
    }

    @Override
    public int getUdpPort() {
        return this.udpPort;
    }

    @Override
    public InetSocketAddress getPeerByID(long peerId) {
        return peerIDtoAddress.get(peerId);
    }

    @Override
    public int getQuorumSize() {
        return (livePeers.size() / 2) + 1 - numberOfObservers;
    }

    @Override
    public void reportFailedPeer(long peerID) {
        this.logger.info("Peer " + peerID + " has been reported as failed");
        InetSocketAddress failedPeer = peerIDtoAddress.get(peerID);
        deadPeers.add(failedPeer);
        livePeers.remove(failedPeer);

   //     roundRobinLeader.reportFailedPeer(failedPeer);
        if (peerID == currentLeader.getProposedLeaderID()) {
            peerEpoch++;
            currentLeader = null;
            setPeerState(ServerState.LOOKING);
        }
    }

    public void reportFailedPeer(long peerID, Logger logger) {
        this.logger.info("Peer " + peerID + " has been reported as failed");
        InetSocketAddress failedPeer = peerIDtoAddress.get(peerID);
        deadPeers.add(failedPeer);
        livePeers.remove(failedPeer);

     //   roundRobinLeader.reportFailedPeer(failedPeer);
        if (currentLeader != null && peerID == currentLeader.getProposedLeaderID()) {
            peerEpoch++;
            currentLeader = null;
            if (state != ServerState.OBSERVER) {
                logger.info(id + ": switching from " + state + " to " + ServerState.LOOKING);
                System.out.println(id + ": switching from " + state + " to " + ServerState.LOOKING);
            }
            setPeerState(ServerState.LOOKING);
        }
    }

    @Override
    public boolean isPeerDead(InetSocketAddress address) {
        return deadPeers.contains(address);
    }

    @Override
    public boolean isPeerDead(long peerID) {
        return deadPeers.contains(peerIDtoAddress.get(peerID));
    }

    public Map<Long, InetSocketAddress> getMap(){ return this.peerIDtoAddress; }

}
