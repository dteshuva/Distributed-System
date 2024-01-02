package edu.yu.cs.com3800;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class ZooKeeperLeaderElection {
    /**
     * time to wait once we believe we've reached the end of leader election.
     */
    private final static int finalizeWait = 200;

    /**
     * Upper bound on the amount of time between two consecutive notification checks.
     * This impacts the amount of time to get the system up again after long partitions. Currently 60 seconds.
     */
    private final static int maxNotificationInterval = 60000;
    private LinkedBlockingQueue<Message> incomingMessages;
    private ZooKeeperPeerServer myPeerServer;
    private long proposedLeader;
    private long proposedEpoch;

    public ZooKeeperLeaderElection(ZooKeeperPeerServer server, LinkedBlockingQueue<Message> incomingMessages) {
        this.incomingMessages = incomingMessages;
        this.myPeerServer = server;
        this.proposedLeader = server.getServerId();
        this.proposedEpoch = server.getPeerEpoch();
        if(server.getPeerState() == ZooKeeperPeerServer.ServerState.OBSERVER){
            this.proposedLeader = -1L;// will break if you propose -1 as id to other servers
        }
    }

    private synchronized Vote getCurrentVote() {
        return new Vote(this.proposedLeader, this.proposedEpoch);
    }

    private void sendNotifications() {
        ElectionNotification n = new ElectionNotification(this.proposedLeader, myPeerServer.getPeerState(), myPeerServer.getServerId(), this.proposedEpoch);
        myPeerServer.sendBroadcast(Message.MessageType.ELECTION, buildMsgContent(n));
    }

    static byte[] buildMsgContent(ElectionNotification notification) {
        ByteBuffer buffer = ByteBuffer.allocate(26);
        buffer.putLong(notification.getProposedLeaderID());
        buffer.putChar(notification.getState().getChar());
        buffer.putLong(notification.getSenderID());
        buffer.putLong(notification.getPeerEpoch());
        buffer.flip();
        return buffer.array();
    }

    private boolean isValidMessage(Message message){
        return message.getReceiverPort() == this.myPeerServer.getUdpPort() && message.getMessageType() == Message.MessageType.ELECTION;
    }

    protected static ElectionNotification getNotificationFromMessage(Message message) {
        byte[] messageContents = message.getMessageContents();
        ByteBuffer buffer = ByteBuffer.wrap(messageContents);
        long proposedLeaderID = buffer.getLong();
        ZooKeeperPeerServer.ServerState state = ZooKeeperPeerServer.ServerState.getServerState(buffer.getChar());
        long senderID = buffer.getLong();
        long peerEpoch = buffer.getLong();
        return new ElectionNotification(proposedLeaderID, state, senderID, peerEpoch);
    }

    public synchronized Vote lookForLeader()
    {
        Queue<Message> otherMessages = new LinkedList<>();
        //send initial notifications to other peers to get things started
        sendNotifications();
        Map<Long, ElectionNotification> receivedVotes = new HashMap<>();
        //Loop, exchanging notifications with other servers until we find a leader
        Message message = null;
        int terminationTime = finalizeWait;
        while (this.myPeerServer.getPeerState() == ZooKeeperPeerServer.ServerState.LOOKING || this.myPeerServer.getCurrentLeader() == null) {
            //Remove next notification from queue, timing out after 2 times the termination time
            try {
                message = this.incomingMessages.poll(2L * terminationTime, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            //if no notifications received..
            //..resend notifications to prompt a reply from others..
            //.and implement exponential back-off when notifications not received..
            if(message == null){
                sendNotifications();
                terminationTime = Math.min(terminationTime * 2,maxNotificationInterval);
                continue;
            }
            //if/when we get a message and it's from a valid server and for a valid server..
            if(isValidMessage(message)) {
                ElectionNotification notification = getNotificationFromMessage(message);
                //switch on the state of the sender:
                switch (notification.getState()) {
                    case LOOKING: //if the sender is also looking
                        //if the received message has a vote for a leader which supersedes mine, change my vote and tell all my peers what my new vote is.
                        //keep track of the votes I received and who I received them from.
                        receivedVotes.put(notification.getSenderID(), notification);
                        if (supersedesCurrentVote(notification.getProposedLeaderID(), notification.getPeerEpoch())) {
                            this.proposedLeader = notification.getProposedLeaderID();
                            this.proposedEpoch = notification.getPeerEpoch();
                            sendNotifications();
                        }
                        ////if I have enough votes to declare my currently proposed leader as the leader:
                        //first check if there are any new votes for a higher ranked possible leader before I declare a leader. If so, continue in my election loop
                        //If not, set my own state to either LEADING (if I won the election) or FOLLOWING (if someone lese won the election) and exit the election
                        if (haveEnoughVotes(receivedVotes, new Vote(this.proposedLeader, this.proposedEpoch))) {
                            if (!incomingMessages.isEmpty()) break;
                            try {
                                Thread.sleep(terminationTime);
                            } catch (InterruptedException ignored) {
                            }
                            if (!incomingMessages.isEmpty()) break;
                            incomingMessages.addAll(otherMessages);
                            return acceptElectionWinner(notification);
                        }

                        break;
                    case FOLLOWING:
                    case LEADING:
                        //if the sender is following a leader already or thinks it is the leader
                        //IF: see if the sender's vote allows me to reach a conclusion based on the election epoch that I'm in, i.e. it gives the majority to the vote of the FOLLOWING or LEADING peer whose vote I just received.
                        //if so, accept the election winner.
                        //As, once someone declares a winner, we are done. We are not worried about / accounting for misbehaving peers.
                        receivedVotes.put(notification.getSenderID(), notification);
                        if (notification.getPeerEpoch() == this.proposedEpoch) {
                            if (haveEnoughVotes(receivedVotes, new Vote(notification.getProposedLeaderID(), notification.getPeerEpoch()))) {
                                return acceptElectionWinner(notification);
                            }
                        }
                        //ELSE: if n is from a LATER election epoch
                        //IF a quorum from that epoch are voting for the same peer as the vote of the FOLLOWING or LEADING peer whose vote I just received.
                        //THEN accept their leader, and update my epoch to be their epoch
                        //ELSE:
                        //keep looping on the election loop.
                        else if (notification.getPeerEpoch() > this.proposedEpoch) {
                            if (haveEnoughVotes(receivedVotes, new Vote(notification.getProposedLeaderID(), notification.getPeerEpoch()))) {
                                this.proposedEpoch = notification.getPeerEpoch();
                                return acceptElectionWinner(notification);
                            }
                        }
                        // If conditions are not met, the loop continues, waiting for more messages
                        break;

                }
            }
            otherMessages.add(message);
          //  incomingMessages.add(message);
        }
        return this.getCurrentVote();
    }


    private Vote acceptElectionWinner(ElectionNotification n) {
        //set my state to either LEADING or FOLLOWING
        if(n.getProposedLeaderID() == this.myPeerServer.getServerId()){
            this.myPeerServer.setPeerState(ZooKeeperPeerServer.ServerState.LEADING);
        }
        else
            this.myPeerServer.setPeerState(ZooKeeperPeerServer.ServerState.FOLLOWING);
        //clear out the incoming queue before returning
        this.incomingMessages.clear();
        // might add something in addition to what I need to clear
        return new Vote(n.getProposedLeaderID(),n.getPeerEpoch());
    }

    /*
     * We return true if one of the following three cases hold:
     * 1- New epoch is higher
     * 2- New epoch is the same as current epoch, but server id is higher.
     */
    protected boolean supersedesCurrentVote(long newId, long newEpoch) {
        return (newEpoch > this.proposedEpoch) || ((newEpoch == this.proposedEpoch) && (newId > this.proposedLeader));
    }

    /**
     * Termination predicate. Given a set of votes, determines if have sufficient support for the proposal to declare the end of the election round.
     * Who voted for who isn't relevant, we only care that each server has one current vote
     */
    protected boolean haveEnoughVotes(Map<Long, ElectionNotification> votes, Vote proposal) {
        //is the number of votes for the proposal > the size of my peer server’s quorum?
        int count = 0;
        for (Vote v : votes.values()) {
            if (proposal.equals(v))
                count++;
        }
        return count >= this.myPeerServer.getQuorumSize();
    }
}