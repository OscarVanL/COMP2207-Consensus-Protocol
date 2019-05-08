import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.TimeoutException;

/**
 * @author Oscar van Leusen
 *
 */
public class Participant extends Thread {

    enum failureCondition { SUCCESS, DURING, AFTER }
    enum revoteReason { FAILURE, INCOMPLETE }

    private List<Thread> participantsHigherPort = new ArrayList<>(); //Stores each connection to a participant on a higher port
    private HashMap<Thread, Socket> participantsLowerPort = new HashMap<>(); //Stores each connection to a participant on a lower port
    private boolean connectionsMade = false;
    private Socket socket;
    private PrintWriter out;
    private BufferedReader in;

    private final int listenPort;
    private final int timeout;
    private final failureCondition failureCond;
    private int[] otherParticipants;

    private boolean failed = false;
    private boolean running; //Whether the thread/connection is running as normal
    private int roundNumber = 1;
    private int participantsFailed = 0;
    private boolean majorityVoteSent = false;
    private boolean revoting = false;
    private int votesSharedCount = 0;
    private List<String> voteOptions = new ArrayList<>();
    private String chosenVote; //Randomly chosen vote from this participant
    private int roundVotesReceived = 0; //Number of votes received this voting round
    private final Map<Integer, String> participantVotes = new HashMap<>();

    private Participant(String[] args) throws InsufficientArgumentsException {
        //Bare-minimum number of arguments is 4, <cport> <pport> <timeout> <failurecond>
        if (args.length < 4) {
            throw new InsufficientArgumentsException(args);
        }
        int coordinatorPort = Integer.parseInt(args[0]);
        listenPort = Integer.parseInt(args[1]);
        timeout = Integer.parseInt(args[2]);
        running = true;

        switch (Integer.parseInt(args[3])) {
            case 0:
                failureCond = failureCondition.SUCCESS;
                break;
            case 1:
                failureCond = failureCondition.DURING;
                break;
            case 2:
                failureCond = failureCondition.AFTER;
                break;
            default:
                throw new IllegalArgumentException();
        }


        try {
            socket = new Socket("localhost", coordinatorPort);
            socket.setSoLinger(true,0);
            out = new PrintWriter(socket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            System.out.println("Initialised Participant, listening on port " + listenPort + " with failure condition: " + failureCond);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        while (running) {
            try {
                if (!connectionsMade) {
                    awaitConnections();
                }

                //Waits for all of the participants to be connected before proceeding to send votes
                long startTime = System.currentTimeMillis();
                while (participantsHigherPort.stream()
                        .map(ParticipantClientConnection.class::cast)
                        .anyMatch(e -> !e.isConnected())) {
                    if (System.currentTimeMillis() - startTime > timeout) {
                        running = false;
                        throw new TimeoutException("Other participants did not connect within timeout");
                    }
                }
                connectionsMade = true;

                roundVotesReceived = participantVotes.size(); //Resets vote counter before starting any round

                //Send round 1 votes
                if (roundNumber == 1) {
                    for (Thread thread : participantsLowerPort.keySet()) {
                        ParticipantServerConnection conn = (ParticipantServerConnection) thread;
                        conn.sendVotes(chosenVote);
                        votesSharedCount++;
                        //Simulates failure condition 1 (Failing during step 4 after sharing its vote with some but not all other participants)
                        if (votesSharedCount >= 1 && failureCond == failureCondition.DURING) {
                            System.out.println("INITIATING FAILURE CONDITION 1");
                            closeConnections();
                            failed = true;
                        }
                    }

                    for (Thread thread : participantsHigherPort) {
                        ParticipantClientConnection conn = (ParticipantClientConnection) thread;
                        conn.sendVotes(chosenVote);
                        votesSharedCount++;
                        //Simulates failure condition 1 (Failing during step 4 after sharing its vote with some but not all other participants)
                        if (votesSharedCount >= 1 && failureCond == failureCondition.DURING) {
                            System.out.println("INITIATING FAILURE CONDITION 1");
                            closeConnections();
                            failed = true;
                        }
                    }
                    sleep(100);
                }

                //Send Round n>1 votes
                if (roundNumber > 1 && running) {
                    System.out.println("RUNNING VOTE ROUND " + roundNumber);
                    String votes = generateCombinedVotes();

                    for (Thread thread : participantsLowerPort.keySet()) {
                        ParticipantServerConnection conn = (ParticipantServerConnection) thread;
                        conn.sendCombinedVotes(votes);
                    }

                    for (Thread thread : participantsHigherPort) {
                        ParticipantClientConnection conn = (ParticipantClientConnection) thread;
                        conn.sendCombinedVotes(votes);
                    }
                    revoting = false; //If the loop has come back to here, then this *is* the revote loop.
                    sleep(500);

                }

                //If failure condition 2, fail here
                if (failureCond == failureCondition.AFTER) {
                    System.out.println("INITIATING FAILURE CONDITION 2");
                    closeConnections();
                    failed = true;
                    sleep(500);
                }

                if (participantVotes.size() >= (otherParticipants.length+1) && running && !revoting) {
                    establishWinner();
                }

                roundNumber++;
            } catch (InterruptedException | TimeoutException e) {
                e.printStackTrace();
            }
        }
    }

    private void awaitConnections() {
        try {
            //Opens ServerSocket used to communicate with participants on lower port numbers
            ServerSocket serverSocket = new ServerSocket(listenPort);

            for (int participant : otherParticipants) {
                //If the participant we're connecting to is at a higher port number, that participant acts as a server.
                //If the participant we're connecting to is at a lower port, this participant is the server.
                if (participant > listenPort) {
                    Thread thread = new ParticipantClientConnection(this, participant);
                    participantsHigherPort.add(thread);
                    thread.start();
                } else if (participant < listenPort) {
                    Socket socket = serverSocket.accept();
                    socket.setSoTimeout(timeout);
                    socket.setSoLinger(true,0);
                    System.out.println("Participant connected to this participant");
                    Thread thread = new ParticipantServerConnection(socket, this);
                    participantsLowerPort.put(thread, socket);
                    thread.start();
                } else {
                    throw new ParticipantConfigurationException("Participant has same port as another participant: " + participant);
                }
            }
        } catch (IOException | ParticipantConfigurationException e) {
            e.printStackTrace();
        }
    }

    /**
     * Simulates a participant failing by closing connections to all other participants and coordinator
     */
    private void closeConnections() {
        running = false;
        try {
            for (Thread thread : participantsLowerPort.keySet()) {
                ParticipantServerConnection conn = (ParticipantServerConnection) thread;
                conn.closeConnection();
            }

            for (Thread thread : participantsHigherPort) {
                ParticipantClientConnection conn = (ParticipantClientConnection) thread;
                conn.closeConnection();
            }

            socket.close();
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Removes a client connection if the connection is lost
     * @param participantConnection Connection to client (ParticipantServerConnection or ParticipantClientConnection)
     */
    void connectionLost(Object participantConnection) {
        if (participantConnection instanceof ParticipantServerConnection) {
            participantsHigherPort.remove(participantConnection);
        } else if (participantConnection instanceof ParticipantClientConnection) {
            participantsLowerPort.remove(participantConnection);
        } else {
            System.err.println("connectionLost() called for non-valid object.");
        }
    }

    private void establishWinner() {
        if (roundVotesReceived >= otherParticipants.length+1 && !revoting && !majorityVoteSent) {
            System.out.print("OVERALL VOTES: ");
            for (Map.Entry<Integer, String> vote : participantVotes.entrySet()) {
                System.out.print(vote.getKey() + " " + vote.getValue() + " ");
            }
            System.out.println();

            //Establish winning vote
            Map<String, Integer> votesCount = new HashMap<>();
            for (Map.Entry<Integer, String> vote : participantVotes.entrySet()) {
                Integer port = vote.getKey();
                String option = vote.getValue();

                //Only counts votes not made by itself
                if (port != listenPort) {
                    int count = votesCount.getOrDefault(option, 0);
                    votesCount.put(option, count+1);
                }
            }
            //Adds its own vote
            int count = votesCount.getOrDefault(chosenVote, 0);
            votesCount.put(chosenVote, count+1);

            List<String> majorityOptions = new ArrayList<>();
            int maxVotes=(Collections.max(votesCount.values()));  //Find the maximum vote for any option
            for (Map.Entry<String, Integer> entry : votesCount.entrySet()) {
                if (entry.getValue() == maxVotes) {
                    majorityOptions.add(entry.getKey());     //Add any option matching the maximum vote to the list
                }
            }

            try {
                if (majorityOptions.size() == 1) {
                    System.out.println("MAJORITY VOTE FOUND: " + majorityOptions.get(0));
                    out.println("OUTCOME " + majorityOptions.get(0) +  " " + participantVotes.keySet());
                    majorityVoteSent = true;
                    running = false; //We're done now, so no further loops are required.
                    sleep(timeout);
                    System.exit(0);
                } else {
                    System.out.println("NO OVERALL MAJORITY, TIE BETWEEN: " + majorityOptions.toString());
                    out.println("OUTCOME null " + participantVotes.keySet());
                    majorityVoteSent = true;
                    running = false; //We're done now, so no further loops are required.
                    sleep(timeout);
                    System.exit(0);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }

    /**
     * Called by a connection instance to instigate another vote if a participant connection fails
     */
    void revote(revoteReason reason) {
        if (reason == revoteReason.FAILURE && !failed) {
            if (participantsFailed == 0) {
                participantsFailed++;
                System.out.println("Initiating revote (Participant failure)");
                revoting = true;
                participantsHigherPort.stream()
                        .map(ParticipantClientConnection.class::cast)
                        .forEach(ParticipantClientConnection::revote);
                participantsLowerPort.keySet().stream()
                        .map(ParticipantServerConnection.class::cast)
                        .forEach(ParticipantServerConnection::revote);
                running = true;
            } else {
                //If multiple participants have failed, we fail too as the system is only designed to allow for 1 failure
                closeConnections();
                System.exit(1);
            }
        } else if (reason == revoteReason.INCOMPLETE && !failed) {
            // This is required to handle the scenario where a vote was received from another participant that was not
            // complete. It ensures another round of votes happen to ensure complete sets of votes propagate fully
            revoting = true;
            System.out.println("Flagging revote (A participant has incomplete votes)");
            participantsHigherPort.stream()
                    .map(ParticipantClientConnection.class::cast)
                    .forEach(ParticipantClientConnection::revote);
            participantsLowerPort.keySet().stream()
                    .map(ParticipantServerConnection.class::cast)
                    .forEach(ParticipantServerConnection::revote);
        }
    }

    private void sendJoin() {
        out.println("JOIN " + listenPort);
    }

    /**
     * Awaits the DETAILS [<port>] message from the Coordinator and stores other participants ports
     * @throws IOException Exception thrown if there is an issue with the socket connection
     */
    private void awaitDetails() throws IOException {
        boolean detailsReceived = false;
        while (!detailsReceived) {
            String details = in.readLine();
            String[] detailsElem = details.split(" ");
            if (detailsElem[0].equals("DETAILS")) {
                detailsReceived = true;
                otherParticipants = new int[detailsElem.length-1];
                for (int i=1; i<detailsElem.length; i++) {
                    otherParticipants[i-1] = Integer.parseInt(detailsElem[i]);
                }
                System.out.println("Other participants received: " + Arrays.toString(otherParticipants));
            } else {
                System.err.println("Message received in awaitDetails() that was not 'DETAILS': " + detailsElem[0]);
            }
        }

    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    boolean receiveMessage(String receivedMessage) {
        if (receivedMessage == null) {
            System.err.println("Connected participant connection closed unexpectedly");
            revote(Participant.revoteReason.FAILURE);
            return false;
        } else {
            String[] messageParts = receivedMessage.split(" ");
            if ("VOTE".equals(messageParts[0])) {//If message has 3 parts, eg: VOTE 12345 A, then it is a vote from round 1
                //Otherwise it's a vote from a later round
                if (messageParts.length == 3 && roundNumber == 1) {
                    System.out.println("Vote received in round 1: " + receivedMessage);
                    roundVotesReceived++;
                    synchronized (participantVotes) {
                        participantVotes.put(Integer.parseInt(messageParts[1]), messageParts[2]);
                    }

                } else if (roundNumber > 1) {
                    if (messageParts.length < (otherParticipants.length+1)*2 + 1) {
                        revote(revoteReason.INCOMPLETE);
                    }
                    System.out.println("Vote received in round " + roundNumber + " : " + receivedMessage);
                    for (int i = 1; i < messageParts.length; i += 2) {
                        roundVotesReceived++;
                        synchronized (participantVotes) {
                            participantVotes.put(Integer.parseInt(messageParts[i]), messageParts[i + 1]);
                        }
                    }
                }

                if (participantVotes.size() >= (otherParticipants.length+1) && running && !revoting) {
                    establishWinner();
                }

            }
            return true;
        }
    }

    private String generateCombinedVotes() {
        StringBuilder voteText = new StringBuilder("VOTE ");

        synchronized (participantVotes) {
            for (Map.Entry<Integer, String> vote : participantVotes.entrySet()) {
                voteText.append(vote.getKey()).append(" ").append(vote.getValue()).append(" ");
            }
        }
        return voteText.toString();
    }

    private void awaitOptions() throws IOException {
        boolean optionsReceived = false;
        while (!optionsReceived) {
            String options = in.readLine();
            String[] optionsElem = options.split(" ");
            if (optionsElem[0].equals("VOTE_OPTIONS")) {
                optionsReceived = true;
                voteOptions.addAll(Arrays.asList(optionsElem).subList(1, optionsElem.length));
                System.out.print("Vote Options received: " + voteOptions.toString());
            }
        }
        //Picks a random vote
        Collections.shuffle(voteOptions);
        chosenVote = voteOptions.get(0);
        System.out.print(", selected option: " + chosenVote);
        System.out.println();
    }

    int getPort() {
        return this.listenPort;
    }

    int getTimeout() {
        return this.timeout;
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    boolean isMajorityVoteSent() {
        return this.majorityVoteSent;
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    boolean hasFailed() {
        return this.failed;
    }

    public static void main(String[] args) {
        try {
            Participant participant = new Participant(args);
            participant.sendJoin();
            participant.awaitDetails();
            participant.awaitOptions();
            //Makes connections to other participants
            participant.start();

        } catch (InsufficientArgumentsException | IOException e) {
            e.printStackTrace();
        }
    }

    static class InsufficientArgumentsException extends Exception {
        String[] args;

        InsufficientArgumentsException(String[] args) {
            this.args = args;
        }

        public String toString() {
            return "Insufficient number of arguments: " + Arrays.toString(args);
        }
    }

    static class ParticipantConfigurationException extends Exception {
        String error;

        ParticipantConfigurationException(String error) {
            this.error = error;
        }

        public String toString() {
            return "Invalid configuration of participant: " + error;
        }
    }

}
