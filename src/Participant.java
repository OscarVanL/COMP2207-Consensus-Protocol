import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * @author Oscar van Leusen
 *
 */
public class Participant extends Thread {

    enum failureCondition { SUCCESS, DURING, AFTER }
    enum revoteReason { FAILURE, INCOMPLETE }

    private List<Thread> participantsHigherPort = new ArrayList<>(); //Stores each connection to a participant on a higher port (ParticipantClientConnection)
    private HashMap<Thread, Socket> participantsLowerPort = new HashMap<>(); //Stores each connection to a participant on a lower port (ParticipantServerConnection)
    private boolean connectionsMade = false;
    private PrintWriter out;
    private BufferedReader in;

    private final int listenPort;
    private final int timeout;
    private final failureCondition failureCond;
    private List<Integer> otherParticipants;

    private boolean failed = false;
    private boolean running; //Whether the thread/connection is running as normal
    private int roundNumber = 1;
    private int votesRequired = 0;
    private int participantsConnected = 0;
    private boolean hasSharedVotes = false; //Flag used to ensure a participant has shared all of its votes before it sends its result to the participant
    private boolean majorityVoteSent = false;
    private boolean revoting = false;
    private int votesSharedCount = 0;
    private List<String> voteOptions = new ArrayList<>();
    private String chosenVote; //Randomly chosen vote from this participant
    private Map<Integer, Long> timeVoteMissing = new HashMap<>(); //Assists in timeout period for missing participant votes
    private final Map<Integer, String> participantVotes = new HashMap<>();
    private List<String> majorityOptions = new ArrayList<>(); //Participant votes with majority of votes (including ties), used during a RESTART round

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
            Socket socket = new Socket("localhost", coordinatorPort);
            socket.setSoLinger(true,0);
            out = new PrintWriter(socket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            System.out.println(listenPort + ": Initialised Participant, listening on " + listenPort + ", failure condition: " + failureCond);
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
                    //Waits for all of the participants to be connected before proceeding to send votes
                    long startTime = System.currentTimeMillis();
                    while (participantsHigherPort.stream()
                            .map(ParticipantClientConnection.class::cast)
                            .anyMatch(e -> !e.isConnected())) {
                        ; //Just stay here until they've all connected...
                    }
                    System.out.println(listenPort + ": ALL PEER-TO-PEER CONNECTIONS ESTABLISHED");
                    connectionsMade = true;
                    //Enables participant timeouts now that connections have been established
                    enableTimeouts();
                    sleep(500);
                }

                //Send round 1 votes
                if (roundNumber == 1) {
                    for (Thread thread : participantsLowerPort.keySet()) {
                        ParticipantServerConnection conn = (ParticipantServerConnection) thread;
                        conn.sendVotes(chosenVote);
                        votesSharedCount++;
                        //Simulates failure condition 1 (Failing during step 4 after sharing its vote with some but not all other participants)
                        if (votesSharedCount >= 1 && failureCond == failureCondition.DURING) {
                            System.out.println(listenPort + ": INITIATING FAILURE CONDITION 1");
                            System.exit(1);
                        }
                    }

                    for (Thread thread : participantsHigherPort) {
                        ParticipantClientConnection conn = (ParticipantClientConnection) thread;
                        conn.sendVotes(chosenVote);
                        votesSharedCount++;
                        //Simulates failure condition 1 (Failing during step 4 after sharing its vote with some but not all other participants)
                        if (votesSharedCount >= 1 && failureCond == failureCondition.DURING) {
                            System.out.println(listenPort + ": INITIATING FAILURE CONDITION 1");
                            System.exit(1);
                        }
                    }
                    hasSharedVotes = true;
                    sleep(100);
                }

                //Send Round n>1 votes
                if (roundNumber > 1 && !majorityVoteSent) {
                    System.out.println(listenPort + ": RUNNING VOTE ROUND " + roundNumber);
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
                    hasSharedVotes = true;
                    sleep(500);
                }

                establishWinner();

                if (!majorityVoteSent) {
                    roundNumber++;
                }
            } catch (InterruptedException | SocketException e) {
                e.printStackTrace();
            }
        }
    }

    ServerSocket serverSocket;

    private void awaitConnections() {
        try {
            //Opens ServerSocket used to communicate with participants on lower port numbers
            if (serverSocket == null) {
                serverSocket = new ServerSocket(listenPort);
            }

            for (int participant : otherParticipants) {
                //If the participant we're connecting to is at a higher port number, that participant acts as a server.
                //If the participant we're connecting to is at a lower port, this participant is the server.
                if (participant > listenPort) {
                    Thread thread = new ParticipantClientConnection(participant);
                    participantsHigherPort.add(thread);
                    thread.start();
                } else if (participant < listenPort) {
                    while (true) {
                        try {
                            Socket socket = serverSocket.accept();
                            socket.setSoLinger(true,0);
                            socket.setSoTimeout(timeout);
                            System.out.println(listenPort + ": Another participant connected to this participant acting as server.");
                            Thread thread = new ParticipantServerConnection(socket);
                            participantsLowerPort.put(thread, socket);
                            thread.start();
                            break;
                        } catch (SocketTimeoutException e) {
                            try {
                                System.out.println("Failed to connect ServerSocket to Participant within timeout, trying again.");
                                Thread.sleep(250);
                            } catch (InterruptedException ex) {
                                ex.printStackTrace();
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }

                } else {
                    throw new ParticipantConfigurationException(listenPort + ": Participant has same port as another participant: " + participant);
                }
                participantsConnected = participantsHigherPort.size() + participantsLowerPort.size();
                votesRequired = participantsConnected + 1;
            }
        } catch (ParticipantConfigurationException | IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Enables timeouts only once all participants have connected, otherwise with large numbers of participants we can
     * find that the first participants to connect will time out before the last ones connect
     */
    private void enableTimeouts() throws SocketException {
        System.out.println(listenPort + ": Enabling timeouts for participants as connections have been established");
        for (Thread connThread : participantsHigherPort) {
            ParticipantClientConnection conn = (ParticipantClientConnection) connThread;
            conn.setTimeout();
        }

        for (Thread connThread : participantsLowerPort.keySet()) {
            ParticipantServerConnection conn = (ParticipantServerConnection) connThread;
            conn.setTimeout();
        }
    }

    /**
     * Removes a client connection if the connection is lost
     * @param participantConnection Connection to client (ParticipantServerConnection or ParticipantClientConnection)
     */
    private void connectionLost(Object participantConnection) {
        if (participantConnection instanceof ParticipantServerConnection) {
            participantsHigherPort.remove(participantConnection);
        } else if (participantConnection instanceof ParticipantClientConnection) {
            participantsLowerPort.remove(participantConnection);
        } else {
            System.err.println("connectionLost() called for non-valid object.");
        }
        participantsConnected--;
    }

    /**
     * Determines whether it's ready to send OUTCOME to the Coordinator, and what this outcome is.
     */
    private void establishWinner() {
        //If we haven't had votes from every connected participant, we need another round of voting (unless the timeout has elapsed).
        if (roundNumber > 1 && participantVotes.size() < votesRequired) {
            //Logs the time the participant's vote was first missing, if it remains missing for the timeout period, we stop expecting to receive it
            for (Integer participant : otherParticipants) {
                if (!participantVotes.keySet().contains(participant)) {
                    if (timeVoteMissing.containsKey(participant)) {
                        if (System.currentTimeMillis() - timeVoteMissing.get(participant) > (timeout * 0.75)) { //Don't wait for the full timeout period in case we still have established connections to other participants that have been left waiting too.
                            System.out.println(listenPort + ": Vote from Participant " + participant + " has been absent for more than the timeout period. Proceeding without that participant's vote.");
                            votesRequired--;
                            timeVoteMissing.remove(participant);
                        }
                    } else {
                        timeVoteMissing.put(participant, System.currentTimeMillis());
                    }
                }
            }
            revote(revoteReason.INCOMPLETE);
        }

        //Ensures two participant connections don't enter establishWinner() simultaneously and send duplicate votes
        //to the Coordinator
        synchronized (this) {
            if (participantVotes.size() >= votesRequired && !revoting && !majorityVoteSent && hasSharedVotes && roundNumber > 1 || participantsConnected == 0) {
                //If failure condition 2 is set, fail here to ensure step 5 does not complete
                if (failureCond == failureCondition.AFTER) {
                    System.out.println(listenPort + ": INITIATING FAILURE CONDITION 2");
                    System.exit(1);
                }

                System.out.print(listenPort + ": OVERALL VOTES: ");
                for (Map.Entry<Integer, String> vote : participantVotes.entrySet()) {
                    System.out.print(vote.getKey() + " " + vote.getValue() + " ");
                }
                System.out.println();

                //Establish winning vote
                Map<String, Integer> votesCount = new HashMap<>();
                for (String vote : participantVotes.values()) {

                    int count = votesCount.getOrDefault(vote, 0);
                    votesCount.put(vote, count + 1);
                }

                int maxVotes = (Collections.max(votesCount.values()));  //Find the maximum vote for any option
                boolean isMajorityVote = false;
                //Ensures we have a majority vote
                if (maxVotes * 2 > participantVotes.size()) {
                    isMajorityVote = true;
                }
                for (Map.Entry<String, Integer> entry : votesCount.entrySet()) {
                    if (entry.getValue() == maxVotes) {
                        majorityOptions.add(entry.getKey());     //Add any option matching the maximum vote to the list (this will result in either 1 outcome, or tied outcomes)
                    }
                }

                try {
                    if (majorityOptions.size() == 1 && !majorityVoteSent && isMajorityVote) {
                        majorityVoteSent = true;
                        running = false; //We're done now, so no further loops are required.
                        System.out.println(listenPort + ": MAJORITY VOTE FOUND: " + majorityOptions.get(0));
                        out.println("OUTCOME " + majorityOptions.get(0) + " " + participantVotes.keySet());

                        //As a majority was found, we can stop now.
                        try {
                            in.readLine(); //Blocks here until the coordinator closes the connection, THEN that SocketException triggers the Participant to close :)
                        } catch (SocketException e) {
                            System.exit(0);
                        }
                    } else if (!majorityVoteSent) {
                        majorityVoteSent = true;
                        if (majorityOptions.size() > 1) {
                            System.out.println(listenPort + ": TIE BETWEEN: " + majorityOptions.toString());
                            for (Map.Entry<String, Integer> entry : votesCount.entrySet()) {
                                System.out.println(listenPort + ": Option: " + entry.getKey() + ", Votes: " + entry.getValue()) ;
                            }
                        } else {
                            System.out.println(listenPort + ": NO OVERALL MAJORITY AMONG OPTIONS: " + votesCount.keySet().toString());
                            for (Map.Entry<String, Integer> entry : votesCount.entrySet()) {
                                System.out.println(listenPort + ": Option: " + entry.getKey() + ", Votes: " + entry.getValue()) ;
                            }
                            //RESTART will use any options voted for in this round
                            majorityOptions.clear();
                            majorityOptions.addAll(votesCount.keySet());
                        }
                        out.println("OUTCOME null " + participantVotes.keySet());
                        awaitRestart();
                        //We didn't reach a majority, so participant continues to run awaiting further instructions from Coordinator
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
        }

    }

    /**
     * Called by a connection instance to instigate another vote if a participant connection fails
     */
    private void revote(revoteReason reason) {
        if (reason == revoteReason.FAILURE && !failed) {
            if (participantVotes.size() < votesRequired) {
                System.out.println(listenPort + ": Initiating revote (Participant failure before all votes propagated)");
                revoting = true;
                hasSharedVotes = false;
            }
        } else if (reason == revoteReason.INCOMPLETE && !failed) {
            // This is required to handle the scenario where a vote was received from another participant that was not
            // complete. It ensures another round of votes happen to ensure complete sets of votes propagate fully
            revoting = true;
            hasSharedVotes = false;
            System.out.println(listenPort + ": Initiating revote (Incomplete votes)");
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
                otherParticipants = new ArrayList<>();
                for (int i=1; i<detailsElem.length; i++) {
                    otherParticipants.add(Integer.parseInt(detailsElem[i]));
                }
                System.out.println(listenPort + ": Other participants: " + otherParticipants.toString());
            } else {
                System.err.println(listenPort + ": Message received in awaitDetails() that was not 'DETAILS': " + detailsElem[0]);
            }
        }
    }

    private void awaitOptions() throws IOException {
        boolean optionsReceived = false;
        while (!optionsReceived) {
            String options = in.readLine();
            String[] optionsElem = options.split(" ");
            if (optionsElem[0].equals("VOTE_OPTIONS")) {
                optionsReceived = true;
                voteOptions.addAll(Arrays.asList(optionsElem).subList(1, optionsElem.length));
                System.out.print(listenPort + ": Vote Options: " + voteOptions.toString());
            }
        }
        //Picks a random vote
        Collections.shuffle(voteOptions);
        chosenVote = voteOptions.get(0);
        participantVotes.put(listenPort, chosenVote);
        System.out.print(", selected: " + chosenVote);
        System.out.println();
    }

    private void awaitRestart() throws IOException {
        String message = in.readLine();
        if (message.equals("RESTART")) {
            System.out.println(listenPort + ": Restarting with previous tied/non-majority options: " + majorityOptions.toString());
            Collections.shuffle(majorityOptions);
            chosenVote = majorityOptions.get(0);
            majorityVoteSent = false;
            hasSharedVotes = false;
            majorityOptions.clear();
            participantVotes.clear();
            timeVoteMissing.clear();
            participantVotes.put(listenPort, chosenVote);
            votesRequired = participantsConnected + 1; //If we're doing a restart, we can't expect a failed participant's vote to propagate (as we did before).
            roundNumber = 1;
            System.out.println(listenPort + ": Selected random option: " + chosenVote);
        }
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private boolean receiveMessage(String receivedMessage, Integer port) throws Coordinator.UnknownMessageException {
        if (receivedMessage == null) {
            System.out.println(listenPort + ": Connected participant connection closed unexpectedly");
            revote(Participant.revoteReason.FAILURE);
            return false;
        } else {
            String[] messageParts = receivedMessage.split(" ");
            if (messageParts[0].equals("VOTE")) {//If message has 3 parts, eg: VOTE 12345 A, then it is a vote from round 1
                //Otherwise it's a vote from a later round
                synchronized (participantVotes) {
                    if (port != null) {
                        System.out.println(listenPort + ": Vote received in round " + roundNumber + ": " + receivedMessage + " from port " + port);
                    } else {
                        System.out.println(listenPort + ": Vote received in round " + roundNumber + ": " + receivedMessage);
                    }

                    for (int i=1; i<messageParts.length; i += 2) {
                        participantVotes.put(Integer.parseInt(messageParts[i]), messageParts[i + 1]);
                    }
                }

                establishWinner();
            } else {
                throw new Coordinator.UnknownMessageException(messageParts[0]);
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

    /**
     * Handles Participant peer-to-peer connection where the connection is designated 'client'
     */
    public class ParticipantClientConnection extends Thread {
        private int participantServerPort;
        private boolean serverConn;
        private volatile boolean running = true;

        private Socket socket;
        private PrintWriter out;
        private BufferedReader in;

        ParticipantClientConnection(int participantServerPort) {
            this.participantServerPort = participantServerPort;

            //This is inside while true in case the connection does not establish to allow retrying.
            while (true) {
                try {
                    socket = new Socket("localhost", participantServerPort);
                    socket.setSoLinger(true,0);
                    socket.setSoTimeout(timeout);
                    out = new PrintWriter(socket.getOutputStream(), true);
                    in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    serverConn = true;
                    System.out.println(listenPort + ": Client participant " + listenPort + " connected to Server participant: " + participantServerPort);
                    break;
                } catch (SocketTimeoutException e) {
                    try {
                        System.out.println("Failed to connect Socket to Server Participant within timeout, trying again.");
                        Thread.sleep(250);
                    } catch (InterruptedException ex) {
                        ex.printStackTrace();
                    }
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }


        }

        @Override
        public void run() {
            while (running && serverConn) {
                //Waits for a message from the Server
                try {
                    if (!receiveMessage(in.readLine(), participantServerPort)) {
                        this.closeConnection();
                    }
                } catch (SocketTimeoutException e) {
                    System.out.println(listenPort + ": Connection to other Participant at port " + participantServerPort + " timed out.");
                    this.closeConnection();
                    if (!majorityVoteSent && !failed) {
                        System.out.println(listenPort + ": A connected participant failed before OUTCOME was sent. Revoting.");
                        revote(Participant.revoteReason.FAILURE);
                    }
                } catch (SocketException e) {
                    System.out.println(listenPort + ": Connection to other Participant at port " + participantServerPort + " closed.");
                    this.closeConnection();
                    if (!majorityVoteSent && !failed) {
                        System.out.println(listenPort + ": A connected participant failed before OUTCOME was sent. Revoting.");
                        revote(Participant.revoteReason.FAILURE);
                    }
                } catch (IOException e) {
                    this.closeConnection();
                    running = false;
                    e.printStackTrace();
                } catch (Coordinator.UnknownMessageException e) {
                    e.printStackTrace();
                }
            }
        }


        void sendVotes(String vote) {
            if (serverConn) {
                System.out.println(listenPort + ": Sending to " + participantServerPort + ": VOTE " + listenPort + " " + vote);
                out.println("VOTE " + listenPort + " " + vote);
            }
        }

        void sendCombinedVotes(String votes) {
            if (serverConn) {
                System.out.println(listenPort + ": Sending to " + participantServerPort + ": "  + votes);
                out.println(votes);
            }
        }

        boolean isConnected() {
            return this.serverConn;
        }

        void setTimeout() throws SocketException {
            this.socket.setSoTimeout(timeout);
        }

        /**
         * Used to simulate a participant failing
         */
        private void closeConnection() {
            connectionLost(this);
            serverConn = false;
            running = false;
            try {
                socket.close();
                in.close();
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }

    /**
     * Handles Participant peer-to-peer connection where the connection is designated 'server'
     */
    public class ParticipantServerConnection extends Thread {
        private Socket socket;
        private PrintWriter out;
        private BufferedReader in;
        private boolean connectionLost = false;
        private volatile boolean running = true;

        ParticipantServerConnection(Socket socket) {
            this.socket = socket;

            try {
                this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                this.out = new PrintWriter(socket.getOutputStream(), true);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void run() {
            while (running) {
                try {
                    if (!receiveMessage(in.readLine(), null)) {
                        this.closeConnection();
                    }
                } catch (SocketTimeoutException e) {
                    System.out.println(listenPort + ": Connection to other Participant timed out.");
                    this.closeConnection();
                    if (!majorityVoteSent && !failed) {
                        System.out.println(listenPort + ": A connected participant failed before OUTCOME was sent. Triggering revote.");
                        revote(Participant.revoteReason.FAILURE);
                    }
                } catch (SocketException e) {
                    System.out.println(listenPort + ": Connection to other Participant closed");
                    this.closeConnection();
                    if (majorityVoteSent && !failed) {
                        System.out.println(listenPort + ": A connected participant failed before OUTCOME was sent. Triggering revote.");
                        revote(Participant.revoteReason.FAILURE);
                    }
                } catch (IOException e) {
                    this.closeConnection();
                    e.printStackTrace();
                } catch (Coordinator.UnknownMessageException e) {
                    e.printStackTrace();
                }
            }
        }

        void sendVotes(String vote) {
            if (!connectionLost && !majorityVoteSent) {
                System.out.println(listenPort + ": Sending: VOTE " + listenPort + " " + vote);
                out.println("VOTE " + listenPort + " " + vote);
            }
        }

        void sendCombinedVotes(String votes) {
            if (!connectionLost && !majorityVoteSent) {
                System.out.println(listenPort + ": Sending: " + votes);
                out.println(votes);
            }
        }

        void setTimeout() throws SocketException {
            if (!connectionLost) {
                this.socket.setSoTimeout(timeout);
            }
        }

        /**
         * Used to simulate a participant failing
         */
        private void closeConnection() {
            connectionLost(this);
            connectionLost = true;
            running = false;
            try {
                socket.close();
                in.close();
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * A Custom Exception which is thrown if a Participant is ran with incorrect arguments
     */
    static class InsufficientArgumentsException extends Exception {
        String[] args;

        InsufficientArgumentsException(String[] args) {
            this.args = args;
        }

        public String toString() {
            return "Insufficient number of arguments: " + Arrays.toString(args);
        }
    }

    /**
     * A custom exception which is thrown if a Participant is configured wrong (has same Port as another Participant)
     */
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
