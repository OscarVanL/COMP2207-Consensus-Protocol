import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Oscar van Leusen
 *
 */
public class Participant extends Thread {

    enum failureCondition { SUCCESS, DURING, AFTER }

    private List<Thread> participantsHigherPort = new ArrayList<>(); //Stores each connection to a participant on a higher port
    private HashMap<Thread, Socket> participantsLowerPort = new HashMap<>(); //Stores each connection to a participant on a lower port
    private boolean connectionsMade = false;
    private Socket socket;
    private PrintWriter out;
    private BufferedReader in;

    private final int coordinatorPort;
    private final int listenPort;
    private final int timeout;
    private final failureCondition failureCond;
    private int[] otherParticipants;
    private boolean failed = false;
    private boolean running; //Whether the thread/connection is running as normal
    private int roundNumber = 1;
    private boolean majorityVoteSent = false;
    private int votesSharedCount = 0;
    private List<String> voteOptions = new ArrayList<>();
    private String chosenVote; //Randomly chosen vote from this participant
    protected Map<Integer, String> participantVotes = new HashMap<>();

    private Participant(String args[]) throws InsufficientArgumentsException {
        //Bare-minimum number of arguments is 4, <cport> <pport> <timeout> <failurecond>
        if (args.length < 4) {
            throw new InsufficientArgumentsException(args);
        }
        coordinatorPort = Integer.parseInt(args[0]);
        listenPort = Integer.parseInt(args[1]);
        timeout = Integer.parseInt(args[2]);
        running = true;

        failureCond = switch (Integer.parseInt(args[3])) {
            case 0 -> failureCondition.SUCCESS;
            case 1 -> failureCondition.DURING;
            case 2 -> failureCondition.AFTER;
            default -> throw new IllegalArgumentException();
        };

        try {
            socket = new Socket("localhost", coordinatorPort);
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
                while (participantsHigherPort.stream()
                        .map(ParticipantClientConnection.class::cast)
                        .anyMatch(e -> !e.isConnected())) {
                    sleep(50);
                }
                connectionsMade = true;

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
                    roundNumber++;
                }

                if (roundNumber > 1 && connectionsMade) {
                    for (Thread thread : participantsLowerPort.keySet()) {
                        ParticipantServerConnection conn = (ParticipantServerConnection) thread;
                        conn.sendCombinedVotes();
                    }

                    for (Thread thread : participantsHigherPort) {
                        ParticipantClientConnection conn = (ParticipantClientConnection) thread;
                        conn.sendCombinedVotes();
                    }

                    sleep(500);
                }

                if (failureCond == failureCondition.AFTER) {
                    System.out.println("INITIATING FAILURE CONDITION 2");
                    closeConnections();
                    failed = true;
                }

                if (running)
                    establishWinner();

                roundNumber++;
            } catch (InterruptedException e) {
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
                    System.out.println("Participant connected to this participant");
                    Thread thread = new ParticipantServerConnection(socket, this);
                    participantsLowerPort.put(thread, socket);
                    thread.start();
                } else {
                    throw new ParticipantConfigurationException("Participant has same port as another participant: " + participant);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParticipantConfigurationException e) {
            System.err.println(e);
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

    private void establishWinner() {
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

        if (majorityOptions.size() == 1) {
            System.out.println("MAJORITY VOTE FOUND: " + majorityOptions.get(0));
            out.println("OUTCOME " + majorityOptions.get(0) +  " " + participantVotes.keySet());
            majorityVoteSent = true;
            running = false; //We're done now, so no further loops are required.
        } else {
            System.out.println("NO OVERALL MAJORITY, TIE BETWEEN: " + majorityOptions.toString());
            out.println("OUTCOME null " + participantVotes.keySet());
            majorityVoteSent = true;
            running = false; //We're done now, so no further loops are required.
        }
    }

    /**
     * Called by a connection instance to instigate another vote if a participant connection fails
     */
    protected void revote() {
        System.out.println("Initiating revote");
        participantsHigherPort.stream()
                .map(ParticipantClientConnection.class::cast)
                .forEach(ParticipantClientConnection::revote);
        participantsLowerPort.keySet().stream()
                .map(ParticipantServerConnection.class::cast)
                .forEach(ParticipantServerConnection::revote);
        running = true;
    }

    public int getRoundNumber() {
        return this.roundNumber;
    }

    private void sendJoin() {
        out.println("JOIN " + listenPort);
    }

    /**
     * Awaits the DETAILS [<port>] message from the Coordinator and stores other participants ports
     * @throws IOException
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

    private void awaitOptions() throws IOException {
        boolean optionsReceived = false;
        while (!optionsReceived) {
            String options = in.readLine();
            String[] optionsElem = options.split(" ");
            if (optionsElem[0].equals("VOTE_OPTIONS")) {
                optionsReceived = true;
                for (int i=1; i<optionsElem.length; i++) {
                    voteOptions.add(optionsElem[i]);
                }
                System.out.print("Vote Options received: " + voteOptions.toString());
            }
        }
        //Picks a random vote
        Collections.shuffle(voteOptions);
        chosenVote = voteOptions.get(0);
        System.out.print(", selected option: " + chosenVote);
        System.out.println();
    }

    public int getPort() {
        return this.listenPort;
    }

    public int getTimeout() {
        return this.timeout;
    }

    public boolean isMajorityVoteSent() {
        return this.majorityVoteSent;
    }

    public boolean hasFailed() {
        return this.failed;
    }

    public static void main(String args[]) {
        try {
            Participant participant = new Participant(args);
            participant.sendJoin();
            participant.awaitDetails();
            participant.awaitOptions();
            //Makes connections to other participants
            participant.start();

        } catch (InsufficientArgumentsException e) {
            System.err.println(e);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static class InsufficientArgumentsException extends Exception {
        String args[];

        public InsufficientArgumentsException (String args[]) {
            this.args = args;
        }

        public String toString() {
            return "Insufficient number of arguments: " + Arrays.toString(args);
        }
    }

    static class ParticipantConfigurationException extends Exception {
        String error;

        public ParticipantConfigurationException (String error) {
            this.error = error;
        }

        public String toString() {
            return "Invalid configuration of participant: " + error;
        }
    }

}
