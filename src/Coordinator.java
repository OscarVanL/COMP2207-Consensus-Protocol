import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

/**
 * @author Oscar van Leusen
 */
public class Coordinator {
    private HashMap<Thread, Socket> participantConnections = new HashMap<>();
    private List<Integer> participantPorts = new ArrayList<>();
    private int participantsJoined = 0;

    private ServerSocket serverSocket;
    private final int listenPort;
    private int parts; //Number of participants to expect
    private final Set<String> options;
    private List<String> outcomes = new ArrayList<>();

    private Coordinator(String[] args) throws InsufficientArgumentsException {
        //Bare-minimum number of arguments is 4, <port> <parts> <option1> <option2>
        if (args.length < 4) {
            throw new InsufficientArgumentsException(args);
        }
        listenPort = Integer.parseInt(args[0]);
        parts = Integer.parseInt(args[1]);
        options = new HashSet<>();

        for (int i=2; i<args.length; i++) {
            options.add(args[i]);
        }

        try {
            serverSocket = new ServerSocket(listenPort);
            serverSocket.setSoTimeout(10000); //Gives 10 seconds maximum for each participant to join before timing out.
            System.out.println("Initialised Coordinator listening on " + listenPort + ", expecting " + parts + " participants, options: " + options.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void awaitConnections() throws IOException {
        Socket socket;
        long startTime = System.currentTimeMillis();
        while (participantConnections.size() < parts) {
            System.out.println("Waiting for client connection");
            socket = serverSocket.accept();
            System.out.println("A participant has connected to the server");

            //Creates a new thread for the participant, so this thread is able to continue to accept new connections.
            Thread thread = new CoordinatorConnHandler(socket, this);
            participantConnections.put(thread, socket);
            thread.start();
        }
        System.out.println("All participants have made a connection to the server");

    }

    public void outcomeReceived(String outcome) {
        outcomes.add(outcome); //OUTCOME <outcome> [<port>]
        checkOutcomes();
    }

    private boolean outcomePrinted = false;

    private void checkOutcomes() {
        //Wait for outcomes from all connected participants (parts is decremented if a participant connection fails)
        if (outcomes.size() >= parts && !outcomePrinted) {
            //If all outcomes are the same, that outcome is conclusive.
            if (outcomes.stream().allMatch(outcomes.get(0)::equals)) {
                if (outcomes.get(0).equals("null")) {
                    System.out.println("Participants could not decide on a majority, there was a tie.");
                    outcomePrinted = true;
                } else {
                    System.out.println("Participants voted for option " + outcomes.get(0));
                    outcomePrinted = true;
                }
                //Close connections to participants as we have conclusive votes
                participantConnections.keySet().stream()
                        .map(CoordinatorConnHandler.class::cast)
                        .forEach(e -> e.closeConnection());
            } else {
                System.out.println("Participants did not reach same outcome: " + outcomes.toString());
            }
        }
    }

    /**
     * Called by a Coordinator thread connected to a participant when a participant fails
     */
    protected void participantDisconnected(CoordinatorConnHandler connection) {
        participantPorts.remove((Integer) connection.getPort());
        participantsJoined--;
        parts--;
        participantConnections.remove(connection);

        checkOutcomes();
    }

    private void sendDetailsVoteOptions() {

        for (Thread thread : participantConnections.keySet()) {
            CoordinatorConnHandler participant = (CoordinatorConnHandler) thread;
            participant.sendDetails(participantPorts);
        }

        String voteOptions = "VOTE_OPTIONS ";
        for (String opt : options) {
            voteOptions += opt + " ";
        }
        for (Thread thread : participantConnections.keySet()) {
            CoordinatorConnHandler participant = (CoordinatorConnHandler) thread;
            participant.sendMessage(voteOptions);
        }

    }

    public void participantJoined(CoordinatorConnHandler participant) {
        participantPorts.add(participant.getPort());
        participantsJoined++;

        if (participantsJoined >= parts) {
            sendDetailsVoteOptions();
        }
    }

    public static void main(String args[]) {
        try {
            Coordinator coordinator = new Coordinator(args);
            //Waits for all participants to connect
            coordinator.awaitConnections();
        } catch (InsufficientArgumentsException e) {
            System.err.println(e);
        } catch (IOException e) {
            System.err.println("Unable to connect to participants");
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

    static class UnknownMessageException extends Exception {
        String message;
        public UnknownMessageException (String message) { this.message = message; }

        public String toString() { return "Unknown message received from participant: " + message; }
    }
}
