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
    private int parts; //Number of participants to expect to JOIN (and expect an OUTCOME from)
    private final Set<String> options;
    private List<String> outcomes = new ArrayList<>();

    private Coordinator(String[] args) throws InsufficientArgumentsException {
        //Bare-minimum number of arguments is 4, <port> <parts> <option1> <option2>
        if (args.length < 4) {
            throw new InsufficientArgumentsException(args);
        }
        int listenPort = Integer.parseInt(args[0]);
        parts = Integer.parseInt(args[1]);
        options = new HashSet<>();

        options.addAll(Arrays.asList(args).subList(2, args.length));

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

    void outcomeReceived(String outcome) {
        outcomes.add(outcome); //OUTCOME <outcome> [<port>]
        checkOutcomes();
    }

    private boolean outcomePrinted = false;

    private void checkOutcomes() {
        //Wait for outcomes from all connected participants (parts is decremented if a participant connection fails)
        if (outcomes.size() >= parts && !outcomePrinted) {
            System.out.println("Received majority votes from " + outcomes.size() + " participants, out of " + parts + " functional participants.");
            //If all outcomes are the same, that outcome is conclusive.
            if (outcomes.stream().allMatch(outcomes.get(0)::equals)) {
                if (outcomes.get(0).equals("null")) {
                    System.out.println("Participants could not decide on a majority, there was a tie.");
                    outcomePrinted = true;
                    System.exit(0);
                } else {
                    System.out.println("Participants voted for option " + outcomes.get(0));
                    outcomePrinted = true;
                    System.exit(0);
                }
                //Close connections to participants as we have conclusive votes
                participantConnections.keySet().stream()
                        .map(CoordinatorConnHandler.class::cast)
                        .forEach(CoordinatorConnHandler::closeConnection);
            } else {
                System.out.println("Participants did not reach same outcome: " + outcomes.toString());
            }
        }
    }

    /**
     * Called by a Coordinator thread connected to a participant when a participant fails
     */
    void participantDisconnected(CoordinatorConnHandler connection) {
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

        StringBuilder voteOptions = new StringBuilder("VOTE_OPTIONS ");
        for (String opt : options) {
            voteOptions.append(opt).append(" ");
        }
        for (Thread thread : participantConnections.keySet()) {
            CoordinatorConnHandler participant = (CoordinatorConnHandler) thread;
            participant.sendMessage(voteOptions.toString());
        }

    }

    void participantJoined(CoordinatorConnHandler participant) {
        participantPorts.add(participant.getPort());
        participantsJoined++;

        if (participantsJoined >= parts) {
            sendDetailsVoteOptions();
        }
    }

    public static void main(String[] args) {
        try {
            Coordinator coordinator = new Coordinator(args);
            //Waits for all participants to connect
            coordinator.awaitConnections();
        } catch (InsufficientArgumentsException e) {
            e.printStackTrace();
        } catch (IOException e) {
            System.err.println("Unable to connect to participants");
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

    static class UnknownMessageException extends Exception {
        String message;
        UnknownMessageException(String message) { this.message = message; }

        public String toString() { return "Unknown message received from participant: " + message; }
    }
}
