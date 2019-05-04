import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

/**
 * @author Oscar van Leusen
 */
public class Coordinator extends Thread {
    private HashMap<Thread, Socket> participantConnections = new HashMap<>();
    private int participantPorts[];
    private int participantsJoined = 0;

    private ServerSocket serverSocket;
    private final int listenPort;
    private final int parts; //Number of participants to expect
    private final Set<String> options;

    private Coordinator(String args[]) throws InsufficientArgumentsException {
        //Bare-minimum number of arguments is 4, <port> <parts> <option1> <option2>
        if (args.length < 4) {
            throw new InsufficientArgumentsException(args);
        }
        listenPort = Integer.parseInt(args[0]);
        parts = Integer.parseInt(args[1]);
        participantPorts = new int[parts];
        options = new HashSet<>();

        for (int i=2; i<args.length; i++) {
            options.add(args[i]);
        }

        try {
            serverSocket = new ServerSocket(listenPort);
            System.out.println("Initialised Coordinator listening on " + listenPort + ", expecting " + parts + " participants, options: " + options.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Handles operations that would otherwise cause the application to hang
     */
    @Override
    public void run() {
        while (true) {
            //does nothing atm
        }
    }

    private void awaitConnections() {
        Socket socket;
        while (participantConnections.size() < parts) {
            System.out.println("Waiting for client connection");
            try {
                socket = serverSocket.accept();
                System.out.println("A participant has connected to the server");

                //Creates a new thread for the participant, so this thread is able to continue to accept new connections.
                Thread thread = new CoordinatorConnHandler(socket, this);
                participantConnections.put(thread, socket);
                thread.start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.out.println("All participants have made a connection to the server");

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
        participantPorts[participantsJoined] = participant.getPort();
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
