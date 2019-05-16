import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.*;

import static java.lang.Thread.sleep;

/**
 * @author Oscar van Leusen
 */
public class Coordinator {
    private final HashMap<Thread, Socket> participantConnections = new HashMap<>();
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
            System.out.println("Initialised Coordinator listening on " + listenPort + ", expecting " + parts + " participants, options: " + options.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void awaitConnections() throws IOException {
        Socket socket;
        while (participantConnections.size() < parts) {
            socket = serverSocket.accept();
            socket.setSoLinger(true,0);
            System.out.println("A participant has connected to the coordinator");

            //Creates a new thread for the participant, so this thread is able to continue to accept new connections.
            Thread thread = new CoordinatorConnHandler(socket, this);
            synchronized (participantConnections) {
                participantConnections.put(thread, socket);
            }

            thread.start();
        }
        System.out.println("All participants have made a connection to the coordinator");

    }

    private void outcomeReceived(String outcome) {
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
                    System.out.println("Participants could not decide on a majority or there was a tie.");
                    //Restart voting for connected participants with tie values
                    try {
                        sleep(2000);
                        synchronized (participantConnections) {
                            participantConnections.keySet().stream()
                                    .map(CoordinatorConnHandler.class::cast)
                                    .forEach(e -> e.sendMessage("RESTART"));
                        }
                        outcomes.clear();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }


                } else {
                    System.out.println("Participants voted for option " + outcomes.get(0));
                    outcomePrinted = true;
                    //Close connections to participants as we have conclusive votes
                    synchronized (participantConnections) {
                        participantConnections.keySet().stream()
                                .map(CoordinatorConnHandler.class::cast)
                                .forEach(CoordinatorConnHandler::closeConnection);
                    }
                    System.exit(0);
                }

            } else {
                System.out.println("Participants did not reach same outcome: " + outcomes.toString());
            }
        }
    }

    /**
     * Called by a Coordinator thread connected to a participant when a participant fails
     */
    private void participantDisconnected(CoordinatorConnHandler connection) {
        participantPorts.remove((Integer) connection.getPort());
        participantsJoined--;
        parts--;
        //Don't remove a connection while something else is referencing it
        synchronized (participantConnections) {
            participantConnections.remove(connection);
        }


        checkOutcomes();
    }

    private void sendDetailsVoteOptions() {
        synchronized (participantConnections) {
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
    }

    private void participantJoined(CoordinatorConnHandler participant) {
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

    public class CoordinatorConnHandler extends Thread {
        private final Socket socket;
        private Coordinator coordinator;
        private BufferedReader in;
        private PrintWriter out;
        private int participantPort;
        private boolean running; //Whether the thread/connection is running as normal

        /**
         * A class for managing a Coordinator connection to a participant
         * @param socket Socket for connection to participant
         * @param coordinator Coordinator
         * @throws IOException Throw exception to Coordinator if socket issue occurs
         */
        CoordinatorConnHandler(Socket socket, Coordinator coordinator) throws IOException {
            this.socket = socket;
            socket.setSoLinger(true,0);
            this.coordinator = coordinator;
            this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            this.out = new PrintWriter(socket.getOutputStream(), true);
            this.running = true;
        }

        @Override
        public void run() {
            String receivedMessage;
            while (running) {
                try {
                    receivedMessage = in.readLine();
                    if (receivedMessage == null) {
                        System.out.println("Connection to participant at port " + participantPort + " closed unexpectedly.");
                        coordinator.participantDisconnected(this);
                        closeConnection();
                        running = false;
                    } else {
                        String[] messageParts = receivedMessage.split(" ");
                        switch (messageParts[0]) {
                            //Participant telling Coordinator its port number/identifier
                            case "JOIN":
                                participantPort = Integer.parseInt(receivedMessage.replaceAll("[^0-9]", ""));
                                coordinator.participantJoined(this);
                                break;
                            case "OUTCOME":
                                System.out.println("Received outcome from: " + participantPort + ": " + messageParts[1]);
                                coordinator.outcomeReceived(messageParts[1]);
                                break;
                            default:
                                throw new Coordinator.UnknownMessageException(receivedMessage);
                        }
                    }

                } catch (SocketTimeoutException e) {
                    this.running = false;
                    System.out.println("Connection to participant at port " + participantPort + " timed out.");
                    coordinator.participantDisconnected(this);
                } catch (SocketException e) {
                    this.running = false;
                    System.out.println("Connection to participant at port " + participantPort + " closed.");
                    coordinator.participantDisconnected(this);
                } catch (Coordinator.UnknownMessageException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    running = false;
                    e.printStackTrace();
                }
            }
        }

        /**
         * Sends message DETAILS [<port>] to the Participant
         * @param participantPorts Ports of all participants connected to Coordinator
         */
        void sendDetails(List<Integer> participantPorts) {
            StringBuilder message = new StringBuilder("DETAILS ");
            for (Integer port : participantPorts) {
                if (port != participantPort) {
                    message.append(port).append(" ");
                }
            }
            out.println(message);
        }

        /**
         * Sends a custom message to the connected Participant
         * @param message Message to send
         */
        void sendMessage(String message) {
            out.println(message);
        }

        int getPort() {
            return this.participantPort;
        }

        void closeConnection() {
            try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

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
