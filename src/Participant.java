import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

/**
 * @author Oscar van Leusen
 *
 */
public class Participant extends Thread {

    enum failureCondition { SUCCESS, DURING, AFTER }

    private ServerSocket serverSocket; //ServerSocket used to communicate to other participants on lower port numbers
    private List<Thread> participantsHigherPort = new ArrayList<>(); //Stores each connection to a participant on a higher port
    private HashMap<Thread, Socket> participantsLowerPort = new HashMap<>(); //Stores each connection to a participant on a lower port
    private Socket socket;
    private PrintWriter out;
    private BufferedReader in;

    private final int coordinatorPort;
    private final int listenPort;
    private final int timeout;
    private final failureCondition failureCond;
    private int[] otherParticipants;
    private boolean running; //Whether the thread/connection is running as normal
    private int roundNumber = 1;
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

        System.out.println("Opening socket to Coordinator");
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
        String receivedMessage;
        boolean connectionsMade = false;
        while (running) {
            try {

                if (!connectionsMade) {
                    //Opens ServerSocket used to communicate with participants on lower port numbers
                    serverSocket = new ServerSocket(listenPort);

                    for (int participant : otherParticipants) {
                        //If the participant we're connecting to is at a higher port number, that participant acts as a server.
                        //If the participant we're connecting to is at a lower port, this participant is the server.
                        if (participant > listenPort) {
                            Thread thread = new ParticipantClientConnection(this, participant);
                            participantsHigherPort.add(thread);
                            thread.start();
                        } else if (participant < listenPort) {
                            Socket socket = serverSocket.accept();
                            System.out.println("Participant connected to this participant");
                            Thread thread = new ParticipantServerConnection(socket, this);
                            participantsLowerPort.put(thread, socket);
                            thread.start();
                        } else {
                            throw new ParticipantConfigurationException("Participant has same port as another participant: " + participant);
                        }
                    }
                    connectionsMade = true;
                    sleep(200);
                }

                if (roundNumber == 1 && connectionsMade) {
                    for (Thread thread : participantsLowerPort.keySet()) {
                        ParticipantServerConnection conn = (ParticipantServerConnection) thread;
                        conn.sendVotes(chosenVote);
                    }

                    for (Thread thread : participantsHigherPort) {
                        ParticipantClientConnection conn = (ParticipantClientConnection) thread;
                        conn.sendVotes(chosenVote);
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
                }
                receivedMessage = in.readLine();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ParticipantConfigurationException e) {
                System.err.println(e);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
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
                System.out.println("Details of other participants received: " + Arrays.toString(otherParticipants));
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
                System.out.println("Vote Options received: " + voteOptions.toString());
            }
        }
        //Picks a random vote
        Collections.shuffle(voteOptions);
        chosenVote = voteOptions.get(0);
        System.out.println("Randomly generated vote choice: " + chosenVote);
    }

    public int getPort() {
        return this.listenPort;
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
