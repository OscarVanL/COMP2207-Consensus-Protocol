import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Oscar van Leusen
 * This object represents a participant 'server' connection instance to one other participant
 *
 */
public class ParticipantServerConnection extends Thread {
    private Socket socket;
    private Participant participant;
    private PrintWriter out;
    private BufferedReader in;
    private boolean connectionLost = false;
    private volatile boolean running = true;

    ParticipantServerConnection(Socket socket, Participant participant) {
        this.socket = socket;
        this.participant = participant;

        try {
            this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            this.out = new PrintWriter(socket.getOutputStream(), true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        String receivedMessage;
        while (running) {
            try {
                receivedMessage = in.readLine();
                if (receivedMessage == null) {
                    System.err.println("Connected participant connection closed unexpectedly");
                    closeConnection();
                } else {
                    String[] messageParts = receivedMessage.split(" ");
                    switch (messageParts[0]) {
                        case "VOTE":
                            //If message has 3 parts, eg: VOTE 12345 A, then it is a vote from round 1
                            //Otherwise it's a vote from a later round
                            if (messageParts.length == 3 && participant.getRoundNumber() == 1) {
                                System.out.println("Vote received in round 1: " + receivedMessage);
                                synchronized (participant.participantVotes) {
                                    participant.participantVotes.put(Integer.parseInt(messageParts[1]), messageParts[2]);
                                }

                            } else if (participant.getRoundNumber() > 1) {
                                System.out.println("Votes received round " + participant.getRoundNumber() + ": " + receivedMessage);
                                for (int i=1; i<messageParts.length; i+=2) {
                                    synchronized (participant.participantVotes) {
                                        participant.participantVotes.put(Integer.parseInt(messageParts[i]), messageParts[i+1]);
                                    }

                                }
                            }

                    }
                }
            } catch (SocketTimeoutException e) {
                System.err.println("Connection to other Participant timed out.");
                this.connectionLost = true;
                this.running = false;
                if (!participant.isMajorityVoteSent() && !participant.hasFailed()) {
                    System.out.println("A connected participant failed before OUTCOME was sent. Revoting.");
                    participant.revote();
                }
            } catch (SocketException e) {
                System.err.println("Connection to other Participant closed");
                this.connectionLost = true;
                this.running = false;
                if (!participant.isMajorityVoteSent() && !participant.hasFailed()) {
                    System.out.println("A connected participant failed before OUTCOME was sent. Revoting.");
                    participant.revote();
                }
            } catch (IOException e) {
                running = false;
                e.printStackTrace();
            }
        }
    }

    public void sendVotes(String vote) {
        if (!connectionLost) {
            System.out.println("Sending: VOTE " + participant.getPort() + " " + vote);
            out.println("VOTE " + participant.getPort() + " " + vote);
        }
    }

    public void sendCombinedVotes() {
        if (!connectionLost) {
            String voteText = "VOTE ";

            synchronized (participant.participantVotes) {
                for (Map.Entry<Integer, String> vote : participant.participantVotes.entrySet()) {
                    voteText += vote.getKey() + " " + vote.getValue() + " ";
                }
            }
            System.out.println("Sending Vote: " + voteText);
            this.running = false;
            out.println(voteText);
        }
    }

    public void revote() {
        if (!connectionLost) {
            this.running = true;
        }
    }

    /**
     * Used to simulate a participant failing
     */
    public void closeConnection() throws IOException {
        connectionLost = true;
        running = false;
        socket.close();
        in.close();
        out.close();
    }
}
