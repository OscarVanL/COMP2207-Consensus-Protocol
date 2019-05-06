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
 * This object represents a participant 'client' connection instance, that connects to a 'server' participant
 */
public class ParticipantClientConnection extends Thread {
    private Participant participant;
    private int participantServerPort;
    private boolean serverConn = false;
    private volatile boolean running = true;

    private Socket socket;
    private PrintWriter out;
    private BufferedReader in;

    ParticipantClientConnection(Participant participant, int participantServerPort) {
        this.participant = participant;
        this.participantServerPort = participantServerPort;

        try {
            socket = new Socket("localhost", participantServerPort);
            socket.setSoTimeout(participant.getTimeout());
            out = new PrintWriter(socket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            serverConn = true;
            System.out.println("Participant connected to participant listening at: " + participantServerPort);
        } catch (IOException e) {
            running = false;
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        String receivedMessage;
        while (running && serverConn) {
            //Waits for a message from the Server
            try {
                receivedMessage = in.readLine();
                if (receivedMessage == null) {
                    System.err.println("Connected participant connection closed unexpectedly");
                    closeConnection();
                } else {
                    String[] messageParts = receivedMessage.split(" ");
                    switch (messageParts[0]) {
                        case "VOTE":
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
                System.err.println("Connection to other Participant at port " + participantServerPort + " timed out.");
                this.serverConn = false;
                if (!participant.isMajorityVoteSent() && !participant.hasFailed()) {
                    System.out.println("A connected participant failed before OUTCOME was sent. Revoting.");
                    participant.revote();
                }
                this.running = false;
            } catch (SocketException e) {
                System.err.println("Connection to other Participant at port " + participantServerPort + " closed.");
                this.serverConn = false;
                if (!participant.isMajorityVoteSent() && !participant.hasFailed()) {
                    System.out.println("A connected participant failed before OUTCOME was sent. Revoting.");
                    participant.revote();
                }
                this.running = false;
            } catch (IOException e) {
                running = false;
                e.printStackTrace();
            }
        }
    }


    public void sendVotes(String vote) {
        if (serverConn) {
            System.out.println("Sending: VOTE " + participant.getPort() + " " + vote);
            out.println("VOTE " + participant.getPort() + " " + vote);
        }
    }

    public void sendCombinedVotes() {
        if (serverConn) {
            String voteText = "VOTE ";
            synchronized (participant.participantVotes) {
                for (Map.Entry<Integer, String> vote : participant.participantVotes.entrySet()) {
                    voteText += vote.getKey() + " " + vote.getValue() + " ";
                }
            }
            System.out.println("Sending Vote: " + voteText);
            //this.running = false;
            out.println(voteText);
        }
    }

    public void revote() {
        if (serverConn) {
            this.running = true;
        }
    }

    public boolean isConnected() {
        return this.serverConn;
    }

    /**
     * Used to simulate a participant failing
     */
    public void closeConnection() throws IOException {
        serverConn = false;
        running = false;
        socket.close();
        in.close();
        out.close();
    }
}
