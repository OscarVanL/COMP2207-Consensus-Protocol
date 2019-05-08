import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;

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
        while (running) {
            try {
                if (!participant.receiveMessage(in.readLine())) {
                    closeConnection();
                }
            } catch (SocketTimeoutException e) {
                System.err.println("Connection to other Participant timed out.");
                closeConnection();
                if (!participant.isMajorityVoteSent() && !participant.hasFailed()) {
                    System.out.println("A connected participant failed before OUTCOME was sent. Triggering revote.");
                    participant.revote(Participant.revoteReason.FAILURE);
                }
            } catch (SocketException e) {
                System.err.println("Connection to other Participant closed");
                closeConnection();
                if (!participant.isMajorityVoteSent() && !participant.hasFailed()) {
                    System.out.println("A connected participant failed before OUTCOME was sent. Triggering revote.");
                    participant.revote(Participant.revoteReason.FAILURE);
                }
            } catch (IOException e) {
                closeConnection();
                e.printStackTrace();
            }
        }
    }

    void sendVotes(String vote) {
        if (!connectionLost) {
            System.out.println("Sending: VOTE " + participant.getPort() + " " + vote);
            out.println("VOTE " + participant.getPort() + " " + vote);
        }
    }

    void sendCombinedVotes(String votes) {
        if (!connectionLost) {
            System.out.println("Sending: " + votes);
            out.println(votes);
        }
    }

    void revote() {
        if (!connectionLost) {
            this.running = true;
        }
    }

    /**
     * Used to simulate a participant failing
     */
    void closeConnection() {
        participant.connectionLost(this);
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
