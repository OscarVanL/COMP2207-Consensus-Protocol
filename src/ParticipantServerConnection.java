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
                if (!participant.receiveMessage(in)) {
                    closeConnection();
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

    void sendVotes(String vote) {
        if (!connectionLost) {
            System.out.println("Sending: VOTE " + participant.getPort() + " " + vote);
            out.println("VOTE " + participant.getPort() + " " + vote);
        }
    }

    void sendCombinedVotes(String votes) {
        if (!connectionLost) {
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
    void closeConnection() throws IOException {
        connectionLost = true;
        running = false;
        socket.close();
        in.close();
        out.close();
    }
}
