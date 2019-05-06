import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;

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
        while (running && serverConn) {
            //Waits for a message from the Server
            try {
                if (!participant.receiveMessage(in)) {
                    closeConnection();
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


    void sendVotes(String vote) {
        if (serverConn) {
            System.out.println("Sending: VOTE " + participant.getPort() + " " + vote);
            out.println("VOTE " + participant.getPort() + " " + vote);
        }
    }

    void sendCombinedVotes(String votes) {
        if (serverConn) {
            out.println(votes);
        }
    }

    void revote() {
        if (serverConn) {
            this.running = true;
        }
    }

    boolean isConnected() {
        return this.serverConn;
    }

    /**
     * Used to simulate a participant failing
     */
    void closeConnection() throws IOException {
        serverConn = false;
        running = false;
        socket.close();
        in.close();
        out.close();
    }
}
