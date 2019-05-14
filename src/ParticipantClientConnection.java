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
            socket.setSoLinger(true,0);
            out = new PrintWriter(socket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            serverConn = true;
            System.out.println("Client participant " + participant.getPort() + " connected to Server participant: " + participantServerPort);
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
                if (!participant.receiveMessage(in.readLine(), participantServerPort)) {
                    closeConnection();
                }
            } catch (SocketTimeoutException e) {
                System.err.println("Connection to other Participant at port " + participantServerPort + " timed out.");
                closeConnection();
                if (!participant.isMajorityVoteSent() && !participant.hasFailed()) {
                    System.out.println("A connected participant failed before OUTCOME was sent. Revoting.");
                    participant.revote(Participant.revoteReason.FAILURE);
                }
            } catch (SocketException e) {
                System.err.println("Connection to other Participant at port " + participantServerPort + " closed.");
                closeConnection();
                if (!participant.isMajorityVoteSent() && !participant.hasFailed()) {
                    System.out.println("A connected participant failed before OUTCOME was sent. Revoting.");
                    participant.revote(Participant.revoteReason.FAILURE);
                }
            } catch (IOException e) {
                closeConnection();
                running = false;
                e.printStackTrace();
            } catch (Coordinator.UnknownMessageException e) {
                e.printStackTrace();
            }
        }
    }


    void sendVotes(String vote) {
        if (serverConn) {
            System.out.println("Sending to " + participantServerPort + ": VOTE " + participant.getPort() + " " + vote);
            out.println("VOTE " + participant.getPort() + " " + vote);
        }
    }

    void sendCombinedVotes(String votes) {
        if (serverConn) {
            System.out.println("Sending to " + participantServerPort + ": "  + votes);
            out.println(votes);
        }
    }

    int getParticipantPort() {
        return this.participant.getPort();
    }

    boolean isConnected() {
        return this.serverConn;
    }

    void setTimeout() throws SocketException {
        this.socket.setSoTimeout(participant.getTimeout());
    }

    /**
     * Used to simulate a participant failing
     */
    void closeConnection() {
        participant.connectionLost(this);
        serverConn = false;
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
