import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

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
        while (true) {
            try {
                receivedMessage = in.readLine();
                String[] messageParts = receivedMessage.split(" ");
                switch (messageParts[0]) {
                    case "VOTE":
                        System.out.println("Vote received: " + receivedMessage);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void sendVotes(String vote) {
        out.println("VOTE " + participant.getPort() + " " + vote);
    }
}
