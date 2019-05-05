import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
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

    private Socket socket;
    private PrintWriter out;
    private BufferedReader in;

    ParticipantClientConnection(Participant participant, int participantServerPort) {
        this.participant = participant;
        this.participantServerPort = participantServerPort;
    }

    @Override
    public void run() {
        String receivedMessage;
        while (true) {
            try {
                //Establishes a connection with server
                if (!serverConn) {
                    socket = new Socket("localhost", participantServerPort);
                    out = new PrintWriter(socket.getOutputStream(), true);
                    in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    serverConn = true;
                    System.out.println("Participant connected to participant listening at: " + participantServerPort);
                }

                //Waits for a message from the Server
                receivedMessage = in.readLine();
                String[] messageParts = receivedMessage.split(" ");
                switch (messageParts[0]) {
                    case "VOTE":
                        if (messageParts.length == 3 && participant.getRoundNumber() == 1) {
                            System.out.println("Vote received in round 1: " + receivedMessage);
                            participant.participantVotes.put(Integer.parseInt(messageParts[1]), messageParts[2]);
                        } else if (participant.getRoundNumber() > 1) {
                            System.out.println("Votes received round " + participant.getRoundNumber() + ": " + receivedMessage);
                            //for (int i=1; i<messageParts.length; i+=2) {
                            //    participantVotes.clear();
                            //
                            //}
                        }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }


    public void sendVotes(String vote) {
        out.println("VOTE " + participant.getPort() + " " + vote);
    }

    public void sendCombinedVotes() {
        String voteText = "VOTE ";
        for (Map.Entry<Integer, String> vote : participant.participantVotes.entrySet()) {
            voteText += vote.getKey() + " " + vote.getValue() + " ";
        }

        System.out.println("Sending Vote: " + voteText);

        out.println(voteText);
    }
}
