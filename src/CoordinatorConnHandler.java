import java.io.*;
import java.net.Socket;
import java.util.List;

/**
 * @author Oscar van Leusen
 */
public class CoordinatorConnHandler extends Thread {
    private final Socket socket;
    private Coordinator coordinator;
    private BufferedReader in;
    private PrintWriter out;
    private int participantPort;
    private boolean running; //Whether the thread/connection is running as normal

    /**
     * A class for managing a Coordinator connection to a participant
     * @param socket
     * @param coordinator
     * @throws IOException
     */
    CoordinatorConnHandler(Socket socket, Coordinator coordinator) throws IOException {
        this.socket = socket;
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
                String[] messageParts = receivedMessage.split(" ");
                switch (messageParts[0]) {
                    //Participant telling Coordinator its port number/identifier
                    case "JOIN":
                        participantPort = Integer.parseInt(receivedMessage.replaceAll("[^0-9]", ""));
                        coordinator.participantJoined(this);
                        break;
                    case "OUTCOME":
                        System.out.print("Outcome received from participant: " + participantPort + ", option " + messageParts[1] + ", votes from ");
                        for (int i=2; i<messageParts.length; i++) {
                            System.out.print(messageParts[i] + " ");
                        }
                        System.out.println();
                        break;
                    default:
                        throw new Coordinator.UnknownMessageException(receivedMessage);
                }


            } catch (IOException e) {
                e.printStackTrace();
            } catch (Coordinator.UnknownMessageException e) {
                System.err.println(e);
            }
        }
    }

    /**
     * Sends message DETAILS [<port>] to the Participant
     * @param participantPorts
     */
    public void sendDetails(int participantPorts[]) {
        String message = "DETAILS ";
        for (int i=0; i<participantPorts.length; i++) {
            if (participantPorts[i] != participantPort) {
                message += participantPorts[i] + " ";
            }
        }
        out.println(message);
    }

    /**
     * Sends a custom message to the connected Participant
     * @param message Message to send
     */
    public void sendMessage(String message) {
        out.println(message);
    }

    public int getPort() {
        return this.participantPort;
    }
}
