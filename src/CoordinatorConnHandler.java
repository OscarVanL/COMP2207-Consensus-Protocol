import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
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
     * @param socket Socket for connection to participant
     * @param coordinator Coordinator
     * @throws IOException Throw exception to Coordinator if socket issue occurs
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
                if (receivedMessage == null) {
                    System.err.println("Connection to participant at port " + participantPort + " closed unexpectedly.");
                    coordinator.participantDisconnected(this);
                    closeConnection();
                    running = false;
                } else {
                    String[] messageParts = receivedMessage.split(" ");
                    switch (messageParts[0]) {
                        //Participant telling Coordinator its port number/identifier
                        case "JOIN":
                            participantPort = Integer.parseInt(receivedMessage.replaceAll("[^0-9]", ""));
                            coordinator.participantJoined(this);
                            break;
                        case "OUTCOME":
                            coordinator.outcomeReceived(messageParts[1]);
                            break;
                        default:
                            throw new Coordinator.UnknownMessageException(receivedMessage);
                    }
                }

            } catch (SocketTimeoutException e) {
                this.running = false;
                System.err.println("Connection to participant at port " + participantPort + " timed out.");
                coordinator.participantDisconnected(this);
            } catch (SocketException e) {
                this.running = false;
                System.err.println("Connection to participant at port " + participantPort + " closed.");
                coordinator.participantDisconnected(this);
            } catch (Coordinator.UnknownMessageException e) {
                e.printStackTrace();
            } catch (IOException e) {
                running = false;
                e.printStackTrace();
            }
        }
    }

    /**
     * Sends message DETAILS [<port>] to the Participant
     * @param participantPorts Ports of all participants connected to Coordinator
     */
    void sendDetails(List<Integer> participantPorts) {
        StringBuilder message = new StringBuilder("DETAILS ");
        for (Integer port : participantPorts) {
            if (port != participantPort) {
                message.append(port).append(" ");
            }
        }
        out.println(message);
    }

    /**
     * Sends a custom message to the connected Participant
     * @param message Message to send
     */
    void sendMessage(String message) {
        out.println(message);
    }

    int getPort() {
        return this.participantPort;
    }

    void closeConnection() {
        try {
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
