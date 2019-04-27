import java.util.Arrays;

/**
 * @author Oscar van Leusen
 */
public class Participant {

    public static void main(String args[]) {
        try {
            Participant participant = new Participant(args);
        } catch (InsufficientArgumentsException e) {
            System.err.println(e);
        }
    }

    enum failureCondition { SUCCESS, DURING, AFTER }

    private final int communicatorPort;
    private final int listenPort;
    private final int timeout;
    private final failureCondition failureCond;

    public Participant(String args[]) throws InsufficientArgumentsException {
        //Bare-minimum number of arguments is 4, <cport> <pport> <timeout> <failurecond>
        if (args.length < 4) {
            throw new InsufficientArgumentsException(args);
        }
        communicatorPort = Integer.parseInt(args[0]);
        listenPort = Integer.parseInt(args[1]);
        timeout = Integer.parseInt(args[2]);

        failureCond = switch (Integer.parseInt(args[3])) {
            case 0 -> failureCondition.SUCCESS;
            case 1 -> failureCondition.DURING;
            case 2 -> failureCondition.AFTER;
            default -> throw new IllegalArgumentException();
        };

        System.out.println("Initialised Participant communicating to Coordinator on port " + communicatorPort + ", listening on port " + listenPort + " with failure condition: " + failureCond);

    }




    static class InsufficientArgumentsException extends Exception {
        String args[];

        public InsufficientArgumentsException (String args[]) {
            this.args = args;
        }

        public String toString() {
            return "Insufficient number of arguments: " + Arrays.toString(args);
        }
    }

}
