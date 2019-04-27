import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Oscar van Leusen
 */
public class Coordinator {

    public static void main(String args[]) {
        try {
            Coordinator coordinator = new Coordinator(args);
        } catch (InsufficientArgumentsException e) {
            System.err.println(e);
        }
    }

    private final int listenPort;
    private final int parts;
    private final Set<String> options;

    public Coordinator(String args[]) throws InsufficientArgumentsException {
        //Bare-minimum number of arguments is 4, <port> <parts> <option1> <option2>
        if (args.length < 4) {
            throw new InsufficientArgumentsException(args);
        }
        listenPort = Integer.parseInt(args[0]);
        parts = Integer.parseInt(args[1]);
        options = new HashSet<>();
        for (int i=2; i<args.length; i++) {
            options.add(args[i]);
        }
        System.out.println("Initialised Coordinator listening on port " + listenPort + ", expecting " + parts + " participants, with options " + options.toString());
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
