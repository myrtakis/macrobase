package alexp.macrobase.streaming;


// https://www.baeldung.com/a-guide-to-java-sockets

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class StreamGenerator {

    private static BufferedReader br;
    private static boolean generatorIsSetted = false;
    private static int maxBound = 10;

    public static void init(){
        generatorIsSetted = false;
    }

    // P/P Streaming Generator e.g. P1
    public static String fetch(String filePath) throws IOException {
        if (!generatorIsSetted) {
            generatorIsSetted = true;
            br = new BufferedReader(new FileReader(new File(filePath)));
            br.readLine();
        }

        String line = br.readLine();

        if (line == null) {
            return "";
        } else {
            return line;
        }
    }

    // Time (Simulation) Streaming Generator e.g. [[P1, P2], [P3, P4, P5], [P6]] ~ 1 SEC of data points
    // Comment: this method is currently not in use.
    public static List<String> fetch(String filePath, int seconds) throws IOException {
        if (!generatorIsSetted) {
            generatorIsSetted = true;
            br = new BufferedReader(new FileReader(new File(filePath)));
            br.readLine();
        }
        int random = new Random().nextInt(maxBound * seconds) + 1;
        List<String> lineChunk = new ArrayList<String>();
        while (random-- != 0) {
            String line = br.readLine();
            if (line == null)
                break;
            lineChunk.add(line);
        }
        return lineChunk;
    }

}
