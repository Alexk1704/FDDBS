package fdbs.util.logger;

import fdbs.sql.FedException;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.PrintWriter;
import java.io.IOException;

public class FileLogDestination implements ILogDestination {

    private final String fileName;
    private boolean isFirstLog = true;

    public FileLogDestination(String fileName) {
        this.fileName = fileName;
    }

    @Override
    public void logln(String log) {
        String out = log;

        if (!isFirstLog) {
            out = "\n" + out;
        }

        this.writeFile(out);
        this.isFirstLog = false;
    }

    @Override
    public void log(String log) {
        this.writeFile(log);
    }

    private void writeFile(String log) {
        try (FileWriter fw = new FileWriter(fileName + ".log", true);
                BufferedWriter bw = new BufferedWriter(fw);
                PrintWriter out = new PrintWriter(bw)) {
            out.print(log);
        } catch (IOException ex) {
            System.out.println(String.format("%s: An error occurred while creating log file. %s", this.getClass().getSimpleName(), ex.getMessage()));
        }
    }
}
