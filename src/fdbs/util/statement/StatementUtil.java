package fdbs.util.statement;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class StatementUtil {

    /**
     * Reads a set of statements from a set of file.
     *
     * @param filePaths
     * @return A set of statements.
     * @throws FileNotFoundException
     * @throws IOException
     */
    public static String[] getStatementsFromFiles(String[] filePaths) throws IOException {
        List<String> statements = new ArrayList<>();

        for (String filePath : filePaths) {
            String[] statementsFromFile = getStatementsFromFile(filePath);
            statements.addAll(Arrays.asList(statementsFromFile));
        }
        return (String[]) statements.toArray(new String[statements.size() - 1]);
    }

    /**
     * Methode zum Holen der Statements aus einen InputStream
     *
     * @param inputStream Der Input-Strea m
     * @return Die Statements.
     * @throws IOException Weiterwerfen der IOException.
     */
    public static String[] getStatementsFromInputStream(InputStream inputStream) throws IOException {
        StringBuilder statements = new StringBuilder();

        try (InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                BufferedReader reader = new BufferedReader(inputStreamReader);) {
            String line;
            while ((line = reader.readLine()) != null) {
                statements.append(line);
            }
        }

        return statements.toString().split(";");
    }

    /**
     * Reads a set of statements from a file.
     *
     * @param filePath
     * @return A set of statements.
     * @throws FileNotFoundException
     * @throws IOException
     */
    private static String[] getStatementsFromFile(String filePath) throws IOException {
        StringBuilder statements = new StringBuilder();

        try (InputStream input = new FileInputStream(new File(filePath));
                InputStreamReader inputStreamReader = new InputStreamReader(input);
                BufferedReader reader = new BufferedReader(inputStreamReader);) {

            String line;
            while ((line = reader.readLine()) != null) {
                statements.append(line);
            }
        }

        return statements.toString().split(";");
    }
}
