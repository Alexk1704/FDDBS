package fdbs.sql;

import fdbs.util.logger.Logger;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * PropertyManager-Class for managing all properties which are needed to get a
 * connection to all databases.
 */
public class PropertyManager {

    private static PropertyManager instance;
    private Properties prop = new Properties();

    /**
     * The constructor for an property manager.
     */
    public PropertyManager() throws FedException {
        //try close stream automatically
        try (InputStream input = this.getClass().getClassLoader().getResourceAsStream("resources/config.properties")) {
            prop.load(input);
            input.close();
        } catch (IOException ex) {
            throw new FedException(String.format("%s: An error occurred while reading federated middleware properties.%n%s", this.getClass().getSimpleName(), ex.getMessage()), ex);
        }
    }

    /**
     * @return returns thread safe instance of the singleton PropertyManager.
     */
    public static synchronized PropertyManager getInstance() throws FedException {
        if (instance == null) {
            instance = new PropertyManager();
        }
        return instance;
    }

    /**
     * @return returns the LogLevel from the properties-file.
     */
    public String getLogLevel() {
        return prop.getProperty("LogLevel");
    }

    /**
     * Gets the database connection string by the database id.
     *
     * @param dbId database id.
     * @return Returns the connection string or null.
     */
    public String getConnectionStringByID(int dbId) {
        return prop.getProperty("Database" + dbId);
    }
}
