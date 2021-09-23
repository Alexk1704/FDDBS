package fdbs.util.logger;

import fdbs.sql.FedException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.ListIterator;

import fdbs.sql.PropertyManager;

public class Logger {

    private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    //Get the LogLevel out of the properties-file
    private static LogLevel loglevel = LogLevel.INFO;
    private static boolean isEnabled = true;
    private static final ArrayList<ILogDestination> logDestinations = new ArrayList<ILogDestination>();
    private static Logger instance;
    private static ILogDestination fedprot = new FileLogDestination("FederatedDatabases");

    private Logger() throws FedException {
        addLogDestination(new ConsoleLogDestionation());
        addLogDestination(fedprot);
        loglevel = LogLevel.valueOf(PropertyManager.getInstance().getLogLevel());
    }

    public static enum LogLevel {
        TRACE, DEBUG, INFO, WARN, ERROR
    }

    public static synchronized Logger getLogger() throws FedException {
        if (instance == null) {
            instance = new Logger();
        }
        return instance;
    }

    public static boolean isEnabled() {
        return isEnabled;
    }

    public static void enableLogging() {
        isEnabled = true;
    }

    public static void disableLogging() {
        isEnabled = false;
    }

    public static LogLevel getLogLevel() {
        return loglevel;
    }

    public static void setLogLevel(LogLevel level) {
        loglevel = level;
    }

    public static void addLogDestination(ILogDestination dest) {
        if (!logDestinations.contains(dest)) {
            logDestinations.add(dest);
        }
    }

    public static void removeLogDestination(ILogDestination dest) {
        if (logDestinations.contains(dest)) {
            logDestinations.remove(dest);
        }
    }

    public static void error(String s) {
        if (loglevel.ordinal() <= LogLevel.ERROR.ordinal()) {
            writeLogln(String.format("[Error] %s", s));
        }
    }

    public static void error(String s, Throwable ex) {
        if (loglevel.ordinal() <= LogLevel.ERROR.ordinal()) {
            try (StringWriter sw = new StringWriter(); PrintWriter pw = new PrintWriter(sw)) {
                ex.printStackTrace(pw);
                writeLogln(String.format("[Error] %s %n %s %n %s", s, ex.getMessage(), sw.toString()));
            } catch (IOException ex1) {
                writeLogln(String.format("[Error] %s %n %s", s, ex1.getMessage()));
            }
        }
    }

    public static void warnln(String s) {
        if (loglevel.ordinal() <= LogLevel.WARN.ordinal()) {
            writeLogln(String.format("[WARNING] %s", s));
        }
    }

    public static void warn(String s) {
        if (loglevel.ordinal() <= LogLevel.WARN.ordinal()) {
            writeLog(String.format("%s", s));
        }
    }

    public static void infoln(String s) {
        if (loglevel.ordinal() <= LogLevel.INFO.ordinal()) {
            writeLogln(String.format("[INFO] %s", s));
        }
    }

    public static void info(String s) {
        if (loglevel.ordinal() <= LogLevel.INFO.ordinal()) {
            writeLog(String.format("%s", s));
        }
    }

    public static void debugln(String s) {
        if (loglevel.ordinal() <= LogLevel.DEBUG.ordinal()) {
            writeLogln(String.format("[DEBUG] %s", s));
        }
    }

    public static void debug(String s) {
        if (loglevel.ordinal() <= LogLevel.DEBUG.ordinal()) {
            writeLog(String.format("%s", s));
        }
    }

    public static void traceln(String s) {
        if (loglevel.ordinal() <= LogLevel.TRACE.ordinal()) {
            writeLogln(String.format("[Trace] %s", s));
        }
    }

    public static void trace(String s) {
        if (loglevel.ordinal() <= LogLevel.TRACE.ordinal()) {
            writeLog(String.format("%s", s));
        }
    }

    /**
     * Logs always independent of curent log level
     *
     * @param s
     */
    public static void logln(String s) {
        writeLogln(s);
    }

    public static void log(String s) {
        writeLog(s);
    }

    private static void writeLogln(String log) {
        if (isEnabled && logDestinations.size() > 0) {
            ListIterator<ILogDestination> iterator = logDestinations.listIterator();
            while (iterator.hasNext()) {
                iterator.next().logln(String.format("[%s]%s", dateFormat.format(new Date()), log));
            }
        }
    }

    private static void writeLog(String log) {
        if (isEnabled && logDestinations.size() > 0) {
            ListIterator<ILogDestination> iterator = logDestinations.listIterator();
            while (iterator.hasNext()) {
                iterator.next().log(log);
            }
        }
    }
}
