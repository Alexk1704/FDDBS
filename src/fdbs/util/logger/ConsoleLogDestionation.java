package fdbs.util.logger;

public class ConsoleLogDestionation implements ILogDestination {

    @Override
    public void logln(String log) {
        System.out.println(log);
    }

    @Override
    public void log(String log) {
        System.out.print(log);
    }
}
