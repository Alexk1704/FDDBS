package fdbs.sql;

public interface FedPseudoDriverInterface {

    FedConnection getConnection(String username, String password) throws FedException;
}
