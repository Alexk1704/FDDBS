/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package junit.fdbs.sql.meta;

import fdbs.sql.FedConnection;
import fdbs.sql.FedException;
import fdbs.sql.meta.MetadataManager;
import fdbs.sql.parser.ParseException;
import fdbs.sql.parser.SqlParser;
import fdbs.sql.parser.ast.AST;
import fdbs.util.statement.StatementUtil;
import java.io.IOException;

/**
 *
 * @author Nico
 */
public class ManagerTestUtils 
{
    /**
     * Method to SetUpDb for MetaDataManager tests. Recreates MetaDataTables and returns new Manager.
     * @param username The username for the db.
     * @param password The password for the db
     * @return The created MetaDataManager with the newly created datasource
     * @throws FedException 
     * @throws fdbs.sql.parser.ParseException 
     */
    public static MetadataManager SetUpDatabase(String username, String password) throws FedException, ParseException
    {
        FedConnection fed = new FedConnection(username, password);
        MetadataManager manager = MetadataManager.getInstance(fed);
        
        // Die Metadaten neu erstellen
        manager.deleteMetadata();
        manager.deleteTables();
        manager.checkAndRefreshTables();
        
        // Die Standardtabellen in die Datenbank einfügen
        try
        {
            String[] statements = StatementUtil.getStatementsFromFiles(new String[]{"test\\junit\\fdbs\\sql\\meta\\CreateQueries.sql"});
            for (String stm : statements) {
                SqlParser parser = new SqlParser(stm);
                AST ast = parser.parseStatement();
                manager.handleQuery(ast);
            }
        }
        catch(IOException e)
        {
            FedException f = new FedException("An Exception has occured");
            f.addSuppressed(e);
        }
        
        return manager;
    }
}
