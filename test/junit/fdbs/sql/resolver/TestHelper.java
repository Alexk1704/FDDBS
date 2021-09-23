/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package junit.fdbs.sql.resolver;

import fdbs.sql.FedPseudoDriver;
import fdbs.sql.FedConnection;
import fdbs.sql.FedException;
import fdbs.sql.executer.SQLExecuter;
import fdbs.sql.executer.SQLExecuterTask;
import fdbs.sql.meta.MetadataManager;
import java.sql.SQLException;
/**
 *
 * @author patrick
 */
public class TestHelper {
    private FedConnection fed;
    private MetadataManager manager;
    
   public TestHelper (FedConnection fedConn) throws FedException{
        fed = fedConn;
        manager = MetadataManager.getInstance(fed); 
    }
   
   public void resetDatabases() {
        try {
            executeStatement("DROP TABLE BUCHUNG", 0);
        } catch (FedException | SQLException ex) {
            System.out.println(ex.getMessage());
            System.out.println("BUCHUNG");
        }

        try {
            executeStatement("DROP TABLE PASSAGIER", 0);
        } catch (FedException | SQLException ex) {
            System.out.println(ex.getMessage());
            System.out.println("PASSAGIER");
        }

        try {
            executeStatement("DROP TABLE FLUG", 0);
        } catch (FedException | SQLException ex) {
            System.out.println(ex.getMessage());
            System.out.println("FLUG");
        }
        try {
            executeStatement("DROP TABLE FLUGLINIE", 0);
        } catch (FedException | SQLException ex) {
            System.out.println(ex.getMessage());
            System.out.println("FLUGLINIE");
        }

        try {
            executeStatement("DROP TABLE FLUGHAFEN", 0);
        } catch (FedException | SQLException ex) {
            System.out.println(ex.getMessage());
            System.out.println("FLUGHAFEN");
        }       

   }
   
   public void resetMetadata() throws FedException{
      manager.deleteMetadata();
      manager.deleteTables();
      manager.checkAndRefreshTables();              
   }
   
    protected void executeStatement(String stmt, Integer db) throws FedException, SQLException {
        SQLExecuter sqlExecuter = new SQLExecuter(fed);
        try (SQLExecuterTask task = new SQLExecuterTask()) {
            if (db == 0) {
                task.addSubTask(stmt, true, 1);
                task.addSubTask(stmt, true, 2);
                task.addSubTask(stmt, true, 3);
            } else if (db == 1) {
                task.addSubTask(stmt, true, 1);
            } else if (db == 2) {
                task.addSubTask(stmt, true, 2);
            } else if (db == 3) {
                task.addSubTask(stmt, true, 3);
            }

            sqlExecuter.executeTask(task);
        }
    }   
    
    protected SQLExecuterTask executeSelect(String stmt, Integer db) throws FedException, SQLException {
        SQLExecuter sqlExecuter = new SQLExecuter(fed);
       
            SQLExecuterTask task = new SQLExecuterTask();
            if (db == 0) {
                task.addSubTask(stmt, false, 1);
                task.addSubTask(stmt, false, 2);
                task.addSubTask(stmt, false, 3);
            } else if (db == 1) {
                task.addSubTask(stmt, false, 1);
            } else if (db == 2) {
                task.addSubTask(stmt, false, 2);
            } else if (db == 3) {
                task.addSubTask(stmt, false, 3);
            }

            sqlExecuter.executeTask(task);
            return task;
        
    }     
   
}
