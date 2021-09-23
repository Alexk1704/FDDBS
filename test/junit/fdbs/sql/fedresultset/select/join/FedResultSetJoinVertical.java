/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package junit.fdbs.sql.fedresultset.select.join;

import fdbs.sql.FedException;

/**
 *
 * @author patrick
 */

//tests aus FedResultSetJoin mit anderem Table-Setup durchlaufen. Ja, die werden vererbt.
public class FedResultSetJoinVertical extends FedResultSetJoin {

    public FedResultSetJoinVertical() {
        super();
    }
    
    @Override
    public void createTestTables()throws FedException {
        executeUpdate("create table FLUGHAFEN ( FHC varchar(3), LAND varchar(3), STADT varchar(50), NAME varchar(50), constraint FLUGHAFEN_PS primary key (FHC) ) VERTICAL ((FHC, LAND), (STADT, NAME))");
        executeUpdate("create table FLUGLINIE ( FLC varchar(2), LAND varchar(3), HUB varchar(3), NAME varchar(30), ALLIANZ varchar(20), constraint FLUGLINIE_PS primary key (FLC), constraint FLUGLINIE_FS_HUB foreign key (HUB) "
                + "references FLUGHAFEN(FHC), constraint FLUGLINIE_LAND_NN check (LAND is not null), constraint FLUGLINIE_ALLIANZ_CHK check (ALLIANZ != 'BlackList') )"
                + " VERTICAL ((FLC, LAND), (HUB, NAME),(ALLIANZ))");
    }
    
}
