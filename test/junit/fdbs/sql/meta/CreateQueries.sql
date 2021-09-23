create table ADRESSE (AID integer, STATE varchar(40), NAME varchar(50), constraint ADRESSE_AID primary key(AID), constraint AID_UNIQUE_STATE UNIQUE(STATE), constraint ADRESSE_STATE_BTW check (STATE BETWEEN 'A' AND 'Z'), constraint ADRESSE_LAND_LQ check (STATE < 'Z'), constraint ADRESSE_LANDNAME_LEQ check (STATE <= NAME), constraint ADRESSE_NAME_NN check (NAME is not null)) HORIZONTAL (STATE('CM'));
create table STADT (SID integer, NAME varchar(30), constraint STADT_PS primary key(SID));
create table FLUGHAFEN ( FHC varchar(3), SID integer, LAND varchar(3), STADT varchar(50) DEFAULT('Berlin'), NAME varchar(50), constraint FLUGHAFEN_PS primary key (FHC), constraint FLUGHAFEN_NAME_NN check (NAME is not null), constraint FHC_UNIQUE_LAND UNIQUE(LAND), constraint FLUGHAFEN_NAME_BW check (NAME BETWEEN 'A' AND 'Z'), constraint FLUGHAFEN_SID_FK foreign key (SID) references STADT(SID), constraint FLUGHAFEN_FK_STADT foreign key (STADT) references STADT(NAME)) VERTICAL (( LAND, SID), (NAME,FHC, STADT));
create table PASSAGIER ( PNR integer, NAME varchar(40), VORNAME varchar(40), LAND varchar(3), AID integer, constraint PASSAGIER_FK_AID foreign key (AID) references ADRESSE(AID), constraint PASSAGIER_NAME_NN check (NAME is not null), constraint PASSAGIER_PS  primary key (PNR) ) HORIZONTAL (PNR(35,70));
create table FLUGZEUG ( FNR integer, NAME varchar(40) DEFAULT('Airbus'), FHC varchar(3), constraint FLUGZEUG_PK primary key(FNR), constraint FLUGZEUG_UNIQUE UNIQUE(NAME), constraint FLUGZEUG_FK_FHC foreign key (FHC) references FLUGHAFEN(FHC)) HORIZONTAL (FNR(40)); 
create table BUCHUNG ( BNR integer, PNR integer, FNR varchar(2), FLNR integer DEFAULT(1), VON varchar(3), NACH varchar(3), TAG varchar(20), MEILEN integer, PREIS integer, constraint BUCHUNG_NACH_NN  check (NACH is not null), constraint BUCHUNG_MEILEN_NN  check (MEILEN is not null), constraint BUCHUNG_PREIS_NN  check (PREIS is not null), constraint BUCHUNG_MEILEN_CHK check (MEILEN >= 0), constraint BUCHUNG_MEILEN_BTW check (MEILEN BETWEEN 0 AND 12),  constraint BUCHUNG_PREIS_CHK check (PREIS > 0), constraint BUCHUNG_PS  primary key (BNR), constraint BUCHUNG_FS_PNR  foreign key (PNR) references PASSAGIER(PNR), constraint BUCHUNG_FS_FNR  foreign key (FNR) references FLUGZEUG(FNR), constraint BUCHUNG_FS_VON  foreign key (VON) references FLUGHAFEN(FHC), constraint BUCHUNG_FS_NACH  foreign key (NACH) references FLUGHAFEN(FHC), constraint BUCHUNG_Binary check (MEILEN > PREIS), constraint BNR_UNIQUE_FLNR UNIQUE(FLNR) );
create table MITARBEITER (MID integer, STATE varchar(40), NAME varchar(50), LAND varchar(100)  Default('DE'), CITY varchar(30), constraint MITARBEITER_MID primary key(MID), constraint MID_UNIQUE_NAME UNIQUE(NAME), constraint MITARBEITER_LAND_BTW check (LAND BETWEEN 'A' AND 'Z'), constraint MITARBEITER_LAND_GQ check (LAND > 'A'), constraint MITARBEITER_LANDNAME_GQ check (LAND > NAME), constraint MITARBEITER_LANDCITY_GQ check (LAND > CITY), constraint MITARBEITER_NAME_NN check (NAME is not null), constraint MITARBEITER_LAND_FK foreign key (LAND) references FLUGHAFEN(LAND)) VERTICAL((MID, STATE),(NAME), (LAND, CITY));
create table PERSON ( PID integer, STATE varchar(40), NAME varchar(50), MID integer, BNR integer, FHR integer, constraint PERSON_FK_FHR foreign key (FHR) references FLUGHAFEN(FHR), constraint PERSON_FK_BNR foreign key (BNR) references BUCHUNG(BNR), constraint PERSONEN_PID primary key(PID), constraint PID_UNIQUE_STATE UNIQUE(STATE), constraint PERSON_STATE_BTW check (STATE BETWEEN 'A' AND 'Z'), constraint PERSON_LAND_GEQ check (STATE >= 'A'), constraint PERSON_LANDNAME_GEQ check (STATE >= NAME), constraint PERSON_NAME_NN check (NAME is not null), constraint PERSON_MID_FK foreign key (MID) references MITARBEITER(MID)) HORIZONTAL (STATE('CT', 'TX')); 
create table STORNIERUNG (SNR integer, BNR integer, Tag varchar(20), constraint STORNIERUNG_SNR primary key(SNR), constraint STORNIERUNG_FK_SNR foreign key (BNR) references BUCHUNG(BNR));
create table CHEF (CID integer, MID integer, MID2 integer, NAME varchar(50), constraint CHEF_CID primary key(CID), constraint CHEF_FK_MID foreign key (MID) references MITARBEITER(MID), constraint CHEF_FK_MID2 foreign key (MID2) references MITARBEITER(MID))  VERTICAL((NAME), (MID), (CID, MID2));