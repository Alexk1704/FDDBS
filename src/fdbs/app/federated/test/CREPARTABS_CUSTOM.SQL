create table FLUGHAFEN (
FHC		varchar(3),
LAND		varchar(3),
STADT		varchar(50),
NAME		varchar(50),
RNDM                           integer,
constraint FLUGHAFEN_PS
		primary key (FHC),
constraint UNIQUE_PS
                                    unique (RNDM),
constraint RNDM_BETW
		check (RNDM between 0 and 2400),
constraint RNDM_CHK_LESEQ
                                    check (RNDM <= 0),
constraint LAND_STADT_NEQ
		check (LAND != STADT)
);


create table FLUGHAFENVTWO (
FHC		varchar(3),
LAND		varchar(3),
STADT		varchar(50),
NAME		varchar(50),
RNDM                           integer,
constraint FLUGHAFEN_PS
		primary key (FHC),
constraint UNIQUE_PS
                                    unique (RNDM),
constraint NOTNULL_NAME
                                    check (NAME is not null),
constraint RNDM_BETW
		check (RNDM between 0 and 2400),
constraint RNDM_CHK_LESEQ
                                    check (RNDM <= 0),
constraint LAND_STADT_NEQ
		check (LAND != STADT)
)
VERTICAL ((FHC, LAND, RNDM), (STADT, NAME));


create table FLUGHAFENVTHREE (
FHC		varchar(3),
LAND		varchar(3),
STADT		varchar(50),
NAME		varchar(50),
RNDM                           integer,
constraint FLUGHAFEN_PS
		primary key (FHC),
constraint UNIQUE_PS
                                    unique (RNDM),
constraint RNDM_BETW
		check (RNDM between 0 and 10),
constraint RNDM_CHK_LES
                                    check (RNDM < 10),
constraint NOTNULL_NAME
                                    check (NAME is not null),
constraint STADT_NAM_NEQ
		check (STADT != NAME)
)
VERTICAL ((FHC, LAND), (STADT, NAME),(RNDM));


create table FLUGLINIEHTWO (
FLC		varchar(2),
LAND		varchar(3),
HUB		varchar(3),
NAME		varchar(30),
ALLIANZ		varchar(20),
RNDM                           integer,
constraint FLUGLINIE_PS
		primary key (FLC),
constraint UNIQUE_PS
                                    unique (HUB),
constraint RNDM_BETW
		check (RNDM between 6 and 9),
constraint NOTNULL_NAME
                                    check (NAME is not null),
constraint RNDM_CHK_GRE
                                    check (RNDM > 0),
constraint ALL_NAM_NEQ
		check (ALLIANZ != NAME)
)
HORIZONTAL (FLC('KK'));


create table FLUGLINIEHTHREE (
FLC		varchar(2),
LAND		varchar(3),
HUB		varchar(3),
NAME		varchar(30),
ALLIANZ		varchar(20),
RNDM                           integer,
constraint FLUGLINIE_PS
		primary key (FLC),
constraint UNIQUE_HUB
                                    unique (HUB),
constraint RNDM_BETW
		check (RNDM between 1337 and 7331),
constraint NOTNULL_NAME
                                    check (NAME is not null),
constraint RNDM_CHK_GREQ
                                    check (RNDM >= 0),
constraint ALL_NAM_NEQ
		check (ALLIANZ != NAME)
)
HORIZONTAL (HUB('KK','MM'));


create table FLUGHAFENDFKD (
FHC		varchar(3),
LAND		varchar(3),
STADT		varchar(50),
NAME		varchar(50),
RNDM                           integer,
constraint FLUGHAFEN_PS
		primary key (FHC),
constraint FLUGHAFEN_FK
                                    foreign key (LAND) references FLUGHAFEN(FHC),
constraint UNIQUE_PS
                                    unique (RNDM),
constraint RNDM_BETW
		check (RNDM between 0 and 2400),
constraint RNDM_CHK_LESEQ
                                    check (RNDM <= 0),
constraint LAND_STADT_NEQ
		check (LAND != STADT)
);


create table FLUGHAFENDFKVTWO (
FHC		varchar(3),
LAND		varchar(3),
STADT		varchar(50),
NAME		varchar(50),
RNDM                           integer,
constraint FLUGHAFEN_PS
		primary key (FHC),
constraint FLUGHAFEN_FK
                                    foreign key (LAND) references FLUGHAFENVTWO(FHC),
constraint UNIQUE_PS
                                    unique (RNDM),
constraint RNDM_BETW
		check (RNDM between 0 and 2400),
constraint RNDM_CHK_LESEQ
                                    check (RNDM <= 0),
constraint LAND_STADT_NEQ
		check (LAND != STADT)
);


create table FLUGHAFENDFKVTHREE (
FHC		varchar(3),
LAND		varchar(3),
STADT		varchar(50),
NAME		varchar(50),
RNDM                           integer,
constraint FLUGHAFEN_PS
		primary key (FHC),
constraint FLUGHAFEN_FK
                                    foreign key (LAND) references FLUGHAFENVTHREE(FHC),
constraint UNIQUE_PS
                                    unique (RNDM),
constraint RNDM_BETW
		check (RNDM between 0 and 2400),
constraint RNDM_CHK_LESEQ
                                    check (RNDM <= 0),
constraint LAND_STADT_NEQ
		check (LAND != STADT)
);


create table FLUGHAFENDFKHTWO (
FHC		varchar(3),
LAND		varchar(3),
STADT		varchar(50),
NAME		varchar(50),
RNDM                           integer,
constraint FLUGHAFEN_PS
		primary key (FHC),
constraint FLUGHAFEN_FK
                                    foreign key (LAND) references FLUGLINIEHTWO(FLC),
constraint UNIQUE_PS
                                    unique (RNDM),
constraint RNDM_BETW
		check (RNDM between 0 and 2400),
constraint RNDM_CHK_LESEQ
                                    check (RNDM <= 0),
constraint LAND_STADT_NEQ
		check (LAND != STADT)
);


create table FLUGHAFENDFKHTHREE (
FHC		varchar(3),
LAND		varchar(3),
STADT		varchar(50),
NAME		varchar(50),
RNDM                           integer,
constraint FLUGHAFEN_PS
		primary key (FHC),
constraint FLUGHAFEN_FK
                                    foreign key (LAND) references FLUGLINIEHTHREE(FLC),
constraint UNIQUE_PS
                                    unique (RNDM),
constraint RNDM_BETW
		check (RNDM between 0 and 2400),
constraint RNDM_CHK_LESEQ
                                    check (RNDM <= 0),
constraint LAND_STADT_NEQ
		check (LAND != STADT)
);


create table FLUGHAFENVTWOFKDEFAULT (
FHC		varchar(3),
LAND		varchar(3),
STADT		varchar(50),
NAME		varchar(50),
RNDM                           integer,
constraint FLUGHAFEN_PS
		primary key (FHC),
constraint FLUGHAFEN_FK
                                    foreign key (LAND) references FLUGHAFEN(FHC),
constraint UNIQUE_PS
                                    unique (RNDM),
constraint NOTNULL_NAME
                                    check (NAME is not null),
constraint RNDM_BETW
		check (RNDM between 0 and 2400),
constraint RNDM_CHK_LESEQ
                                    check (RNDM <= 0),
constraint LAND_STADT_NEQ
		check (LAND != STADT)
)
VERTICAL ((FHC, LAND, RNDM), (STADT, NAME));


create table FLUGHAFENVTWOFKVTWO (
FHC		varchar(3),
LAND		varchar(3),
STADT		varchar(50),
NAME		varchar(50),
RNDM                           integer,
constraint FLUGHAFEN_PS
		primary key (FHC),
constraint FLUGHAFEN_FK
                                    foreign key (LAND) references FLUGHAFENVTWO(FHC),
constraint UNIQUE_PS
                                    unique (RNDM),
constraint NOTNULL_NAME
                                    check (NAME is not null),
constraint RNDM_BETW
		check (RNDM between 0 and 2400),
constraint RNDM_CHK_LESEQ
                                    check (RNDM <= 0),
constraint LAND_STADT_NEQ
		check (LAND != STADT)
)
VERTICAL ((FHC, LAND, RNDM), (STADT, NAME));


create table FLUGHAFENVTWOFKVTHREE (
FHC		varchar(3),
LAND		varchar(3),
STADT		varchar(50),
NAME		varchar(50),
RNDM                           integer,
constraint FLUGHAFEN_PS
		primary key (FHC),
constraint FLUGHAFEN_FK
                                    foreign key (LAND) references FLUGHAFENVTHREE(FHC),
constraint UNIQUE_PS
                                    unique (RNDM),
constraint NOTNULL_NAME
                                    check (NAME is not null),
constraint RNDM_BETW
		check (RNDM between 0 and 2400),
constraint RNDM_CHK_LESEQ
                                    check (RNDM <= 0),
constraint LAND_STADT_NEQ
		check (LAND != STADT)
)
VERTICAL ((FHC, LAND, RNDM), (STADT, NAME));


create table FLUGHAFENVTHREEFKDEFAULT (
FHC		varchar(3),
LAND		varchar(3),
STADT		varchar(50),
NAME		varchar(50),
RNDM                           integer,
constraint FLUGHAFEN_PS
		primary key (FHC),
constraint FLUGHAFEN_FK
                                    foreign key (LAND) references FLUGHAFEN(FHC),
constraint UNIQUE_PS
                                    unique (RNDM),
constraint RNDM_BETW
		check (RNDM between 0 and 10),
constraint RNDM_CHK_LES
                                    check (RNDM < 10),
constraint NOTNULL_NAME
                                    check (NAME is not null),
constraint STADT_NAM_NEQ
		check (STADT != NAME)
)
VERTICAL ((FHC, LAND), (STADT, NAME),(RNDM));


create table FLUGHAFENVTHREEFKVTWO (
FHC		varchar(3),
LAND		varchar(3),
STADT		varchar(50),
NAME		varchar(50),
RNDM                           integer,
constraint FLUGHAFEN_PS
		primary key (FHC),
constraint FLUGHAFEN_FK
                                    foreign key (LAND) references FLUGHAFENVTWO(FHC),
constraint UNIQUE_PS
                                    unique (RNDM),
constraint RNDM_BETW
		check (RNDM between 0 and 10),
constraint RNDM_CHK_LES
                                    check (RNDM < 10),
constraint NOTNULL_NAME
                                    check (NAME is not null),
constraint STADT_NAM_NEQ
		check (STADT != NAME)
)
VERTICAL ((FHC, LAND), (STADT, NAME),(RNDM));


create table FLUGHAFENVTHREEFKVTHREE (
FHC		varchar(3),
LAND		varchar(3),
STADT		varchar(50),
NAME		varchar(50),
RNDM                           integer,
constraint FLUGHAFEN_PS
		primary key (FHC),
constraint FLUGHAFEN_FK
                                    foreign key (LAND) references FLUGHAFENVTHREE(FHC),
constraint UNIQUE_PS
                                    unique (RNDM),
constraint RNDM_BETW
		check (RNDM between 0 and 10),
constraint RNDM_CHK_LES
                                    check (RNDM < 10),
constraint NOTNULL_NAME
                                    check (NAME is not null),
constraint STADT_NAM_NEQ
		check (STADT != NAME)
)
VERTICAL ((FHC, LAND), (STADT, NAME),(RNDM));


create table FLUGHAFENVTWOFKHTWO (
FHC		varchar(3),
LAND		varchar(3),
STADT		varchar(50),
NAME		varchar(50),
RNDM                           integer,
constraint FLUGHAFEN_PS
		primary key (FHC),
constraint FLUGHAFEN_FK
                                    foreign key (LAND) references FLUGLINIEHTWO(FLC),
constraint UNIQUE_PS
                                    unique (RNDM),
constraint NOTNULL_NAME
                                    check (NAME is not null),
constraint RNDM_BETW
		check (RNDM between 0 and 2400),
constraint RNDM_CHK_LESEQ
                                    check (RNDM <= 0),
constraint LAND_STADT_NEQ
		check (LAND != STADT)
)
VERTICAL ((FHC, LAND, RNDM), (STADT, NAME));


create table FLUGHAFENVTWOFKHTHREE (
FHC		varchar(3),
LAND		varchar(3),
STADT		varchar(50),
NAME		varchar(50),
RNDM                           integer,
constraint FLUGHAFEN_PS
		primary key (FHC),
constraint FLUGHAFEN_FK
                                    foreign key (LAND) references FLUGLINIEHTHREE(FLC),
constraint UNIQUE_PS
                                    unique (RNDM),
constraint NOTNULL_NAME
                                    check (NAME is not null),
constraint RNDM_BETW
		check (RNDM between 0 and 2400),
constraint RNDM_CHK_LESEQ
                                    check (RNDM <= 0),
constraint LAND_STADT_NEQ
		check (LAND != STADT)
)
VERTICAL ((FHC, LAND, RNDM), (STADT, NAME));

create table FLUGHAFENVTHREEFKHTWO (
FHC		varchar(3),
LAND		varchar(3),
STADT		varchar(50),
NAME		varchar(50),
RNDM                           integer,
constraint FLUGHAFEN_PS
		primary key (FHC),
constraint FLUGHAFEN_FK
                                    foreign key (LAND) references FLUGLINIEHTWO(FLC),
constraint UNIQUE_PS
                                    unique (RNDM),
constraint RNDM_BETW
		check (RNDM between 0 and 10),
constraint RNDM_CHK_LES
                                    check (RNDM < 10),
constraint NOTNULL_NAME
                                    check (NAME is not null),
constraint STADT_NAM_NEQ
		check (STADT != NAME)
)
VERTICAL ((FHC, LAND), (STADT, NAME),(RNDM));

create table FLUGHAFENVTHREEFKHTHREE (
FHC		varchar(3),
LAND		varchar(3),
STADT		varchar(50),
NAME		varchar(50),
RNDM                           integer,
constraint FLUGHAFEN_PS
		primary key (FHC),
constraint FLUGHAFEN_FK
                                    foreign key (LAND) references FLUGLINIEHTHREE(FLC),
constraint UNIQUE_PS
                                    unique (RNDM),
constraint RNDM_BETW
		check (RNDM between 0 and 10),
constraint RNDM_CHK_LES
                                    check (RNDM < 10),
constraint NOTNULL_NAME
                                    check (NAME is not null),
constraint STADT_NAM_NEQ
		check (STADT != NAME)
)
VERTICAL ((FHC, LAND), (STADT, NAME),(RNDM));


create table FLUGLINIEHTWOFKD (
FLC		varchar(2),
LAND		varchar(3),
HUB		varchar(3),
NAME		varchar(30),
ALLIANZ		varchar(20),
RNDM                           integer,
constraint FLUGLINIE_PS
		primary key (FLC),
constraint FLUGHAFEN_FK
                                    foreign key (LAND) references FLUGHAFEN(FHC),
constraint UNIQUE_PS
                                    unique (HUB),
constraint RNDM_BETW
		check (RNDM between 6 and 9),
constraint NOTNULL_NAME
                                    check (NAME is not null),
constraint RNDM_CHK_GRE
                                    check (RNDM > 0),
constraint ALL_NAM_NEQ
		check (ALLIANZ != NAME)
)
HORIZONTAL (FLC('KK'));


create table FLUGLINIEHTHREEFKD (
FLC		varchar(2),
LAND		varchar(3),
HUB		varchar(3),
NAME		varchar(30),
ALLIANZ		varchar(20),
RNDM                           integer,
constraint FLUGLINIE_PS
		primary key (FLC),
constraint FLUGHAFEN_FK
                                    foreign key (LAND) references FLUGHAFEN(FHC),
constraint UNIQUE_HUB
                                    unique (HUB),
constraint RNDM_BETW
		check (RNDM between 1337 and 7331),
constraint NOTNULL_NAME
                                    check (NAME is not null),
constraint RNDM_CHK_GREQ
                                    check (RNDM >= 0),
constraint ALL_NAM_NEQ
		check (ALLIANZ != NAME)
)
HORIZONTAL (HUB('KK','MM'));


create table FLUGLINIEHTWOFKHTWO (
FLC		varchar(2),
LAND		varchar(3),
HUB		varchar(3),
NAME		varchar(30),
ALLIANZ		varchar(20),
RNDM                           integer,
constraint FLUGLINIE_PS
		primary key (FLC),
constraint FLUGHAFEN_FK
                                    foreign key (LAND) references FLUGLINIEHTWO(FLC),
constraint UNIQUE_PS
                                    unique (HUB),
constraint RNDM_BETW
		check (RNDM between 6 and 9),
constraint NOTNULL_NAME
                                    check (NAME is not null),
constraint RNDM_CHK_GRE
                                    check (RNDM > 0),
constraint ALL_NAM_NEQ
		check (ALLIANZ != NAME)
)
HORIZONTAL (FLC('KK'));


create table FLUGLINIEHTWOFKHTHREE (
FLC		varchar(2),
LAND		varchar(3),
HUB		varchar(3),
NAME		varchar(30),
ALLIANZ		varchar(20),
RNDM                           integer,
constraint FLUGLINIE_PS
		primary key (FLC),
constraint FLUGHAFEN_FK
                                    foreign key (LAND) references FLUGLINIEHTHREE(FLC),
constraint UNIQUE_PS
                                    unique (HUB),
constraint RNDM_BETW
		check (RNDM between 6 and 9),
constraint NOTNULL_NAME
                                    check (NAME is not null),
constraint RNDM_CHK_GRE
                                    check (RNDM > 0),
constraint ALL_NAM_NEQ
		check (ALLIANZ != NAME)
)
HORIZONTAL (FLC('KK'));


create table FLUGLINIEHTHREEFKHTWO (
FLC		varchar(2),
LAND		varchar(3),
HUB		varchar(3),
NAME		varchar(30),
ALLIANZ		varchar(20),
RNDM                           integer,
constraint FLUGLINIE_PS
		primary key (FLC),
constraint FLUGHAFEN_FK
                                    foreign key (LAND) references FLUGLINIEHTWO(FLC),
constraint UNIQUE_HUB
                                    unique (HUB),
constraint RNDM_BETW
		check (RNDM between 1337 and 7331),
constraint NOTNULL_NAME
                                    check (NAME is not null),
constraint RNDM_CHK_GREQ
                                    check (RNDM >= 0),
constraint ALL_NAM_NEQ
		check (ALLIANZ != NAME)
)
HORIZONTAL (HUB('KK','MM'));


create table FLUGLINIEHTHREEFKHTHREE (
FLC		varchar(2),
LAND		varchar(3),
HUB		varchar(3),
NAME		varchar(30),
ALLIANZ		varchar(20),
RNDM                           integer,
constraint FLUGLINIE_PS
		primary key (FLC),
constraint FLUGHAFEN_FK
                                    foreign key (LAND) references FLUGLINIEHTHREE(FLC),
constraint UNIQUE_HUB
                                    unique (HUB),
constraint RNDM_BETW
		check (RNDM between 1337 and 7331),
constraint NOTNULL_NAME
                                    check (NAME is not null),
constraint RNDM_CHK_GREQ
                                    check (RNDM >= 0),
constraint ALL_NAM_NEQ
		check (ALLIANZ != NAME)
)
HORIZONTAL (HUB('KK','MM'));


create table FLUGLINIEHTWOFKVTWO (
FLC		varchar(2),
LAND		varchar(3),
HUB		varchar(3),
NAME		varchar(30),
ALLIANZ		varchar(20),
RNDM                           integer,
constraint FLUGLINIE_PS
		primary key (FLC),
constraint FLUGHAFEN_FK
                                    foreign key (LAND) references FLUGHAFENVTWO(FHC),
constraint UNIQUE_PS
                                    unique (HUB),
constraint RNDM_BETW
		check (RNDM between 6 and 9),
constraint NOTNULL_NAME
                                    check (NAME is not null),
constraint RNDM_CHK_GRE
                                    check (RNDM > 0),
constraint ALL_NAM_NEQ
		check (ALLIANZ != NAME)
)
HORIZONTAL (FLC('KK'));


create table FLUGLINIEHTWOFKVTHREE (
FLC		varchar(2),
LAND		varchar(3),
HUB		varchar(3),
NAME		varchar(30),
ALLIANZ		varchar(20),
RNDM                           integer,
constraint FLUGLINIE_PS
		primary key (FLC),
constraint FLUGHAFEN_FK
                                    foreign key (LAND) references FLUGHAFENVTHREE(FHC),
constraint UNIQUE_PS
                                    unique (HUB),
constraint RNDM_BETW
		check (RNDM between 6 and 9),
constraint NOTNULL_NAME
                                    check (NAME is not null),
constraint RNDM_CHK_GRE
                                    check (RNDM > 0),
constraint ALL_NAM_NEQ
		check (ALLIANZ != NAME)
)
HORIZONTAL (FLC('KK'));


create table FLUGLINIEHTHREEFKVTWO (
FLC		varchar(2),
LAND		varchar(3),
HUB		varchar(3),
NAME		varchar(30),
ALLIANZ		varchar(20),
RNDM                           integer,
constraint FLUGLINIE_PS
		primary key (FLC),
constraint FLUGHAFEN_FK
                                    foreign key (LAND) references FLUGHAFENVTWO(FHC),
constraint UNIQUE_HUB
                                    unique (HUB),
constraint RNDM_BETW
		check (RNDM between 1337 and 7331),
constraint NOTNULL_NAME
                                    check (NAME is not null),
constraint RNDM_CHK_GREQ
                                    check (RNDM >= 0),
constraint ALL_NAM_NEQ
		check (ALLIANZ != NAME)
)
HORIZONTAL (HUB('KK','MM'));


create table FLUGLINIEHTHREEFKVTHREE (
FLC		varchar(2),
LAND		varchar(3),
HUB		varchar(3),
NAME		varchar(30),
ALLIANZ		varchar(20),
RNDM                           integer,
constraint FLUGLINIE_PS
		primary key (FLC),
constraint FLUGHAFEN_FK
                                    foreign key (LAND) references FLUGHAFENVTHREE(FHC),
constraint UNIQUE_HUB
                                    unique (HUB),
constraint RNDM_BETW
		check (RNDM between 1337 and 7331),
constraint NOTNULL_NAME
                                    check (NAME is not null),
constraint RNDM_CHK_GREQ
                                    check (RNDM >= 0),
constraint ALL_NAM_NEQ
		check (ALLIANZ != NAME)
)
HORIZONTAL (HUB('KK','MM'));
