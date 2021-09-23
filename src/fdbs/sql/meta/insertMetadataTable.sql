/* set echo on und alter werden nicht unterstützt */

/* set echo on;

alter session set nls_language = english;
alter session set nls_date_format = 'DD-MON-YYYY';
alter session set nls_date_language = english;

Database id:
database1=jdbc\:oracle\:thin\:pinatubo.informatik.hs-fulda.de\:1521\:ORALV8A
database2=jdbc\:oracle\:thin\:mtsthelens.informatik.hs-fulda.de\:1521\:ORALV9A
database3=jdbc\:oracle\:thin\:krakatau.informatik.hs-fulda.de\:1521\:ORALV10A


*/


--Insert testvalues in Meta_Table_Partition
INSERT INTO Meta_Table_Partition VALUES (1, 'Passagier', 'V');
INSERT INTO Meta_Table_Partition VALUES (2, 'Fluglinie', 'V');
INSERT INTO Meta_Table_Partition VALUES (3, 'Flug', 'H');


--Insert testvalues in Meta_Columns
--Vertical Passagier
INSERT INTO Meta_Columns VALUES (1, 1, 'Name', 'VARCHAR', 1);
INSERT INTO Meta_Columns VALUES (2, 1, 'Vorname', 'VARCHAR', 1);
--Vertical Fluglinie
INSERT INTO Meta_Columns VALUES (3,2, 'Name', 'VARCHAR', 2);
INSERT INTO Meta_Columns VALUES (4,2, 'Headquarter', 'VARCHAR', 3);
--Horizontal Flug
--If NULL = Horizontal --> Meta_Horizontal
INSERT INTO Meta_Columns VALUES (5,3, 'Identifier', 'VARCHAR', NULL);
INSERT INTO Meta_Columns VALUES (6,3, 'VON', 'VARCHAR', NULL);
INSERT INTO Meta_Columns VALUES (7,3, 'NACH', 'VARCHAR', NULL);

--Insert testvalues in Meta_Table_Constraints
-- Passagier
INSERT INTO Meta_Table_Constraints VALUES (1, 1, 'Constraint_Name_1', 'P', 'Name', NULL, NULL);
INSERT INTO Meta_Table_Constraints VALUES (1, 1, 'Constraint_Name_2', 'F', 'VORNAME','Igendneanderetabelle', 'IrgendeieneSpalte');
-- Fluglinie
INSERT INTO Meta_Table_Constraints VALUES (1, 2, 'Constraint_Name_11', 'P', 'Name', NULL, NULL);
INSERT INTO Meta_Table_Constraints VALUES (1, 3, 'Constraint_Name_22', 'F', 'Headquarter','Igendneanderetabelle1', 'IrgendeieneSpalte1');
-- Flug
INSERT INTO Meta_Table_Constraints VALUES (1, 3, 'Constraint_Name_111', 'P', 'Identifier', NULL, NULL);
INSERT INTO Meta_Table_Constraints VALUES (1, 3, 'Constraint_Name_222', 'F', 'VON','Igendneanderetabelle2', 'IrgendeieneSpalte2');
INSERT INTO Meta_Table_Constraints VALUES (1, 3, 'Constraint_Name_333', 'F', 'NACH','Igendneanderetabelle3', 'IrgendeieneSpalte3');

--Insert testvalues in Meta_Horizontal
--Horizontal Flug
INSERT INTO Meta_Horizontal VALUES (1, 3, 'VON', 'C', 'A','C', 'A', 1);
INSERT INTO Meta_Horizontal VALUES (2, 3, 'VON', 'C', 'D','L', 'D', 2);
INSERT INTO Meta_Horizontal VALUES (3, 3, 'VON', 'C', 'M','Z', 'M', 3);

INSERT INTO Meta_Horizontal VALUES (4, 3, 'NACH', 'C', 'A','D', 'A', 2);
INSERT INTO Meta_Horizontal VALUES (5, 3, 'NACH', 'C', 'D','M', 'D', 3);
INSERT INTO Meta_Horizontal VALUES (6, 3, 'NACH', 'C', 'N','Z', 'N', 1);

