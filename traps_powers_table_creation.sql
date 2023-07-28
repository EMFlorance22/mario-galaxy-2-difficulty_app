CREATE SEQUENCE trap_sequence
START WITH 4000
INCREMENT BY 1;

CREATE TABLE traps (
    id INT DEFAULT trap_sequence.nextval NOT NULL PRIMARY KEY,
    name VARCHAR2(30) NOT NULL,
    description VARCHAR2(50)
);

CREATE OR REPLACE PROCEDURE insert_trap (
    trap_name VARCHAR2) AS
    
BEGIN
    INSERT INTO traps(name) VALUES (trap_name);
END;
    
CREATE OR REPLACE PROCEDURE extract_traps ( --Will find new traps in the trap string and add them to the traps table
    traps_string VARCHAR2
    ) AS

    trap VARCHAR2(30);
    cnt INT;
BEGIN
    FOR i IN (SELECT REGEXP_SUBSTR(traps_string, '[^,]+', 1, level) AS parts FROM dual
                CONNECT BY REGEXP_SUBSTR(traps_string, '[^,]+', 1, level) IS NOT NULL)
            
                LOOP
                    SELECT TRIM(i.parts) INTO trap FROM dual;
                    SELECT COUNT(*) INTO cnt FROM traps WHERE name = trap;
                    IF cnt = 0 THEN
                        insert_trap(trap);
                        dbms_output.put_line(trap || ' Inserted');
                    END IF;
                    
                END LOOP;
END;

--Run a program that will add all new traps to the traps table
DECLARE
    cursor traps_cursor IS SELECT id, traps FROM stars; --Return the star_id, enemies, and new enemies string
    trap stars.traps%TYPE;
    
BEGIN
    FOR rec IN traps_cursor
    
    LOOP
        IF rec.traps <> 'None' THEN
            SELECT rec.traps INTO trap FROM dual;
            extract_traps(trap);
            
        ELSE CONTINUE;
        END IF;
    END LOOP;
END;

--Create a new procedure that will add trap details to the trap details table (after creating the table and the insert_trapdets procedure)
--Make sure to skip when the trap is squishing platforms

CREATE TABLE trap_details (
    id INT DEFAULT basic_sequence.nextval NOT NULL PRIMARY KEY,
    trap_id NUMBER(4),
    t_star_id NUMBER(4)
);

CREATE OR REPLACE PROCEDURE insert_trapdets (
    trap NUMBER,
    star NUMBER) AS

BEGIN
    INSERT INTO trap_details(trap_id, t_star_id) VALUES(trap, star);
    
END;

DECLARE
    CURSOR trapdets_cursor IS SELECT id, traps FROM stars;
    star_id stars.id%TYPE;
    trap_id traps.id%TYPE;
    trap_name stars.traps%TYPE;
    
BEGIN
    FOR rec IN trapdets_cursor
    
    LOOP
        IF rec.traps <> 'None' THEN
            FOR i IN (SELECT REGEXP_SUBSTR(rec.traps, '[^,]+', 1, level) AS parts FROM dual
                CONNECT BY REGEXP_SUBSTR(rec.traps, '[^,]+', 1, level) IS NOT NULL)
            
                LOOP
                BEGIN
                    SELECT TRIM(i.parts) INTO trap_name FROM dual; --Traps are separated by a comma and space so we need to trim the white space
                    SELECT rec.id INTO star_id FROM dual;
                    SELECT id INTO trap_id FROM traps WHERE name = trap_name;
                    dbms_output.put_line('This worked for ' || trap_name);
                    insert_trapdets(trap_id, star_id);
                EXCEPTION --Catch the exception during the loop so it can add the new trap to the table and continue iterating
                    WHEN no_data_found THEN --If we find a new trap in the traps column we need to add that trap to the traps table
                        insert_trap(trap_name);
                        SELECT id INTO trap_id FROM traps WHERE name = trap_name;
                        dbms_output.put_line('Adding a new trap: ' || trap_name);
                        insert_trapdets(trap_id, star_id);
                END;
                END LOOP;
        ELSE
            CONTINUE;
        END IF;
            
    END LOOP;

END;

SELECT * from traps; --should be 47 rows
SELECT * FROM trap_details; --Should be 211 rows

CREATE SEQUENCE pow_sequence --Powerups Table Sequence
START WITH 5000
INCREMENT BY 1;

CREATE TABLE powerups ( --Create the powerups table
    id INT DEFAULT pow_sequence.nextval NOT NULL PRIMARY KEY,
    name VARCHAR2(30),
    description VARCHAR2(50)
);

CREATE TABLE power_details ( --Create the powerup details table
    id INT DEFAULT basic_sequence.nextval NOT NULL PRIMARY KEY,
    pow_id NUMBER(4),
    star_id NUMBER(4)
);

CREATE OR REPLACE PROCEDURE insert_powerup (
    powerup_name VARCHAR2) AS
    
BEGIN
    INSERT INTO powerups(name) VALUES (powerup_name);
END;

CREATE OR REPLACE PROCEDURE insert_pow_details (
    powerup NUMBER,
    star NUMBER) AS

BEGIN
    INSERT INTO power_details(pow_id, star_id) VALUES(powerup, star);
    
END;

--Fill the powerups and power_details table similar to traps table

CREATE OR REPLACE PROCEDURE extract_powers (
    power_string VARCHAR2
    ) AS

    powerup VARCHAR2(30);
    cnt INT;
BEGIN
    FOR i IN (SELECT REGEXP_SUBSTR(power_string, '[^,]+', 1, level) AS parts FROM dual
                CONNECT BY REGEXP_SUBSTR(power_string, '[^,]+', 1, level) IS NOT NULL)
            
                LOOP
                    SELECT TRIM(i.parts) INTO powerup FROM dual;
                    SELECT COUNT(*) INTO cnt FROM powerups WHERE name = powerup;
                    IF cnt = 0 THEN
                        insert_powerup(powerup);
                        dbms_output.put_line(Powerup || ' Inserted');
                    END IF;
                    
                END LOOP;
END;

-- Insert powerups into the powerups table, very similar to traps
DECLARE
    cursor power_cursor IS SELECT id, powerups FROM stars; --Return the star_id, enemies, and new enemies string
    powerup stars.powerups%TYPE;
    
BEGIN
    FOR rec IN power_cursor
    
    LOOP
        IF rec.powerups <> 'None' THEN
            SELECT rec.powerups INTO powerup FROM dual;
            extract_powers(powerup);
            
        ELSE CONTINUE;
        END IF;
    END LOOP;
END;

--Insert Powerup Details Script, very similar to trap details
DECLARE
    CURSOR powerdets_cursor IS SELECT id, powerups FROM stars;
    star_id stars.id%TYPE;
    power_id powerups.id%TYPE;
    power_name stars.powerups%TYPE;
    
BEGIN
    FOR rec IN powerdets_cursor
    
    LOOP
        IF rec.powerups <> 'None' THEN
            FOR i IN (SELECT REGEXP_SUBSTR(rec.powerups, '[^,]+', 1, level) AS parts FROM dual
                CONNECT BY REGEXP_SUBSTR(rec.powerups, '[^,]+', 1, level) IS NOT NULL)
            
                LOOP
                BEGIN
                    SELECT TRIM(i.parts) INTO power_name FROM dual; --Enemies are separated by a comma and space so we need to trim the white space
                    SELECT rec.id INTO star_id FROM dual;
                    SELECT id INTO power_id FROM powerups WHERE name = power_name;
                    dbms_output.put_line('This worked for ' || power_name);
                    insert_pow_details(power_id, star_id);
                EXCEPTION --Catch the exception during the loop so it can add the new powerup to the table and continue iterating
                    WHEN no_data_found THEN --If we find a new power in the powerups column we need to add that powerup to the powerups table
                        insert_powerup(power_name);
                        SELECT id INTO power_id FROM powerups WHERE name = power_name;
                        dbms_output.put_line('Adding a new powerup: ' || power_name);
                        insert_pow_details(power_id, star_id);
                END;
                END LOOP;
        ELSE
            CONTINUE;
        END IF;
            
    END LOOP;

END;

SELECT * FROM powerups; --Should be 12
SELECT * FROM power_details; --Should have 95 rows

