CREATE SEQUENCE star_sequence
START WITH 1000
INCREMENT BY 1;

CREATE TABLE stars ( -- create the table that stars/level data will go into --> couldn't import timestamp in the format we wanted so had to be CHAR
    id NUMBER(4) DEFAULT star_sequence.NEXTVAL NOT NULL PRIMARY KEY,
    name VARCHAR2(100) NOT NULL,
    pk CHAR(1) NOT NULL,
    deaths INT,
    star_bits INT,
    coins INT,
    hits INT,
    hits2 INT,
    boss_cat CHAR(1) NOT NULL,
    boss_name VARCHAR2(80) NOT NULL,
    enemies VARCHAR2(150),
    new_enemies VARCHAR2(100),
    traps VARCHAR2(120),
    comet_medal CHAR(1),
    completion CHAR(19) NOT NULL,
    powerups VARCHAR2(100),
    major_sections INT NOT NULL,
    lives INT,
    techniques VARCHAR2(80),
    difficulty VARCHAR2(20) NOT NULL
);

-- Used the right click on the created table name --> import data and imported the data via CSV file locally saved

INSERT INTO STARS (NAME, PK, DEATHS, STAR_BITS, COINS, HITS, HITS2, BOSS_CAT, BOSS_NAME, ENEMIES, NEW_ENEMIES, TRAPS, COMET_MEDAL, COMPLETION, POWERUPS, MAJOR_SECTIONS, LIVES, TECHNIQUES, DIFFICULTY) VALUES ('Grandmaster Galaxy, Star 2','Y',3,3,0,3,NULL,'N','None','Choppa, Targeting Bullet Bill, Mobile Sentry Beam, Ring Beam, Octopus, Hammer Bro, Flomp, Boomerang Bro','None','Spike Ball, Electric Fence, Disappearing Platform, Moving Electric Fence','N','2022-12-10 17:15:29','Cloud Flower',6,2,'Yoshi, Spin, Jump and Spin, Backflip, Long Jump','Insane');
select * from stars; -- One column in this row is supposed to be null but was imported as '-' so the import didn't work for a second so have to insert manually

CREATE SEQUENCE boss_sequence
START WITH 2000
INCREMENT BY 1;

-- create the bosses table
CREATE TABLE bosses (
    id NUMBER(4) DEFAULT boss_sequence.nextval NOT NULL PRIMARY KEY,
    name VARCHAR2(20) NOT NULL,
    star_id NUMBER(4) NOT NULL, --which star does this boss FIRST appear in?
    description VARCHAR2(100),
    health INT,
    attack VARCHAR2(50),
    attack_damage INT,
    weakness VARCHAR2(50),
    other_features VARCHAR2(100)
);

--Use PL/SQL to extract the bosses from the stars table and add a row to the bosses table for each one
CREATE OR REPLACE PROCEDURE insert_bosses ( -- Inserts a row for that boss extracted into the bosses table and skips the boss blitz level with multiple bosses
    star_num NUMBER,
    boss_name VARCHAR2) AS

    e_insert EXCEPTION;
    pragma exception_init(e_insert, -12899);
BEGIN
    INSERT INTO bosses(name, star_id) VALUES (boss_name, star_num);
    
EXCEPTION 
    WHEN e_insert 
    THEN dbms_output.put_line('Row Skipped because we have multiple bosses');
END;

DECLARE
    cursor stars_cursor IS SELECT id, boss_cat, boss_name FROM stars; -- This cursor will return id, boss_cat, boss_name columns from the table 
    star_id STARS.id%TYPE;
    boss STARS.boss_name%TYPE;
    
BEGIN
    FOR rec IN stars_cursor
    LOOP
        IF rec.boss_cat = 'Y' --If the level contains a boss
        THEN   
            star_id := rec.id;
            boss := rec.boss_name;
            insert_bosses(star_id, boss); --insert the boss and star_id in the bosses table via this procedure
        ELSE
            CONTINUE;  
        END IF;
    END LOOP;
END;
    
SELECT * FROM bosses; --Should have 25 bosses

--Create the enemies table
CREATE SEQUENCE e_sequence
START WITH 3000
INCREMENT BY 1;

CREATE TABLE enemies (
    eid NUMBER(4) DEFAULT e_sequence.nextval NOT NULL PRIMARY KEY,
    name VARCHAR2(25) NOT NULL,
    description VARCHAR2(50),
    health INT,
    attack VARCHAR2(20),
    attack_damage INT,
    weakness VARCHAR(50),
    other_features VARCHAR(100)
);

CREATE SEQUENCE basic_sequence
START WITH 1
INCREMENT BY 1;

--Create the enemy details table for star_id
CREATE TABLE enemy_details (
    id INT DEFAULT basic_sequence.nextval NOT NULL PRIMARY KEY,
    eid NUMBER(4) NOT NULL,
    star_id NUMBER(4) NOT NULL
);

--Procedure to add new enemies to the enemies and enemy details table
CREATE OR REPLACE PROCEDURE insert_enemies (
    ename VARCHAR2) AS
    
BEGIN
    INSERT INTO enemies(name) VALUES (ename);

END;

--Procedure to add enemies found in multiple levels to the enemy_details table
CREATE OR REPLACE PROCEDURE insert_edetails (
    eid_d NUMBER,
    star NUMBER) AS
    
BEGIN
    INSERT INTO enemy_details(eid, star_id) VALUES (eid_d, star);

END;

--Write code to extract the new enemies from each star, add those as rows to the enemies table then add their info (enemy id, star id) to the enemy_details table

DECLARE
    cursor enemy_cursor IS SELECT id, new_enemies FROM stars; --Return the star_id, enemies, and new enemies string
    enemy stars.enemies%TYPE;

BEGIN
    FOR rec IN enemy_cursor --Return each row in the table one by one using the cursor
    
    LOOP
        IF rec.new_enemies <> 'None' 
        
        THEN
            FOR i IN (SELECT REGEXP_SUBSTR(rec.new_enemies, '[^,]+', 1, level) AS parts FROM dual --Extracts the individual enemies from the comma delimited string using regular expresions
            CONNECT BY REGEXP_SUBSTR(rec.new_enemies, '[^,]+', 1, level) IS NOT NULL)
            
            LOOP
                SELECT TRIM(i.parts) INTO enemy FROM dual; --Enemies are separated by a comma and space so we need to trim the white space
                insert_enemies(enemy);
                dbms_output.put_line('Enemy inserted: ' || enemy);
                
            END LOOP;
        ELSE
            CONTINUE; --Skip the record iteration if there are no new enemies in that star to add
            
        END IF;
    END LOOP;
END;

DECLARE
    cursor enemy_cursor2 IS SELECT id, enemies FROM stars; --Return the star_id, enemies, and new enemies string
    enemy stars.enemies%TYPE;
    enemy_id enemies.eid%TYPE;
    star_id stars.id%TYPE;

BEGIN
    FOR rec IN enemy_cursor2
    
    LOOP
        IF rec.enemies <> 'None' THEN
            FOR i IN (SELECT REGEXP_SUBSTR(rec.enemies, '[^,]+', 1, level) AS parts FROM dual
                CONNECT BY REGEXP_SUBSTR(rec.enemies, '[^,]+', 1, level) IS NOT NULL)
            
                LOOP
                BEGIN
                    SELECT TRIM(i.parts) INTO enemy FROM dual; --Enemies are separated by a comma and space so we need to trim the white space
                    SELECT rec.id INTO star_id FROM dual;
                    SELECT eid INTO enemy_id FROM enemies WHERE name = enemy;
                    dbms_output.put_line('This worked for ' || enemy);
                    insert_edetails(enemy_id, star_id);
                EXCEPTION --Catch the exception during the loop so it can add the new enemy to the table and continue iterating
                    WHEN no_data_found THEN --If we find a new enemy in the enemies column and not the new_enemies column we need to add that enemy to the enemies table
                        insert_enemies(enemy);
                        SELECT eid INTO enemy_id FROM enemies WHERE name = enemy;
                        dbms_output.put_line('Adding a new enemy: ' || enemy);
                        insert_edetails(enemy_id, star_id);
                END;
                END LOOP;
        ELSE
            CONTINUE;
        END IF;
            
    END LOOP;

END;

SELECT * FROM enemies; --Should have 122 enemies (need to delete Spiky Plant)
SELECT * FROM enemy_details; --Should have 479 rows