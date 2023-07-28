-- Inserting techniques and their details- similar to traps and powerups

CREATE SEQUENCE tech_sequence
START WITH 6000
INCREMENT BY 1;

CREATE TABLE techniques (
    id INT DEFAULT tech_sequence.nextval NOT NULL PRIMARY KEY,
    name VARCHAR2(30) NOT NULL,
    uses VARCHAR2(100)
);

CREATE SEQUENCE tdet_sequence
START WITH 7000
INCREMENT BY 1;

CREATE TABLE tech_details (
    id INT DEFAULT tdet_sequence.nextval NOT NULL PRIMARY KEY,
    tech_id NUMBER(4),
    tech_star_id NUMBER(4)
);

CREATE OR REPLACE PROCEDURE insert_tech (
    tech_name VARCHAR2) AS
    
BEGIN
    INSERT INTO techniques(name) VALUES (tech_name);
END;

CREATE OR REPLACE PROCEDURE insert_techdets (
    tech NUMBER,
    star NUMBER) AS

BEGIN
    INSERT INTO tech_details(tech_id, tech_star_id) VALUES(tech, star);
    
END;

--Insert techniques into tech table similar to traps and powerups
CREATE OR REPLACE PROCEDURE extract_tech ( --Will find new traps in the trap string and add them to the traps table
    tech_string VARCHAR2
    ) AS

    tech VARCHAR2(30);
    cnt INT;
BEGIN
    FOR i IN (SELECT REGEXP_SUBSTR(tech_string, '[^,]+', 1, level) AS parts FROM dual
                CONNECT BY REGEXP_SUBSTR(tech_string, '[^,]+', 1, level) IS NOT NULL)
            
                LOOP
                    SELECT TRIM(i.parts) INTO tech FROM dual;
                    SELECT COUNT(*) INTO cnt FROM techniques WHERE name = tech;
                    IF cnt = 0 THEN
                        insert_tech(tech);
                        dbms_output.put_line(tech || ' Inserted');
                    END IF;
                    
                END LOOP;
END;

DECLARE
    cursor tech_cursor IS SELECT id, techniques FROM stars; --Return the star_id, enemies, and new enemies string
    techn stars.techniques%TYPE;
    
BEGIN
    FOR rec IN tech_cursor
    
    LOOP
        IF rec.techniques <> 'None' THEN
            SELECT rec.techniques INTO techn FROM dual;
            extract_tech(techn);
            
        ELSE CONTINUE;
        END IF;
    END LOOP;
END;

--Insert technique details (procedure and program) similar to traps and powerups
DECLARE
    CURSOR techdets_cursor IS SELECT id, techniques FROM stars;
    star_id stars.id%TYPE;
    tech_id techniques.id%TYPE;
    tech_name stars.techniques%TYPE;
    
BEGIN
    FOR rec IN techdets_cursor
    
    LOOP
        IF rec.techniques <> 'None' THEN
            FOR i IN (SELECT REGEXP_SUBSTR(rec.techniques, '[^,]+', 1, level) AS parts FROM dual
                CONNECT BY REGEXP_SUBSTR(rec.techniques, '[^,]+', 1, level) IS NOT NULL)
            
                LOOP
                BEGIN
                    SELECT TRIM(i.parts) INTO tech_name FROM dual; --Techniques are separated by a comma and space so we need to trim the white space
                    SELECT rec.id INTO star_id FROM dual;
                    SELECT id INTO tech_id FROM techniques WHERE name = tech_name;
                    dbms_output.put_line('This worked for ' || tech_name);
                    insert_techdets(tech_id, star_id);
                EXCEPTION --Catch the exception during the loop so it can add the new trap to the table and continue iterating
                    WHEN no_data_found THEN --If we find a new trap in the traps column we need to add that trap to the traps table
                        insert_tech(tech_name);
                        SELECT id INTO tech_id FROM techniques WHERE name = tech_name;
                        dbms_output.put_line('Adding a new trap: ' || tech_name);
                        insert_techdets(tech_id, star_id);
                END;
                END LOOP;
        ELSE
            CONTINUE;
        END IF;
            
    END LOOP;

END;

DELETE FROM techniques WHERE id = 6012;
DELETE FROM tech_details WHERE tech_id = 6012;

SELECT * FROM techniques; --Should be 12
SELECT * FROM tech_details; --Should be 320 rows