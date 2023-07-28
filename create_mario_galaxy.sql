DROP DATABASE IF EXISTS mario_galaxy_2;

CREATE DATABASE mario_galaxy_2;

use mario_galaxy_2;

CREATE TABLE stars ( /* Mario Galaxy 2 stars */
	id INT PRIMARY KEY,
    galaxy_name VARCHAR(40) NOT NULL, /* What galaxy is the star in */
    star_label VARCHAR(5) NOT NULL, /* Star Number or Label */
    prankster ENUM('Y', 'N') NOT NULL, /* If the star is a prankster comet star */
    deaths INT,
    star_bits INT,
    coins INT,
    hits INT,
    hits2 INT,
    boss ENUM('Y','N') NOT NULL, /* Is there a boss in this star? */
    new_enemies VARCHAR(200), /* A list of enemies that are new */
    comet_medal ENUM('Y', 'N') NOT NULL, /* Does the star have a comet medal to collect? */
    completion_timestamp TIMESTAMP, /* When the star was completed */
    major_sections INT NOT NULL, /* How many parts to the level */
    lives_found INT, /* How many 1-Ups Exist in the level */
    difficulty ENUM('Very Easy','Easy','Easy-Medium','Medium','Medium-Hard','Hard','Harder','Expert', 'Insane')
);

CREATE TABLE bosses ( /* Mario Galaxy 2 bosses */
	boss_id INT NOT NULL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    description VARCHAR(200), /* Describes what kind of creature the boss is */
	star_id INT, /* Describes which level the boss is found in */
    health INT,
    attack VARCHAR(200), /* Describes how boss attacks mario */
    attack_damage INT, /* Describes how much damage the boss deals in one hit */
    weakness VARCHAR(200), /* Describes how to defeat it/An obvious weakness of the boss */
    other_features VARCHAR(500), /* Describes other unique features of the boss */
    FOREIGN KEY (star_id) REFERENCES stars(id)
    
);

CREATE TABLE enemies ( /* Mario Galaxy 2 enemies, includes bosses */
	eid INT NOT NULL PRIMARY KEY,
    is_boss ENUM('Y', 'N') NOT NULL,
    name VARCHAR(100) NOT NULL,
    description VARCHAR(200),
    health VARCHAR(5),
    attack VARCHAR(200),
    attack_damage INT,
    weakness VARCHAR(200),
    other_features VARCHAR(500)

);

CREATE TABLE enemy_details (
	id INT NOT NULL PRIMARY KEY,
    e_id INT NOT NULL,
    star_id INT NOT NULL,
    FOREIGN KEY (e_id) REFERENCES enemies(eid),
    FOREIGN KEY (star_id) REFERENCES stars(id)

);


CREATE TABLE traps ( /* Mario galaxy 2 traps */
	trap_id INT NOT NULL PRIMARY KEY,
	name VARCHAR(100) NOT NULL,
    description VARCHAR(200) /* Briefly describes what the trap is */
    
);

CREATE TABLE trap_details ( /* Mario Galaxy 2 trap details */
	id INT NOT NULL PRIMARY KEY,
    trap_id INT NOT NULL,
    star_id INT NOT NULL,
    FOREIGN KEY (trap_id) REFERENCES traps(trap_id),
    FOREIGN KEY (star_id) REFERENCES stars(id)

);

CREATE TABLE powerups ( /* Mario Galaxy powerups */
	power_id INT NOT NULL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    description VARCHAR(500) /* Briefly describes what the powerup does */
	
);

CREATE TABLE powerup_details (
	id INT NOT NULL PRIMARY KEY,
    p_id INT NOT NULL,
    star_id INT NOT NULL,
    FOREIGN KEY (p_id) REFERENCES powerups(power_id),
    FOREIGN KEY (star_id) REFERENCES stars(id)

);

CREATE TABLE techniques ( /* What Mario can do in Mario Galaxy 2*/
	te_id INT NOT NULL PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    uses VARCHAR(200)

);

CREATE TABLE technique_details (
	id INT NOT NULL PRIMARY KEY,
    te_id INT NOT NULL,
    star_id INT NOT NULL,
    FOREIGN KEY (te_id) REFERENCES techniques(te_id),
    FOREIGN KEY (star_id) REFERENCES stars(id)

);

/*CREATE TABLE prankster_comets ( Describes Prankster Comet Levels in Mario Galaxy 2 
	p_id INT AUTO_INCREMENT PRIMARY KEY, 
    star_id INT, 
    description VARCHAR(200), Briefly descibes the difference between the regular level and this one 
    FOREIGN KEY (p_id) REFERENCES stars(id)
    
); */
    