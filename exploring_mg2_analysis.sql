/* Exploratory Data Querying/Analysis using SQL */
use mario_galaxy_2;

SELECT count(eid) FROM enemies; # How many enemies are in Mario Galaxy 2? --> 119
SELECT count(trap_id) FROM traps; # How many traps are in Mario Galaxy 2? --> 45
SELECT count(boss_id) FROM bosses; # How many bosses are in Mario Galaxy 2? --> 29
SELECT count(power_id) FROM powerups; # How many powerups are in Mario Galaxy 2? --> 12
SELECT count(te_id) FROM techniques; # How many techniques can be used in Mario Galaxy 2? --> 12
SELECT count(id) FROM stars; # How many normal stars are in Mario Galaxy 2? --> 122 + 1 repeat

SELECT * FROM stars;
/* Create a view that returns id, galaxy name and star label (in one column), deaths, star bits, coins, avg_hits (avg of hit, hits2)
major sections, lives, difficulty */
CREATE OR REPLACE VIEW basic_stats AS SELECT id, CONCAT(galaxy_name, ',' , star_label) AS star, prankster, deaths, star_bits, coins, hits + hits2 / 2 AS avg_hits, major_sections, lives_found, difficulty FROM stars;
-- SELECT * FROM basic_stats;
SELECT * from basic_stats;
-- Show the counts of enemies, traps, techniques, powerups in order of most often to least often seen
SELECT count(e_id) AS num_enemies, e_id FROM enemy_details GROUP BY e_id ORDER BY num_enemies DESC; -- Top 3 were 3001, 3002, 3003
SELECT count(trap_id) AS num_traps, trap_id FROM trap_details GROUP BY trap_id ORDER BY num_traps DESC; -- Top 3 were 4000, 4006, 4001 and 4017
SELECT count(te_id) AS num_techs_used, te_id FROM technique_details GROUP BY te_id ORDER BY num_techs_used DESC; -- Top 4 were 6000, 6001, 6004, 6003
SELECT count(p_id) AS num_powers, p_id FROM powerup_details GROUP BY p_id ORDER BY num_powers DESC; -- Top 3 were 5001, 5004, 5002
 
-- Show the top 3 counts of e, tr, te, and p and their details using join
SELECT e.eid, e.is_boss, e.name, e.description, e.health, e.attack, e.attack_damage, e.other_features, num_enemies FROM enemies e JOIN 
(SELECT count(e_id) AS num_enemies, e_id FROM enemy_details GROUP BY e_id ORDER BY num_enemies DESC ) AS top_enemies
ON e.eid = top_enemies.e_id; -- Top 3 enemies seen in levels and their info --> use Python to take a deeper dive into top counts of e, tr, and p
-- Investigate which enemies, traps, and powerups appear most often and in what difficulties --> what about them yields these results?

SELECT t.trap_id, t.name, t.description, num_traps FROM traps t JOIN
(SELECT count(trap_id) AS num_traps, trap_id FROM trap_details GROUP BY trap_id ORDER BY num_traps DESC LIMIT 3) AS top_traps
ON t.trap_id = top_traps.trap_id; -- Top 3 traps seen in stars and their basic info

SELECT p.power_id, p.name, p.description, num_powers FROM powerups p JOIN
(SELECT count(p_id) AS num_powers, p_id FROM powerup_details GROUP BY p_id ORDER BY num_powers DESC LIMIT 3) AS top_powers
ON p.power_id = top_powers.p_id; -- Top 3 powerups seen and their basic info

SELECT s.id, p.power_id, p.name FROM stars s LEFT JOIN powerup_details pd ON s.id = pd.star_id LEFT JOIN powerups p ON pd.p_id = p.power_id;
SELECT te.te_id, te.name, te.uses, num_techs_used FROM techniques te JOIN
(SELECT count(te_id) AS num_techs_used, te_id FROM technique_details GROUP BY te_id ORDER BY num_techs_used DESC LIMIT 4) AS top_techs
ON te.te_id = top_techs.te_id; -- Top 4 techniques used and their basic info and uses

-- Return min, max, and average coins, star_bits, hits, deaths, lives, sections for levels in each difficulty up to Hard difficulty --> like 5 number summary in stats
CREATE OR REPLACE VIEW difficulty_stats AS SELECT count(id) AS num_stars, min(coins) AS min_coins, max(coins) AS max_coins, avg(coins) AS mean_coins, min(star_bits) AS min_bits, max(star_bits) AS max_bits,
avg(star_bits) AS mean_starbits, max(hits) AS max_damage_taken, avg(hits) AS mean_damage, max(deaths) AS max_deaths, avg(deaths) AS mean_deaths,
difficulty FROM stars GROUP BY difficulty HAVING difficulty != 'Insane';
SELECT * FROM difficulty_stats; -- Use Python to investigate more deaths and damage seen in the different diffulties

-- Group the stars table by prankster and boss column (Y/N) and study the average number of star bits, coins, hits, and deaths seen for the different groups
SELECT count(id) AS num_stars, prankster, boss, AVG(star_bits), AVG(coins), AVG(hits), AVG(deaths) FROM stars GROUP BY prankster, boss;
-- About equal and the lowest avg star bits and coins for prankster comets with and without bosses; non-pk stars yield more --> investigate further in python
-- highest avg hits and deaths for prankster comet levels and seemingly low death rate for non-pk boss levels and highest hit avg for pk levels with bosses

/* Group the stars table by star_label (use the having clause to only look at stars with star label 1 or 2 or 3 to include Prankster Comets) and compare 
hits, deaths, average number of enemies, lives_found, average number of traps, average # of major sections */
SELECT star_label, count(star_label) AS num_stars, SUM(hits) AS total_hits, SUM(deaths) AS total_deaths, AVG(total_enemies) AS avg_enemies_seen, AVG(total_traps) AS avg_traps_seen, SUM(lives_found) AS total_lives, AVG(major_sections) AS avg_sections
FROM stars s LEFT JOIN (SELECT COUNT(e_id) AS total_enemies, star_id FROM enemy_details GROUP BY star_id) AS enemy_counts 
ON s.id = enemy_counts.star_id
LEFT JOIN (SELECT COUNT(trap_id) AS total_traps, star_id FROM trap_details GROUP BY star_id) AS trap_counts
ON s.id = trap_counts.star_id
GROUP BY s.star_label HAVING s.star_label IN (1, 2, 3);
-- Think about using number of types of enemies seen in a star vs. sheer amount of enemies that show up which I did not document
-- At the end of the analysis, would adding in sheer number of enemies seen vs. types of enemies make more of a difference for determining difficulty? same for traps
-- Interestingly enough, highest hit counts and death counts with stars of label 1 --> is it because of how long the levels are, types of enemies or traps seen, or something else?

-- Figure out some EDA that involve joining the stars table and another table from (enemies, powerups, stars, techniques, traps) using the details tables
SELECT * FROM basic_stats s LEFT JOIN trap_details td ON s.id = td.star_id LEFT JOIN traps t ON td.trap_id = t.trap_id;

-- Use ranking to find most number of star bits, coins, deaths, hits, number of enemies/traps seen in levels and return the difficulties of those levels
SELECT id, star, deaths, star_bits, avg_hits, difficulty, RANK() OVER (ORDER BY star_bits DESC) FROM basic_stats; -- Top star is the very first star of the game, no other interesting behaviors/trends
SELECT id, star, deaths, coins, avg_hits, difficulty, RANK() OVER (ORDER BY coins DESC) FROM basic_stats; -- Top star is Righside Down, Star 1 and no interesting trends really
SELECT id, star, deaths, star_bits, coins, avg_hits, difficulty, RANK() OVER (ORDER BY deaths DESC) FROM basic_stats; -- Top stars are the last 2 stars in the game followed by 6 stars of relatively hard difficulty; not very many stars had deaths
SELECT id, star, deaths, star_bits, coins, avg_hits, difficulty, RANK() OVER (ORDER BY avg_hits DESC) FROM basic_stats; -- Top star is Grandmaster Galaxy, Star 1 and the other top ones are stars from Worlds 5,6,S which are very difficulty so there is def a relation between hits and difficulty to investigate further

-- Rank by types of enemies seen and traps seen
SELECT s.id, s.star, s.deaths, s.avg_hits, s.lives_found, s.difficulty, count(td.trap_id) AS num_traps, DENSE_RANK() OVER (ORDER BY count(td.trap_id) DESC) FROM basic_stats s JOIN trap_details td ON s.id = td.star_id JOIN traps t ON td.trap_id = t.trap_id GROUP BY td.star_id;
SELECT s.id, s.star, s.deaths, s.avg_hits, s.lives_found, s.difficulty, count(ed.e_id) AS num_enemies, DENSE_RANK() OVER (ORDER BY count(ed.e_id) DESC) FROM basic_stats s JOIN enemy_details ed ON s.id = ed.star_id JOIN enemies e ON ed.e_id = e.eid GROUP BY ed.star_id;
-- By itself ranking by just enemy or trap count yields very different results but most of the harder levels have the most traps and types of enemies but that alone isn't very helpful
-- Rank by more criteria (combo of enemies and traps)

SELECT *, RANK() OVER (ORDER BY health DESC, attack_damage DESC) FROM enemies WHERE is_boss = 'N' ORDER BY e_rank;
SELECT * FROM enemies WHERE e_rank IS NULL AND is_boss = 'N' ORDER BY attack_damage DESC;
-- ALTER TABLE enemies ADD COLUMN e_rank INT;
UPDATE enemies SET e_rank = 86 WHERE eid = 3078;
UPDATE enemies SET e_rank = 87 WHERE eid = 3085;
UPDATE enemies SET e_rank = 88 WHERE eid = 3008;
UPDATE enemies SET e_rank = 89 WHERE eid = 3077;
UPDATE enemies SET e_rank = 90 WHERE eid = 3116;
UPDATE enemies SET e_rank = 91 WHERE eid = 3115;
UPDATE enemies SET e_rank = 92 WHERE eid = 3030; # Had to rank the enemies to incorporate them numerically into the difficulty model
# Ranked by enemy weaknesses, features, health, attacks and damage, etc.
-- Rank by bosses table for some criteria (attack damage, health, difficulty of the star its in, if it has a weakness)
UPDATE bosses SET weakness = 'None' WHERE weakness = '';
SELECT s.star, s.avg_hits, s.deaths, s.difficulty, b.name, b.health, b.attack_damage, b.weakness, RANK() OVER (ORDER BY b.health DESC, b.attack_damage DESC, CASE b.weakness WHEN 'None' THEN 1 ELSE 0 END DESC) FROM basic_stats s JOIN bosses b ON s.id = b.star_id;

SELECT s.star, s.avg_hits, s.deaths, s.difficulty, b.name, b.health, b.attack_damage, b.weakness, ROW_NUMBER() OVER (ORDER BY b.health DESC, b.attack_damage DESC, CASE b.weakness WHEN 'None' THEN 1 ELSE 0 END DESC) FROM basic_stats s JOIN bosses b ON s.id = b.star_id;
-- Use ROW_NUMBER so you have different values of 'rank' for each boss

ALTER TABLE traps ADD COLUMN t_rank INT;
SELECT * from traps;

UPDATE traps SET t_rank = 41 WHERE name = 'Bomp'; # Rank the traps by if they cause death, damage, or just get in Mario's way
UPDATE traps SET t_rank = 42 WHERE name = 'Moving Walls';
UPDATE traps SET t_rank = 43 WHERE name = 'Water Shooter';
UPDATE traps SET t_rank = 44 WHERE name = 'Tall Tower';

ALTER TABLE bosses ADD COLUMN b_rank INT;
SELECT * FROM bosses ORDER BY b_rank;

UPDATE bosses SET b_rank = 27 WHERE name = 'Sorbetti'; # Rank bosses in a similar way to enemies
UPDATE bosses SET b_rank = 28 WHERE name = 'Peewee Piranha';
UPDATE bosses SET b_rank = 29 WHERE name = 'Bugaboom';
UPDATE bosses SET b_rank = 30 WHERE name = 'Mandibug Stack';

ALTER TABLE powerups ADD COLUMN p_rank INT;
SELECT * FROM powerups ORDER BY p_rank;

UPDATE powerups SET p_rank = 9 WHERE name = 'Dash Pepper'; # Rank the powerups from most OP/useful to least OP/useful
UPDATE powerups SET p_rank = 10 WHERE name = 'Bulb Berry';
UPDATE powerups SET p_rank = 11 WHERE name = 'Spring';
UPDATE powerups SET p_rank = 12 WHERE name = 'Bee';