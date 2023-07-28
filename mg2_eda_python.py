# EDA with Python/PySpark

import mysql.connector as mysql
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sb

conn = mysql.connect(user = 'root', password = 'yayAtHa3$', host = '127.0.0.1', database = 'mario_galaxy_2') # Connect to the Database
cursor = conn.cursor()

query = 'SELECT e.eid, e.is_boss, e.name, e.description, e.health, e.attack, e.attack_damage, e.other_features, num_enemies FROM enemies e JOIN (SELECT count(e_id) AS num_enemies, e_id FROM enemy_details GROUP BY e_id ORDER BY num_enemies DESC) AS top_enemies ON e.eid = top_enemies.e_id'
cursor.execute(query) # query is in string and use the execute method to run the query and then we can save the results in a df
top_enemies_df = pd.DataFrame(cursor.fetchall(), columns = ['enemy_id', 'is_boss', 'name', 'description', 'health', 'attack', 'attack_damage', 'other_features', 'num_enemies'])
# Write a query/expand on the above query to look at what difficulties contain the most common/least common enemies and traps
# Incorporate top counts of enemies and traps and powerups and see the distribution of difficulties (what difficulties contain those top enemies --> need further investigation as different distributions for each top enemy, trap)

query = "SELECT s.difficulty, e.name, s.id FROM stars s LEFT JOIN enemy_details ed ON s.id = ed.star_id LEFT JOIN enemies e ON ed.e_id = e.eid WHERE e.name = 'Goomba'"
cursor.execute(query)
goomba_df = pd.DataFrame(cursor.fetchall(), columns = ['difficulty', 'enemy_name', 'star_id'])
goomba_diff_plot = sb.displot(goomba_df['difficulty'], kde = True)

query = "SELECT s.difficulty, e.name, s.id FROM stars s LEFT JOIN enemy_details ed ON s.id = ed.star_id LEFT JOIN enemies e ON ed.e_id = e.eid WHERE e.name = 'Octoomba'"
cursor.execute(query)
octo_df = pd.DataFrame(cursor.fetchall(), columns = ['difficulty', 'enemy_name', 'star_id'])
octo_diff_plot = sb.displot(octo_df['difficulty'], kde = True)

query = "SELECT s.difficulty, e.name, s.id FROM stars s LEFT JOIN enemy_details ed ON s.id = ed.star_id LEFT JOIN enemies e ON ed.e_id = e.eid WHERE e.name = 'Paragoomba'"
cursor.execute(query)
para_df = pd.DataFrame(cursor.fetchall(), columns = ['difficulty', 'enemy_name', 'star_id'])
paragoomba_diff_plot = sb.displot(para_df['difficulty'], kde = True)
plt.show()

# Do the same for top traps
query = "SELECT s.difficulty, t.name, s.id FROM stars s LEFT JOIN trap_details td ON s.id = td.star_id LEFT JOIN traps t ON td.trap_id = t.trap_id WHERE t.name = 'Black Hole'"
cursor.execute(query)
black_hole_df = pd.DataFrame(cursor.fetchall(), columns = ['difficulty', 'trap_name', 'star_id'])
hole_diff_plot = sb.displot(black_hole_df['difficulty'], kde = True)

query = "SELECT s.difficulty, t.name, s.id FROM stars s LEFT JOIN trap_details td ON s.id = td.star_id LEFT JOIN traps t ON td.trap_id = t.trap_id WHERE t.name = 'Lava'"
cursor.execute(query)
lava_df = pd.DataFrame(cursor.fetchall(), columns = ['difficulty', 'trap_name', 'star_id'])
lava_diff_plot = sb.displot(lava_df['difficulty'], kde = True)

query = "SELECT s.difficulty, t.name, s.id FROM stars s LEFT JOIN trap_details td ON s.id = td.star_id LEFT JOIN traps t ON td.trap_id = t.trap_id WHERE t.name = 'Spiky Plant'"
cursor.execute(query)
spiky_df = pd.DataFrame(cursor.fetchall(), columns = ['difficulty', 'trap_name', 'star_id'])
spiky_diff_plot = sb.displot(spiky_df['difficulty'], kde = True)
plt.show()

# Distributions of top traps and enemies are very different so we need a more elaborate approach to rank them by difficulty

query= 'SELECT count(id) AS num_stars, prankster, boss, AVG(star_bits), AVG(coins), AVG(hits), AVG(deaths), AVG(major_sections) FROM stars GROUP BY prankster, boss'
# Graph the relationship between the four groups for the different stats, especially hits and deaths and number of major sections
cursor.execute(query)

star_stats_df = pd.DataFrame(cursor.fetchall(), columns = ['num_stars', 'is_prankster', 'is_boss', 'mean_bits', 'mean_coins', 'mean_hits', 'mean_deaths', 'mean_sections'])

star_stats_df['prankster, boss'] = star_stats_df['is_prankster'] + star_stats_df['is_boss']
for i in ['mean_bits', 'mean_coins', 'mean_hits', 'mean_deaths', 'mean_sections']:
    star_stats_df[i] = star_stats_df[i].astype(float) # For some reason the returned aggregate data from the query is not a float type
star_stats_df.plot(x = 'prankster, boss', y = ['mean_hits', 'mean_deaths', 'mean_sections'], kind = 'bar', stacked = False)
star_stats_df.plot(x = 'prankster, boss', y = ['mean_bits', 'mean_coins'], kind = 'bar', stacked = False)
plt.show() # Shows two bar charts on the same graph by inputting a list into y to include multiple columns (must be on the same scale)
# The distributions of the 4 groups are very different --> should include is_boss and is_prankster in model
# Also we see more sections in non-prankster comet levels and in pk levels we see sligtly fewer sections in boss pks

query = 'SELECT star_label, count(star_label), SUM(hits), SUM(deaths), AVG(total_enemies), AVG(total_traps), SUM(lives_found), AVG(major_sections) FROM stars s LEFT JOIN (SELECT COUNT(e_id) AS total_enemies, star_id FROM enemy_details GROUP BY star_id) AS enemy_counts ON s.id = enemy_counts.star_id LEFT JOIN (SELECT COUNT(trap_id) AS total_traps, star_id FROM trap_details GROUP BY star_id) AS trap_counts ON s.id = trap_counts.star_id GROUP BY s.star_label HAVING s.star_label IN (1, 2, 3)'
cursor.execute(query)
label_df = pd.DataFrame(cursor.fetchall(), columns = ['label', 'num_stars', 'total_hits', 'total_deaths', 'avg_enemies_seen', 'avg_traps_seen', 'total_lives', 'avg_sections'])
# cursor.fetchall() method returns a list of tuples that can directly be turned into rows of a df

for i in ['avg_enemies_seen', 'avg_traps_seen', 'avg_sections']:
    label_df[i] = label_df[i].astype(float)

mean_plot = label_df.plot(x = 'label', y = ['avg_enemies_seen', 'avg_traps_seen', 'avg_sections'], kind = 'bar')
plt.show()

query = 'SELECT s.id, s.star, s.deaths, s.avg_hits, s.lives_found, s.difficulty, count(td.trap_id), DENSE_RANK() OVER (ORDER BY count(td.trap_id) DESC) FROM basic_stats s JOIN trap_details td ON s.id = td.star_id JOIN traps t ON td.trap_id = t.trap_id GROUP BY td.star_id'
cursor.execute(query)
trap_counts_ordering = pd.DataFrame(cursor.fetchall(), columns = ['star_id', 'star', 'deaths', 'avg_hits', 'lives_found', 'difficulty', 'num_traps', 'rank'])

# Save enemies, traps, and powerups and their details into separate dataframes
query = 'SELECT s.id, s.star, s.prankster, s.deaths, s.star_bits, s.coins, s.avg_hits, s.major_sections, s.lives_found, td.trap_id, t.name, t.description, s.difficulty FROM basic_stats s LEFT JOIN trap_details td ON s.id = td.star_id LEFT JOIN traps t ON td.trap_id = t.trap_id'
cursor.execute(query)
traps_df = pd.DataFrame(cursor.fetchall(), columns = ['star_id', 'star', 'is_prankster', 'deaths', 'star bits', 'coins', 'average hits', 'major sections', 'lives found', 'trap_id', 'trap name', 'trap description', 'star difficulty'])

query = 'SELECT s.id, s.star, s.prankster, s.deaths, s.star_bits, s.coins, s.avg_hits, s.major_sections, s.lives_found, ed.e_id, e.name, e.is_boss, e.health, e.attack, e.attack_damage, e.weakness, s.difficulty FROM basic_stats s LEFT JOIN enemy_details ed ON s.id = ed.star_id LEFT JOIN enemies e ON ed.e_id = e.eid'
cursor.execute(query)
enemies_df = pd.DataFrame(cursor.fetchall(), columns = ['star_id', 'star', 'is_prankster', 'deaths', 'star bits', 'coins', 'average hits', 'major sections', 'lives found', 'enemy_id', 'enemy name', 'Is Boss?', 'health', 'attacks', 'attack_damage', 'weakness', 'star difficulty'])

query = 'SELECT s.id, s.star, s.prankster, s.deaths, s.star_bits, s.coins, s.avg_hits, s.major_sections, s.lives_found, pd.p_id, p.name, p.description, s.difficulty FROM basic_stats s LEFT JOIN powerup_details pd ON s.id = pd.star_id LEFT JOIN powerups p ON pd.p_id = p.power_id'
cursor.execute(query)
powers_df = pd.DataFrame(cursor.fetchall(), columns = ['star_id', 'star', 'is_prankster', 'deaths', 'star bits', 'coins', 'average hits', 'major sections', 'lives found', 'powerup_id', 'powerup name', 'powerup description', 'star difficulty'])

# Build some charts/analysis on stats of coins, star bits, deaths, avg_hits, lives found, and sections for different difficulties (1 for each stat)
query = 'SELECT * FROM basic_stats'
cursor.execute(query)
stars_df = pd.DataFrame(cursor.fetchall(), columns = ['star_id', 'star', 'is_prankster', 'deaths', 'star_bits', 'coins', 'avg_hits', 'num_sections', 'lives', 'difficulty'])
stars_df = stars_df[stars_df['avg_hits'].isna() != True]

query = 'SELECT FLOOR(AVG(coins)), difficulty FROM basic_stats GROUP BY difficulty'
cursor.execute(query)
coins_v_diff = pd.DataFrame(cursor.fetchall(), columns = ['avg_coins', 'difficulty'])
coins_plot = coins_v_diff.plot.bar(x = 'difficulty', y = 'avg_coins', stacked = True)
# Highest avg of coins for Easy, about the same for Easy-Medium through Hard, lowest for Harder then Insane

query = 'SELECT FLOOR(AVG(star_bits)), difficulty FROM basic_stats GROUP BY difficulty'
cursor.execute(query)
bits_v_diff = pd.DataFrame(cursor.fetchall(), columns = ['avg_bits', 'difficulty'])
starbits_plot = bits_v_diff.plot.bar(x = 'difficulty', y = 'avg_bits', stacked = True)
# Similar trends to coins vs. difficulty, where the average decreases as the difficulty increases

query = 'SELECT CEIL(AVG(major_sections)), difficulty FROM basic_stats GROUP BY difficulty'
cursor.execute(query)
secs_v_diff = pd.DataFrame(cursor.fetchall(), columns = ['avg_sections', 'difficulty'])
sections_plot = secs_v_diff.plot.bar(x = 'difficulty', y = 'avg_sections', stacked = True)
# About equal average sections but there are a little bit more as difficulty increases past Hard

query = 'SELECT CEIL(AVG(lives_found)), difficulty FROM basic_stats GROUP BY difficulty'
cursor.execute(query)
lives_v_diff = pd.DataFrame(cursor.fetchall(), columns = ['avg_lives', 'difficulty'])
sections_plot = lives_v_diff.plot.bar(x = 'difficulty', y = 'avg_lives', stacked = True)
# About equal average lives seen, no really interesting trends here

query = 'SELECT CEIL(AVG(deaths)), difficulty FROM basic_stats GROUP BY difficulty'
cursor.execute(query)
deaths_v_diff = pd.DataFrame(cursor.fetchall(), columns = ['avg_deaths', 'difficulty'])
deaths_plot = deaths_v_diff.plot.bar(x = 'difficulty', y = 'avg_deaths', stacked = True)
# no interesting trends, clearly Insane has the highest deaths and about 0 for the other difficulties

query = 'SELECT CEIL(AVG(avg_hits)), difficulty FROM basic_stats GROUP BY difficulty'
cursor.execute(query)
hits_v_diff = pd.DataFrame(cursor.fetchall(), columns = ['avg_hits', 'difficulty'])
hits_plot = hits_v_diff.plot.bar(x = 'difficulty', y = 'avg_hits', stacked = True)
# Only a few differences in avg_hits and a bit more as you get to the highest difficulties and 0 for Insane
# because it was the Perfect Run

# Graph the relationship between avg_hits and major sections for each difficulty
hits_secs_joint = sb.jointplot(data = stars_df, x = 'avg_hits', y = 'num_sections', hue = 'difficulty', kind = 'scatter')
#plt.show() # Shows not a very distinct trend in changes in sections vs. hits for each difficulty (looks very all over the place so they don't appear to be related)

# Find a way to rank traps, enemies, and powerups using Pandas/Python (that will be part of the ETL) --> EDA to see how to rank them
    # Manually did it in MySQL but for the ETL the numerical incorporation of enemies, traps, powerups we will take the average rank for all things found in each level
    # Also need to add the boss ranks to the enemies table then add 29 to all other enemies' ranks so bosses are above enemies
# Look at the distribution of coins collected, avg_hits, and if there was a mushroom powerup for the different difficulties
query = 'SELECT s.id, p.power_id, p.name FROM stars s LEFT JOIN powerup_details pd ON s.id = pd.star_id LEFT JOIN powerups p ON pd.p_id = p.power_id'
cursor.execute(query)
power_df = pd.DataFrame(cursor.fetchall(), columns = ['star_id', 'powerup_id', 'powerup_name'])
power_df['has_mushroom'] = power_df['powerup_name'].apply(lambda x: 'Y' if x == 'Life Mushroom' else 'N')
mushroom_df = power_df[power_df['powerup_name'] == 'Life Mushroom']
stars_copy = stars_df.set_index('star_id')
stars_copy['has_mushroom'] = 'N'
for i in mushroom_df['star_id']:
    stars_copy.loc[i, 'has_mushroom'] = 'Y'

stars_copy['avg_hits'] = stars_copy['avg_hits'].astype(float)
mushroom_plot = sb.boxplot(x = 'difficulty', y = 'coins', data = stars_copy, hue = 'has_mushroom')
mushroom_plot2 = sb.boxplot(x = 'difficulty', y = 'avg_hits', data = stars_copy, hue = 'has_mushroom') # Distributions among the different difficulties are not much different and some don't make sense really so mushrooms and avg_hits don't have much of an association
hits_coins_plot = sb.scatterplot(data = stars_copy, x = 'coins', y = 'avg_hits', hue = 'difficulty') # No trend/association really, although there are distinctly higher avg_hits and way less coins for expert, hard, harder levels (very few points to look at though) 
#and way more coins and less avg_hits for easy levels and less coins for medium and medium-hard levels but still very little hits
plt.show()

# Look at lives found, deaths, and difficulty of stars and see if there are any interesting trends
stars_copy2 = stars_df
lives_deaths_plot = sb.scatterplot(data = stars_copy2, x = 'lives', y = 'deaths', hue = 'difficulty')
#plt.show() # Plot showed very few points but you can get anywhere from 0 to 5 lives in a single level and I only die a couple times
# so EDA with deaths doesn't give me much but a level must be pretty hard if I die in it because it is so rare
# No really interesting trends to notice but lives can be helpful if we know how many places it is possible to die in a level maybe

# Save the updated MySQL data tables as a CSV and then upload to S3
# Connect the data in S3 to Glue Data Catalog and run a Glue PySpark job to perform ETL on the data and save to S3
# Create a Lambda function that will trigger once the updated data hits S3 and send it to Redshift
# In the DW, build the model somehow and connect to a BI tool with the results