import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node mario-galaxy2-source
mariogalaxy2_stars = glueContext.create_dynamic_frame.from_catalog( # Stars Table
    database="mario-galaxy-2",
    table_name="galaxy2-stars_data",
    transformation_ctx="mariogalaxy2_stars",
)

mariogalaxy2_bosses = glueContext.create_dynamic_frame.from_catalog( # Bosses Table
    database = "mario-galaxy-2",
    table_name = "galaxy2-bosses_table_csv",
    transformation_ctx = "mariogalaxy2_bosses",
    )

mariogalaxy2_enemies = glueContext.create_dynamic_frame.from_catalog( # Enemies Table
    database = "mario-galaxy-2",
    table_name = "galaxy2-enemies_table_csv",
    transformation_ctx = "mariogalaxy2_enemies",
    )
    
mariogalaxy2_powerups = glueContext.create_dynamic_frame.from_catalog ( # Powerups table
    database = "mario-galaxy-2",
    table_name = "galaxy2-powerups_table_csv",
    transformation_ctx = "mariogalaxy2_powerups",
    )
    
mariogalaxy2_traps = glueContext.create_dynamic_frame.from_catalog( # Traps Table
    database = "mario-galaxy-2",
    table_name = "galaxy2-traps_table_csv",
    transformation_ctx = "mariogalaxy2_traps",
    )
    
mariogalaxy2_enemydets = glueContext.create_dynamic_frame.from_catalog( # Enemy Details Table
    database = "mario-galaxy-2",
    table_name = "galaxy2-enemy_details_table_csv",
    transformation_ctx = "mariogalaxy2_enemydets",
    )

mariogalaxy2_trapdets = glueContext.create_dynamic_frame.from_catalog( # Trap Details Table
    database = "mario-galaxy-2",
    table_name = "galaxy2-trap_details_table_csv",
    transformation_ctx = "mariogalaxy2_trapdets",
    )
    
mariogalaxy2_powerupdets = glueContext.create_dynamic_frame.from_catalog( # Powerups Details Table
    database = "mario-galaxy-2",
    table_name = "galaxy2-powerup_details_table_csv",
    transformation_ctx = "mariogalaxy2_powerupdets",
    )
    
stars_df = mariogalaxy2_stars.toDF()
enemies_df = mariogalaxy2_enemies.toDF()
enemy_dets = mariogalaxy2_enemydets.toDF()
traps_df = mariogalaxy2_traps.toDF()
trap_dets = mariogalaxy2_trapdets.toDF()
power_df = mariogalaxy2_powerups.toDF()
power_dets = mariogalaxy2_powerupdets.toDF()
bosses_df = mariogalaxy2_bosses.toDF()

from pyspark.sql import functions as f

# Take the average of the two hit columns
final_df_last = stars_df.filter(stars_df.id == 1123).withColumn('avg_hits', ((f.col('hits') + f.col('hits2')) / 2)+ 2) # separate the last star and add 2 to the avg hits because hits2 was null
final_df = stars_df.filter(stars_df.id != 1123).withColumn('avg_hits', (f.col('hits') + f.col('hits2')) / 2) # take the average of hits and hits2 for all other stars/rows 
final_df = final_df.unionByName(final_df_last) # Join the last row to the other rows again
final_df = final_df.drop('hits', 'hits2') # Drop the hits columns because we only need the average

# Convert the boss and prankster columns to numerical using a Pipeline and StringIndexer
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline

ind = [StringIndexer(inputCol = 'boss', outputCol = 'bossIndex'), StringIndexer(inputCol = 'prankster', outputCol = 'prankIndex')]
pipeline = Pipeline(stages = ind)
final_df = pipeline.fit(final_df).transform(final_df)

# Deal with the duplicate entries for the final bowser star
final_bowser_df = final_df.filter((final_df.galaxy_name == "Bowser's Galaxy Generator") & (final_df.prankster == 'N')).toPandas() # Filter out the final bowser stars into a pandas df
final_df = final_df[~final_df['galaxy_name'].endswith('Generator')] # Remove the duplicated final bowser star from the df after filtering it into a separate one

final_bowser_df['id'] = 1070 # Define what the final bowser row's values will be
final_bowser_df['star_label'] = 'GS6'
final_bowser_df['star_bits'] = 72
final_bowser_df['coins'] = 22
final_bowser_df['new_enemies'] = 'Bowser III'
final_bowser_df['comet_medal'] = 'Y'
final_bowser_df['difficulty'] = 'Harder' # First iteration of the final bowser star was Harder, the second Expert
final_bowser_df['avg_hits'] = 2

final_bowser_df.drop(final_bowser_df.tail(1).index,inplace = True) # Extract the first row in the df
final_bowser = spark.createDataFrame(final_bowser_df.astype(str)) # Convert the new final bowser star row into a Spark DF
final_df = final_df.unionByName(final_bowser) # Add the final bowser star to the original df

# Using the enemies, bosses, and enemy details dataframes convert the enemies data into numerical predictors for difficulty and add to final df

final_df.createOrReplaceTempView("stars") # Create a temporary table out of our created dataframe 

# Run a SQL query to combine enemy, bosses, enemy_details, and stars tables
enemies_df.createOrReplaceTempView("enemies")
enemy_dets.createOrReplaceTempView("enemy_details")
bosses_df.createOrReplaceTempView("bosses")

# we can save the results of a SparkSQL query as a Spark DF (enemy details DF to be exact)
basic_boss_df = bosses_df.select(['star_id', 'name', 'b_rank']) # Save bosses, their rank, and the stars they are found in in a separate df
basic_boss_df = basic_boss_df.withColumnRenamed('star_id', 'id')

# create a dataframe that combines stars, enemy, and enemy details tables together so we can group stars and enemy ranks found in the stars
star_enemy_df = spark.sql("SELECT s.id, s.boss, e.eid, e.is_boss, e.name, e.e_rank FROM stars s LEFT JOIN enemy_details ed ON s.id = ed.star_id LEFT JOIN enemies e ON ed.e_id = e.eid")
star_enemy_df = star_enemy_df.withColumn('e_rank', f.col('e_rank').cast('integer'))
star_avg_rank = star_enemy_df.groupby('id').agg({'eid':'count', 'e_rank':'avg'}) # Take the average rank and count of how many enemies are found in stars
star_avg_rank = star_avg_rank.withColumn('e_rank_avg', f.round(f.col('avg(e_rank)'))) # Round the average rank to the highest integer
plus_bosses = star_avg_rank.join(basic_boss_df, on = 'id', how = 'left_outer') # Add bosses to the star, enemy rank df

plus_bosses = plus_bosses.withColumn('total_rank', f.col('avg(e_rank)') * f.col('count(eid)')) # Find the new enemy average by incorporating the boss found in stars
plus_bosses = plus_bosses.withColumn('rank_with_boss', f.round((f.col('total_rank') + (f.col('b_rank') * 0.5)) / (f.col('count(eid)') + 1)))
plus_bosses = plus_bosses.withColumn('final_e_rank', f.when(plus_bosses.rank_with_boss.isNotNull(), plus_bosses.rank_with_boss).otherwise(plus_bosses.e_rank_avg))
# Record the final average rank by returning the non-boss average when there is not a boss in the star and the boss average when there is a boss
plus_bosses = plus_bosses.select(['id', 'final_e_rank'])

star_enemy_ranks = plus_bosses.groupby('id').avg('final_e_rank') # Save the final enemy average rank and the star id in a separate df
star_enemy_ranks = star_enemy_ranks.na.fill(100.0) # Fill the avg rank as 100 for stars with no enemies

final_df = final_df.join(star_enemy_ranks, ['id'])

# Do the same thing was traps as with enemies

traps_df.createOrReplaceTempView("traps")
trap_dets.createOrReplaceTempView("trap_details")

# create a dataframe that combines stars, trap, and trap details tables together so we can group stars and trap ranks found in the stars
star_trap_df = spark.sql("SELECT s.id, t.trap_id, t.name, t.t_rank FROM stars s LEFT JOIN trap_details td ON s.id = td.star_id LEFT JOIN traps t ON td.trap_id = t.trap_id")
star_trap_df = star_trap_df.withColumn('t_rank', f.col('t_rank').cast('integer'))

star_tavg_rank = star_trap_df.groupby('id').avg('t_rank') # Take the average rank and count of how many enemies are found in stars
star_tavg_rank = star_tavg_rank.withColumn('t_rank_avg', f.round(f.col('avg(t_rank)'))) # Round the average rank to the highest integer

star_trap_ranks = star_tavg_rank.select(['id', 't_rank_avg']) # Save the final enemy average rank and the star id in a separate df
star_trap_ranks = star_trap_ranks.na.fill(50.0) # Fill the null avg_rank with 50 for stars with no traps
final_df = final_df.join(star_trap_ranks, ['id'])

# Do the same thing was powerups as with enemies, traps
power_df.createOrReplaceTempView("powerups")
power_dets.createOrReplaceTempView("powerup_details")

# create a dataframe that combines stars, trap, and trap details tables together so we can group stars and trap ranks found in the stars
star_power_df = spark.sql("SELECT s.id, p.power_id, p.name, p.p_rank FROM stars s LEFT JOIN powerup_details pd ON s.id = pd.star_id LEFT JOIN powerups p ON pd.p_id = p.power_id")
star_power_df = star_power_df.withColumn('p_rank', f.col('p_rank').cast('integer'))

star_pavg_rank = star_power_df.groupby('id').avg('p_rank') # Take the average rank and count of how many enemies are found in stars
star_pavg_rank = star_pavg_rank.withColumn('p_rank_avg', f.round(f.col('avg(p_rank)'))) # Round the average rank to the highest integer

star_powers_ranks = star_pavg_rank.select(['id', 'p_rank_avg']) # Save the final enemy average rank and the star id in a separate df
star_powers_ranks = star_powers_ranks.na.fill(20.0) # Fills the null values in the df with 20 as the average ranking for stars with no powerups
final_df = final_df.join(star_powers_ranks, ['id'])

final_df = final_df.withColumnRenamed('avg(final_e_rank)', 'avg_enemy_rank')

mariogalaxy2out_node = DynamicFrame.fromDF(final_df, glueContext, 'difficulty_model')

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=mariogalaxy2out_node,
    mappings=[
        ("id", "long", "id", "long"),
        ("galaxy_name", "string", "galaxy_name", "string"),
        ("star_label", "string", "star_label", "string"),
        ("prankster", "string", "prankster", "string"),
        ("deaths", "long", "deaths", "long"),
        ("star_bits", "long", "star_bits", "long"),
        ("coins", "long", "coins", "long"),
        ("boss", "string", "boss", "string"),
        ("new_enemies", "string", "new_enemies", "string"),
        ("comet_medal", "string", "comet_medal", "string"),
        ("completion_timestamp", "string", "completion_timestamp", "string"),
        ("major_sections", "long", "major_sections", "long"),
        ("lives_found", "long", "lives_found", "long"),
        ("difficulty", "string", "difficulty", "string"),
        ("avg_hits", "float", "avg_hits", "float"),
        ("bossIndex", "long", "bossIndex", "long"),
        ("prankIndex", "long", "prankIndex", "long"),
        ("avg_enemy_rank", "double", "enemy_rank", "double"),
        ("t_rank_avg", "double", "trap_rank", "double"),
        ("p_rank_avg", "double", "power_rank", "double"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node mario-galaxy2-target
mariogalaxy2target_node3 = glueContext.write_dynamic_frame.from_options(
    frame=ApplyMapping_node2,
    connection_type="s3",
    format="csv",
    connection_options={"path": "s3://galaxy2-transformed-data", "partitionKeys": []},
    transformation_ctx="mariogalaxy2target_node3",
)

job.commit()
