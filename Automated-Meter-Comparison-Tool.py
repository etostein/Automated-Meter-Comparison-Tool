import psycopg2
from psycopg2 import sql 
import csv
import os


# Constants
DB_HOST = "" # Add your actual database host
DB_USER = ""        # Add your actual database user
DB_PASSWORD = ""  # Add your actual database password
DB_PORT = 5432
DB_NAME = ""  # Add your actual database name


print("Connecting to the database...")
print("The structure of the table should be as follows: fid: should be the identifier , wkb_geometry: should be the geometry column,  'MikesAnalysis' schema should be the schema name if not change it in the code") 
# Get table name from user input
DB_TABLE = input("Enter the table name: ")


query1 = sql.SQL(
    '''
    alter table "MikesAnalysis".{} 
    alter column "fid" set not null;
    '''
).format(sql.SQL(DB_TABLE), sql.SQL(DB_TABLE), sql.SQL(DB_TABLE))

query2 = sql.SQL(
    ''' alter table "MikesAnalysis".{} 
        add column if not exists geog geography(point, 4326) ;
        create index on "MikesAnalysis".{}  using gist (geog);
        /**
  We update geog with the origianl geom 
 */
        update "MikesAnalysis".{} 
        set geog = st_transform(wkb_geometry, 4326)::geography
        where wkb_geometry is not null;
   '''
).format(sql.SQL(DB_TABLE), sql.SQL(DB_TABLE), sql.SQL(DB_TABLE))


query3 = sql.SQL(
    ''' alter table "MikesAnalysis".{}
        add column if not exists closest_point_fid bigint,
        add column if not exists closest_point_distance double precision;
       
       ;'''
).format(
    sql.SQL(DB_TABLE))

query4 = sql.SQL(
    '''
WITH closest AS (SELECT a.fid     AS current_id,
                        close.bid AS closest_id,
                        close.dist
                 FROM "MikesAnalysis".{} a
                          CROSS JOIN LATERAL (
                     SELECT b.fid                             AS bid,
                            a.geog <-> b.geog AS dist
                     FROM "MikesAnalysis".{} b
                     WHERE a.fid <> b.fid
                       AND b.geog IS NOT NULL
                     ORDER BY dist
                     LIMIT 1
                     ) close
                 WHERE a.wkb_geometry IS NOT NULL
)
UPDATE "MikesAnalysis".{} AS target
SET closest_point_distance = closest.dist,
    closest_point_fid = closest.closest_id
FROM closest
WHERE target.fid = closest.current_id;

select fid, closest_point_distance, closest_point_fid
from "MikesAnalysis".{}
order by fid;

'''
).format(sql.SQL(DB_TABLE), sql.SQL(DB_TABLE), sql.SQL(DB_TABLE), sql.SQL(DB_TABLE))

query5 = sql.SQL(
    '''WITH categorized AS (
    SELECT
    	CASE
            WHEN "closest_point_distance" <= 30 THEN '0-100ft'
            WHEN "closest_point_distance" > 30 AND "closest_point_distance" <= 90 THEN '100-300ft'
    	    WHEN "closest_point_distance" > 90 THEN 'Above 300ft'
    	    WHEN "geog" is null THEN 'no geometry'
            ELSE 'Unknown'
        END AS range_category,
        "closest_point_distance"
    FROM "MikesAnalysis".{}
)
SELECT
    range_category as "Distance",
    COUNT(*) AS "Count",
    (AVG("closest_point_distance") * 3.281):: numeric(6,1) AS "Average distance (ft)"
FROM categorized
GROUP BY  range_category
ORDER BY range_category '''
    
).format(sql.SQL(DB_TABLE))


output_path = r'C:\Users\ernesto.monique\Downloads'
filename = f'{DB_TABLE}_resultsf.csv'
full_path = os.path.join(output_path, filename)        


try:
    # Establish connection
    conn = psycopg2.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT,
        dbname=DB_NAME
    )
    cursor = conn.cursor()
    
    queries = [query1,query2, query3, query4, query5]



# Execute all queries sequentially
   # Execute all queries sequentially with success messages
    for idx, query in enumerate(queries, start=1):
        try:
            if 'VACUUM' in query.as_string(conn).upper():  # Check if the query is a VACUUM command
                conn.autocommit = True  # Enable autocommit for VACUUM
                cursor.execute(query)
                conn.autocommit = False  # Disable autocommit after VACUUM
            else:
                cursor.execute(query)
        
            print(f"Query {idx} has been successful.")  # Success message for each query

            # Export result of query5 to CSV
            if idx == len(queries):  # Assuming query12 is the last query
                results = cursor.fetchall()  # Fetch all rows
                column_names = [desc[0] for desc in cursor.description]  # Get column names
                
                # Write to CSV file
                with open(full_path, mode='w', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerow(column_names)  # Write header
                    writer.writerows(results)  # Write data rows
                
                print(f"Query 5 results exported to '{DB_TABLE}_results.csv'.")
        
        except Exception as e:
            print(f"Error in Query {idx}: {e}")
            conn.rollback()
            break  # Stop execution if any query fails


    # Commit if all succeed
    conn.commit()
    print("Queries executed successfully!")

except Exception as e:
    # Rollback if any query fails
    conn.rollback()
    print(f"Error executing queries: {e}")

finally:
    # Ensure resources are released
    cursor.close()
    conn.close()