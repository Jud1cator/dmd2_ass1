# Create a report that shows a table actor (rows) vs actor (columns)
# with the number of movies the actors co-starred.

from pymongo import MongoClient
import pandas as pd
import time


start_time = time.time()

client = MongoClient("mongodb://mongo")
db = client['dvdrental']

film_actors = {}
for fa in db.film_actor.find():
    if not fa['film_id'] in film_actors:
        film_actors.update({fa['film_id']: [fa['actor_id']]})
    else:
        film_actors[fa['film_id']].append(fa['actor_id'])

co_star = {}
for film_id in film_actors:
    for a1 in film_actors[film_id]:
        if not a1 in co_star:
                co_star.update({a1: {}})
        for a2 in film_actors[film_id]:
            if a1 == a2: continue
            if not a2 in co_star[a1]:
                co_star[a1].update({a2: 1})
            else:
                co_star[a1][a2] += 1

raw_report = pd.DataFrame(co_star)
report = raw_report.fillna(0).astype(int).sort_index().sort_index(axis=1)
pd.set_option('display.max_columns', 10)
print(report)

print("--- %.3f seconds ---" % (time.time() - start_time))
