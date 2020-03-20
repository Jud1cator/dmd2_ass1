from pymongo import MongoClient
import pandas as pd
import time
import random


def run():
    print("--- QUERY 5 ---")
    print("Create a report that shows the degrees of separation between a \
certain actor\n(choose one, this should be fixed) and any other actor.\n")

    start_time = time.time()

    client = MongoClient("mongodb://mongo")
    db = client['dvdrental']

    choosen_actor = random.choice(list(db.actor.find()))['actor_id']


    film_actors = {}
    for fa in db.film_actor.find():
        if fa['film_id'] in film_actors:
            film_actors[fa['film_id']].append(fa['actor_id'])
        else:
            film_actors.update({fa['film_id']: [fa['actor_id']]})

    actor_films = {}
    for fa in db.film_actor.find():
        if fa['actor_id'] in actor_films:
            actor_films[fa['actor_id']].append(fa['film_id'])
        else:
            actor_films.update({fa['actor_id']: [fa['film_id']]})

    bacon = {choosen_actor: 0}

    def bfs(choosen):
        actors = []
        for film in actor_films[choosen]:
            for actor in film_actors[film]:
                if not actor in bacon:
                    actors.append(actor)
                    bacon.update({actor: bacon[choosen]+1})
                else:
                    bacon[actor] = min(bacon[actor], bacon[choosen]+1)
        for a in actors:
            bfs(a)
        
    bfs(choosen_actor)

    report = pd.DataFrame.from_dict(bacon, orient='index', 
                                    columns=['Bacon number']).sort_index()
    report.index.name = 'Actor id'
    print(report)

    print("Run time: %.3f s\n" % (time.time() - start_time))
