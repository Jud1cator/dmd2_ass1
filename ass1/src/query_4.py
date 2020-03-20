from pymongo import MongoClient
import pandas as pd
import time
import random


def run():
    print("--- QUERY 4 ---")
    print("A report that, given a certain customer (parameter) and his/her historical data on rented movies,\n\
recommends other movies. Such a recommendation should be based on customers that watched similar\n\
sets of movies. Create a metric (any) to assess to which degree a movie is a good recommendation.\n")

    start_time = time.time()

    client = MongoClient("mongodb://mongo")
    db = client['dvdrental']

    usr_id = random.choice(list(db.customer.find()))['customer_id']

    usr = db.customer.find({'customer_id': usr_id})[0]
    print("Customer name: " + usr['first_name']
        + ' ' + usr['last_name'])

    inv_film = {}
    for inv in db.inventory.find():
        inv_film.update({inv['inventory_id']: inv['film_id']})

    cust_watch = {}
    for rent in db.rental.find():
        if rent['customer_id'] in cust_watch:
            if not inv_film[rent['inventory_id']] in cust_watch[rent['customer_id']]:
                cust_watch[rent['customer_id']].append(inv_film[rent['inventory_id']])
        else:
            cust_watch.update({rent['customer_id']: [inv_film[rent['inventory_id']]]})

    sample_set = cust_watch[usr_id]

    good_recomendators = {}
    max_m = 0
    for cust in cust_watch:
        if cust == usr_id: continue
        m = 0
        for f in sample_set:
            if f in cust_watch[cust]:
                m += 1
        good_recomendators.update({cust: m})

        if max_m < m and len(cust_watch[cust]) >= len(cust_watch[usr_id]):
            max_m = m

    good_films = {}
    for cust in good_recomendators:
        if good_recomendators[cust] != max_m: continue
        rec_set = [film for film in cust_watch[cust] if not film in sample_set]
        for film in rec_set:
            if film in good_films:
                good_films[film] += 1
            else:
                good_films.update({film: 1})

    max_m = max(good_films.values())

    rec_set = [film for film in good_films if good_films[film] == max_m]
    rec = db.film.find({'film_id': random.choice(rec_set)})[0]['title']
    print('Recommended movie: ' + rec)
    print('Degree of recommendation: ' + str(max_m))

    print("Run time: %.3f s" % (time.time() - start_time))
    print()
