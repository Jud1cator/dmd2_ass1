from pymongo import MongoClient
import pandas as pd
import time


def run():
    print("--- QUERY 3 ---")
    print("A report that lists all films, their film category and\n\
the number of times it has been rented by a customer.\n")

    start_time = time.time()

    client = MongoClient("mongodb://mongo")
    db = client['dvdrental']

    film_inventory = {i['film_id']: [] for i in db.film.find()}
    for inv in db.inventory.find():
        film_inventory[inv['film_id']].append(inv['inventory_id'])

    inv_times_rented = {}
    for rent in db.rental.find():
        iid = rent['inventory_id']
        if iid in inv_times_rented:
            inv_times_rented[iid] += 1
        else:
            inv_times_rented.update({iid: 1})

    raw_report = []
    for film in film_inventory:
        times_rented = 0
        for inv in film_inventory[film]:
            if inv in inv_times_rented:
                times_rented += inv_times_rented[inv]
        category_id = db.film_category.find({'film_id': film})[0]['category_id']
        category_name = db.category.find({'category_id': category_id})[0]['name']
        film_title = db.film.find({'film_id': film})[0]['title']
        raw_report.append([film_title, category_name, times_rented])

    report = pd.DataFrame(raw_report)
    report.columns = ['title', 'category', 'times rented']
    print(report)

    print("Run time: %.3f s\n" % (time.time() - start_time))
