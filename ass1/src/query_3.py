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

    film_categories = {fc['film_id']: fc['category_id']
                    for fc in db.film_category.find()}
    category_names = {c['category_id']: c['name']
                    for c in db.category.find()}
    film_titles = {f['film_id']: f['title']
                for f in db.film.find()}
                
    raw_report = []
    for film in film_inventory:
        times_rented = 0
        for inv in film_inventory[film]:
            if inv in inv_times_rented:
                times_rented += inv_times_rented[inv]
        category_name = category_names[film_categories[film]]
        film_title = film_titles[film]
        raw_report.append([film_title, category_name, times_rented])

    report = pd.DataFrame(raw_report)
    report.columns = ['title', 'category', 'times rented']
    print(report)

    print("Run time: %.3f s\n" % (time.time() - start_time))
