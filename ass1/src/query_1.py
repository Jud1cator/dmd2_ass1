from pymongo import MongoClient
import pandas as pd
import time


def run():
    print("--- QUERY 1 ---")
    print("Retrieve all the customers that rented movies of at least\n\
two different categories during the current year\n")

    start_time = time.time()

    client = MongoClient("mongodb://mongo")
    db = client['dvdrental']

    recent_year = 0 
    for rent in db.rental.find():
        if recent_year < rent['rental_date'].year:
            recent_year = rent['rental_date'].year

    inventory_film = {}
    for inv in db.inventory.find():
        inventory_film.update({inv['inventory_id']: inv['film_id']})

    film_categories = {}
    for film in db.film_category.find():
        film_categories.update({film['film_id']: film['category_id']})

    customers_inventory = {}
    for rent in db.rental.find():
        if rent['rental_date'].year == recent_year:
            if not rent['customer_id'] in customers_inventory:
                customers_inventory.update({
                    rent['customer_id']: [rent['inventory_id']]
                })
            else:
                customers_inventory[rent['customer_id']].append(
                    rent['inventory_id'])

    diff_categories = {}
    for cust_id in customers_inventory:
        if len(customers_inventory[cust_id]) > 1:
            for inv in customers_inventory[cust_id]:
                if not cust_id in diff_categories:
                    diff_categories.update({cust_id: [film_categories[inventory_film[inv]]]})
                elif not film_categories[inventory_film[inv]] in diff_categories[cust_id]:
                    diff_categories[cust_id].append(film_categories[inventory_film[inv]])

    out = []
    for cust_id in diff_categories:
        if len(diff_categories[cust_id]) >= 2:
            for c in db.customer.find({'customer_id': cust_id}):
                out.append(c['first_name'] + ' ' + c['last_name'])

    report = pd.DataFrame(out)
    print(report)

    print("Run time: %.3f s\n" % (time.time() - start_time))
