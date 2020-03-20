# Retrieve all the customers that rented movies of at least
# two different categories during the current year

from pymongo import MongoClient
import time


start_time = time.time()

client = MongoClient("mongodb://mongo")
db = client['dvdrental']

# Find the most recent year in table 'rental'
recent_year = 0
rentals = db.rental.find()
for rent in rentals:
    if recent_year < rent['rental_date'].year:
        recent_year = rent['rental_date'].year

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

diff_categories = dict()
for cust_id in customers_inventory:
    if len(customers_inventory[cust_id]) > 1:
        for inv in customers_inventory[cust_id]:
            if cust_id not in diff_categories:
                diff_categories.update({cust_id: [inv]})
            elif inv not in diff_categories[cust_id]:
                diff_categories[cust_id].append(inv)

for cust_id in diff_categories:
    if len(diff_categories[cust_id]) >= 2:
        for c in db.customer.find({'customer_id': cust_id}):
            print(c['first_name'] + ' ' + c['last_name'])

print("--- %.3f seconds ---" % (time.time() - start_time))
