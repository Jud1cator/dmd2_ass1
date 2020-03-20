# A report that, given a certain customer (parameter) and his/her historical data on rented movies,
# recommends other movies. Such a recommendation should be based on customers that watched similar 
# sets of movies. Create a metric (any) to assess to which degree a movie is a good recommendation.

from pymongo import MongoClient
import pandas as pd
import time
import random


print("Enter customer id:")
usr_id = int(input())

start_time = time.time()

client = MongoClient("mongodb://mongo")
db = client['dvdrental']

usr = db.customer.find({'customer_id': usr_id})[0]
print("Customer name: " + usr['first_name']
    + ' ' + usr['last_name'])

inventory_film = {}
for inv in db.inventory.find():
    inventory_film.update({inv['inventory_id']: inv['film_id']})

customer_watched = {}
for rent in db.rental.find():
    if rent['customer_id'] in customer_watched:
        if not inventory_film[rent['inventory_id']] in customer_watched[rent['customer_id']]:
            customer_watched[rent['customer_id']].append(inventory_film[rent['inventory_id']])
    else:
        customer_watched.update({rent['customer_id']: [inventory_film[rent['inventory_id']]]})

sample_set = customer_watched[usr_id]

good_recomendators = {}
max_m = 0
for cust in customer_watched:
    if cust == usr_id: continue
    m = 0
    for f in sample_set:
        if f in customer_watched[cust]:
            m += 1
    good_recomendators.update({cust: m})

    if max_m < m and len(customer_watched[cust]) >= len(customer_watched[usr_id]):
        max_m = m

good_films = {}
for cust in good_recomendators:
    if good_recomendators[cust] != max_m: continue
    rec_set = [film for film in customer_watched[cust] if not film in sample_set]
    for film in rec_set:
        if film in good_films:
            good_films[film] += 1
        else:
            good_films.update({film: 1})

max_m = max(good_films.values())

rec_set = [film for film in good_films if good_films[film] == max_m]
# for i in rec_set:
#     print(db.film.find({'film_id': i})[0]['title'] + ' ' + str(good_films[i]))
rec = db.film.find({'film_id': random.choice(rec_set)})[0]['title']
print('Recommended movie: ' + rec)
print('Degree of recommendation: ' + str(max_m))

print("--- %.3f seconds ---" % (time.time() - start_time))
