import requests
import sqlite3
import json
import plotly
import plotly.plotly as py
from plotly.graph_objs import *
import secrets
import unittest

plotly.tools.set_credentials_file(username='elysegoldblum', api_key=secrets.PLOTLY_KEY)

try:
    con = sqlite3.connect('yelp.db')
    cur = con.cursor()
except:
    print("ERORR CONNECTING TO DATABASE.")


baseurl1 = 'https://api.yelp.com/v3/businesses/search'
baseurl2 = 'https://maps.googleapis.com/maps/api/geocode/json'

def reinit_db():
    cur.execute('DROP Table IF EXISTS Google')
    cur.execute('DROP Table IF EXISTS Yelp')

    cur.execute('CREATE Table IF NOT Exists Yelp (Id Integer Primary Key, Name Text, Type Text, Rating Real, Price Text, Address Text, Distance Real, GoogleId Integer)')
    cur.execute('CREATE TABLE IF NOT Exists Google (Id Integer Primary Key, Latitude Real, Longitude Real, State Text, Country Text, SearchString Text)')
    con.commit()

CACHE_FNAME = 'yelp_cache.json'
try:
    cache_file = open(CACHE_FNAME, 'r')
    cache_contents = cache_file.read()
    CACHE_DICTION = json.loads(cache_contents)
    cache_file.close()

except:
    CACHE_DICTION = {}

def params_unique_combination(baseurl, params):
    alphabetized_keys = sorted(params.keys())
    res = []
    for k in alphabetized_keys:
        res.append("{}-{}".format(k, params[k]))
    return baseurl + "_".join(res)

def make_request_using_cache(baseurl, params, headers = {}):
    unique_ident = params_unique_combination(baseurl,params)

    if unique_ident in CACHE_DICTION:
        return CACHE_DICTION[unique_ident]

    else:
        resp = requests.get(baseurl, params, headers = headers)
        CACHE_DICTION[unique_ident] = json.loads(resp.text)
        dumped_json_cache = json.dumps(CACHE_DICTION)
        fw = open(CACHE_FNAME,"w")
        fw.write(dumped_json_cache)
        fw.close()
        return CACHE_DICTION[unique_ident]

def location_change(location):
    r = make_request_using_cache(baseurl2, params = {'address': location, 'key' : secrets.GOOGLE_KEY})
    latitude = r['results'][0]['geometry']['location']['lat']
    longitude = r['results'][0]['geometry']['location']['lng']
    state = r['results'][0]['address_components'][-2]['long_name']
    country = r['results'][0]['address_components'][-1]['long_name']
    cur.execute('INSERT INTO Google VALUES (?,?,?,?,?,?)', (None, latitude, longitude, state, country, location))
    con.commit()
    return(latitude, longitude)

def restaurant_info(lat, lng, rest_type):
    rest_list = []
    for offset in range(3):
        r = make_request_using_cache(baseurl1, params = {'latitude':lat, 'longitude': lng, 'term': rest_type, 'limit' : 50, 'offset': 50 * offset}, headers = {'Authorization': "Bearer {}".format(secrets.YELP_KEY)})
        rest_list.extend(r["businesses"])
        for item in r['businesses']:
            name = item['name']
            type = item['categories'][0]['title']
            rating = item['rating']
            price = item.get('price', '')
            address = ' '.join(item['location']['display_address'])
            distance = item['distance']
            cur.execute('SELECT Id FROM Google WHERE latitude = ? AND longitude = ?', (lat, lng))
            google_id = cur.fetchone()[0]
            cur.execute('INSERT INTO Yelp VALUES (?,?,?,?,?,?,?,?)', (None, name, type, rating, price, address, distance, google_id))
            con.commit()
    return rest_list

def ratingsVprices(raw_data):
    ratings = list(zip(*raw_data))[0]
    prices = [len(x) for x in list(zip(*raw_data))[1]]
    size = [raw_data.count(x) for x in raw_data]

    print("\nScatter plot counts:")
    for tup in sorted(set(raw_data), key=lambda x: x[0]):
        print("{} appears {} times.".format(tup, raw_data.count(tup)))

    return ratings, prices, size

def distanceDictionary(raw_data):
    dist_dict = {"nearby" : 0, "sorta": 0, "medium" : 0, "pretty_far":0, "far" : 0}
    for dist in raw_data:
        if dist > 10000: # in meters
            dist_dict['far'] += 1
        elif dist >= 7500:
            dist_dict['pretty_far'] += 1
        elif dist >= 5000:
            dist_dict['medium'] += 1
        elif dist >= 2500:
            dist_dict['sorta'] += 1
        else:
            dist_dict['nearby'] += 1
    return dist_dict

def top_categories_dict(raw_data):
    category_count = {key : raw_data.count(key) for key in set(raw_data)}
    top_categories = sorted(category_count.items(), key=lambda x : x[1], reverse=True)[:5]
    top_categories=dict(top_categories)
    return top_categories

if __name__ == '__main__':
    while True:
        user_input = input("Enter a city or town (or 'exit' to run unit tests): ")
        if user_input == 'exit':
            break
        user_input2 = input("Enter a restaurant type (or 'exit' to run unit tests): ")
        if user_input2 == 'exit':
            break
        reinit_db()
        try:
            lat,lng = location_change(user_input)
            restaurant_info(lat,lng, user_input2)
            cur.execute('SELECT Rating, Price FROM Yelp JOIN Google ON GoogleId = Google.Id WHERE Google.SearchString = ?', (user_input, ))
            results = cur.fetchall()

            ratings, prices, size = ratingsVprices(results)

            trace0 = Scatter(
                x= ratings,
                y= prices,
                mode = "markers",
                marker = dict(
                    size=size,
                    sizemode='area',
                     sizeref=2.*max(size)/(40.**2),
                     sizemin=4
                )
            )

            data = Data([trace0])

            unique_url = py.plot(data, filename='ratings vs. prices')


            cur.execute('SELECT Distance FROM Yelp JOIN Google ON GoogleId = Google.Id WHERE Google.SearchString = ?', (user_input, ))
            results = [tup[0] for tup in cur.fetchall()]
            dist_dict = distanceDictionary(results)

            trace0 = Bar(
                x=list(dist_dict.keys()),
                y=list(dist_dict.values())
            )

            data = Data([trace0])

            unique_url = py.plot(data, filename='distance to user')

            cur.execute("SELECT Type from YELP JOIN Google ON GoogleId = Google.Id WHERE Google.SearchString = ?", (user_input, ))
            results = [tup[0] for tup in cur.fetchall()]
            top_categories = top_categories_dict(results)

            trace0 = Pie(
                labels= list(top_categories.keys()),
                values = list(top_categories.values())
            )
            data = Data([trace0])

            unique_url = py.plot(data, filename='category pie chart')

            cur.execute("SELECT Rating from Yelp JOIN Google ON GoogleId = Google.Id WHERE Google.SearchString = ?", (user_input, ))
            results = [tup[0] for tup in cur.fetchall()]
            trace0 = Histogram(
                x=results
            )

            data = Data([trace0])

            unique_url = py.plot(data, filename='rating histogram')
        except:
            print("Error handling data for your search: {} in {}".format(user_input2, user_input))
            print("Try again!\n")


class TestData(unittest.TestCase):

    def testGoogle(self):
        testData = location_change("Ann Arbor")
        self.assertTrue(type(testData) == tuple)
        self.assertTrue(type(testData[0]) == float)
        self.assertTrue(type(testData[1]) == float)
        self.assertTrue(testData[0] == 42.2808256)
        self.assertTrue(testData[1] == -83.7430378)
    def testYelp(self):
        testData = restaurant_info(42.2808256, -83.7430378, "Italian")
        self.assertTrue(type(testData) == list)
        self.assertTrue(type(testData[0]) == dict)
        self.assertTrue('name' in testData[0])
        self.assertTrue(len(testData) > 0)

class TestDatabase(unittest.TestCase):
    def testGoogle(self):
        cur.execute("SELECT * FROM Google")
        results = cur.fetchall()
        self.assertTrue(len(results) > 0)
        self.assertTrue(type(results) == list)
        self.assertTrue(type(results[0]) == tuple)
        self.assertTrue(len(results[0]) == 6)

    def testYelp(self):
        cur.execute("SELECT * FROM Yelp")
        results = cur.fetchall()
        self.assertTrue(len(results) > 0)
        self.assertTrue(type(results) == list)
        self.assertTrue(type(results[0]) == tuple)
        self.assertTrue(len(results[0]) == 8)

class TestProcessing(unittest.TestCase):
    def testRatingsVprices(self):
        func = ratingsVprices([(4.0, "$$"), (3.5,"$$$"), (3.0, "$$"), (4.0, "$$")])
        self.assertTrue(type(func) == tuple)
        self.assertEqual(func[0], (4.0, 3.5, 3.0, 4.0))
        self.assertEqual(func[1], [2, 3, 2, 2])

    def testDistDict(self):
        func = distanceDictionary([566, 5624, 99872, 6753, 6789, 2100, 4500])
        self.assertTrue(type(func) == dict)
        self.assertEqual(func["nearby"], 2)
        self.assertEqual(func["pretty_far"], 0)

    def testTopCategories(self):
        func = top_categories_dict(["Italian", "Pizza", "Bar", "Greek", "TexMex", "Pizza"])
        self.assertTrue(type(func) == dict)
        self.assertEqual(func, {'Pizza': 2, 'Greek': 1, 'TexMex': 1, 'Italian': 1, 'Bar': 1})


unittest.main()
