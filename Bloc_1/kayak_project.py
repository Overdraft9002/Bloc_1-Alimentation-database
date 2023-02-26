import requests
import json
import pandas as pd
import os
import logging
import scrapy
from scrapy.crawler import CrawlerProcess

path = os.getcwd() + "/"


# Import list of cities

city_list = ["Mont Saint Michel", "St Malo", "Bayeux", "Le Havre", "Rouen", "Paris", "Amiens", "Lille", "Strasbourg", "Chateau du Haut Koenigsbourg", "Colmar", "Eguisheim", "Besancon", "Dijon", "Annecy", "Grenoble", "Lyon", "Gorges du Verdon", "Bormes les Mimosas", "Cassis", "Marseille", "Aix en Provence", "Avignon", "Uzes", "Nimes", "Aigues Mortes", "Saintes Maries de la mer", "Collioure", "Carcassonne", "Ariege", "Toulouse", "Montauban", "Biarritz", "Bayonne", "La Rochelle"]

place_lat = []
place_lon = []
place_name = []

# Create city df

for i in city_list:
    city_data_raw = requests.get(f"https://nominatim.openstreetmap.org/search?q=<{i}>&format=json&addressdetails=1&limit=1")
    city_data_json = city_data_raw.json()
    place_lat.append(city_data_json[0]['lat'])
    place_lon.append(city_data_json[0]['lon'])

df_cities = pd.DataFrame(zip(city_list, place_lat, place_lon), columns = ['location', 'lat', 'lon'])

# Get Forecast

city_weather = []

for lat, lon in zip(df_cities.lat, df_cities.lon):
    target_get = requests.get(f'https://api.openweathermap.org/data/2.5/forecast?lon={lon}&lat={lat}&exclude=daily&units=metric&appid=3216ee210219bf362ec0382c6686af7c')
    target_weather = target_get.json()
    forecast = 0
    for i in range(len(target_weather['list'])):
        forecast = forecast + target_weather['list'][i]['main']['temp']
    forecast = forecast/len(target_weather['list'])
    forecast = round(forecast, 2)
    city_weather.append(forecast)

df_cities['Forecast'] = city_weather


df_cities.to_csv(path + r'df_cities.csv')

## Create hotels database

# Create booking spider
class BookingSpider(scrapy.Spider):
    # Name of your spider
    name = "Booking"
    # Starting URL
    start_urls = ['https://www.booking.com/']

    cities = city_list

    def parse(self, response):
        # FormRequest used to make a search on booking.com
        for i in self.cities:
            yield scrapy.FormRequest.from_response(
                response,
                formdata={'ss': i},
                callback=self.after_search,
                cb_kwargs = {'location': i}
                )
    # Parse function for form request
    def after_search(self, response, location):   
        property_cards = response.css('div[data-testid="property-card"]')
        for property in property_cards:
            url = property.css('a[data-testid="title-link"]').attrib['href']
            name = property.css('div.fcab3ed991.a23c043802::text').get()
            description = property.css('div.d8eab2cf7f::text').get()
            score = property.css('div.b5cd09854e.d10a6220b4::text').get()
            data = {
            "name" : name,
            "location":location,
            "description" : description,
            "score" : score,
            "url" : url}  
            yield response.follow(url=url, callback=self.get_gps, cb_kwargs = {'data':data})  

    def get_gps(self, response, data):
        corr = response.css('a#hotel_sidebar_static_map').attrib['data-atlas-latlng']
        corr = corr.split(',')
        data["lat"] = float(corr[0])
        data["lon"] = float(corr[1])
        yield (data)

# Name of the file where the results will be saved
filename = "Hotels_dataset.json"

# If file already exists, delete it before crawling (because Scrapy will concatenate the last and new results otherwise)
if filename in os.listdir(path):
        os.remove(path + filename)

# Declare a new CrawlerProcess with some settings
process = CrawlerProcess(settings = {
    'USER_AGENT': 'Chrome/97.0',
    'LOG_LEVEL': logging.INFO,
    "FEEDS": {
        path + filename: {"format": "json"},
    }
})

# Start the crawling using the spider you defined above
process.crawl(BookingSpider)
process.start()
