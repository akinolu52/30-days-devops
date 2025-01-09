import requests
import os
from dotenv import load_dotenv

load_dotenv()

class OpenWeatherService:
    def __init__(self):
        self.api_key = os.getenv("OPEN_WEATHER_API_KEY")
        self.base_url = "https://api.openweathermap.org/data/2.5/"

    @staticmethod
    def make_request(url: str, name: str):
        """makes an api call to the url and returns the response"""
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f'Error while fetching {name} data: {e}')
            return None

    def get_weather(self, city: str):
        """Fetch weather data from OpenWeather API"""
        url = f"{self.base_url}/weather?appid={self.api_key}&q={city}&units=imperial"

        return self.make_request(url, "weather")
    
    def get_forecast(self, city: str):
        """Fetch weather forecast data from OpenWeather API"""
        url = f"{self.base_url}/forecast?appid={self.api_key}&q={city}&units=imperial"

        return self.make_request(url, "weather forecast")
       