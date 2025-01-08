from services.openweather import OpenWeatherService
from services.s3 import S3

class WeatherDashboard:
    def __init__(self):
        """initialize the weather and s3 services"""
        self.weather_service = OpenWeatherService()
        self.s3_service = S3()

    def get_weather(self, city):
        """get the current weather for the city"""
        return self.weather_service.get_weather(city)

    def get_forecast(self, city):
        """get the current weather for the city"""
        return self.weather_service.get_forecast(city)
    
