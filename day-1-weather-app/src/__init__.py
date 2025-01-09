from weather_dashboard import WeatherDashboard
from datetime import datetime
import json

def show_weather_data(data):
    value = data.get('main') or data['list'][0]['main']

    temp = value['temp']
    feels_like = value['feels_like']
    humidity = value['humidity']
    description = data['weather'][0]['description'] if "weather" in data else data['list'][0]['weather'][0]['description']

    print(f"Temperature: {temp}°F")
    print(f"Feels like: {feels_like}°F")
    print(f"Humidity: {humidity}%")
    print(f"Conditions: {description}")

def main():
    cities = json.loads(open('./data/cities.json').read())['cities']

    weather = WeatherDashboard()

    # create s3 bucket for file storage
    weather.s3_service.create_bucket()

    for city in cities:
        if weather_data := weather.get_weather(city):
            print(f'Current weather for {city} at {datetime.fromtimestamp(weather_data["dt"])}')
            show_weather_data(weather_data)
            print('--' * 45)

            # save the weather data to S3
            result = weather.s3_service.save_json_file(weather_data)

            if not result:
                print(f'Failed to save weather data of {city} to S3')

        else:
            print(f'No weather data found for {city}')

        if forecast_data :=weather.get_forecast(city):
            print(f'Forecast Weather for {city}')
            show_weather_data(forecast_data)

        print('//'* 70, '\n')

if __name__ == '__main__':
    main()