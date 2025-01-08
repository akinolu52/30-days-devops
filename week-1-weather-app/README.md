# Weather dashboard app

Day 1: Building a weather data collection system using AWS S3 and OpenWeather API

## Project Overview
This project is a Weather Data Collection System that demonstrates core DevOps principles by combining:
- External API Integration (OpenWeather API)
- Cloud Storage (AWS S3)
- Infrastructure as Code
- Version Control (Git)
- Python Development
- Error Handling
- Environment Management

## Features
- Fetches real-time weather data for multiple cities
- Displays temperature (°F), humidity, and weather conditions
- Automatically stores weather data in AWS S3
- Supports multiple cities tracking
- Timestamps all data for historical tracking

## Technical Architecture
- **Language:** Python 3.x
- **Cloud Provider:** AWS (S3)
- **External API:** OpenWeather API
- **Dependencies:** 
  - boto3 (AWS SDK)
  - python-dotenv
  - requests

```markdown
## Project Structure
weather-dashboard/
  src/
    __init__.py
    weather_dashboard.py
    services/
      s3.py
      openweather.py
  tests/
  data/
    cities.json
  .env
  .gitignore
  requirements.txt

## Setup Instructions
1. Clone the repository:
--bash
git clone https://github.com/akinolu52/30-days-devops.git

2. Change directory:
--bash
cd week-1-weather-app

3. Install dependencies:
--bash
pip install -r requirements.txt

4. Configure environment variables (.env):
OPEN_WEATHER_API_KEY=your_api_key
AWS_BUCKET_NAME=weather-app-$random
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_DEFAULT_REGION=your_region

5. Change cities (OPTIONAL)
Edit the data/cities.json file to add or remove cities

5. Run the application:
--bash
python src/__init__.py


