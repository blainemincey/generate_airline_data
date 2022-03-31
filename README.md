# generate_airline_data
Python script to generate Airline related data using Faker and Faker AirTravel

## Steps to install/run
1. Assumes Python 3.9 with PIP is installed.
2. ```pip3 install -r requirements.txt```
3. Rename env.example to .env
4. Modify the MongoDB connection string in .env
5. ```python3 generate_airline_data.py```

Defaults set in .env are to generate 100K records in a flight_reservations
collection in the airline_flights database.


