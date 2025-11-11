from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
import sqlite3
import random
from faker import Faker
import os

from datetime import datetime
from typing import List, Dict, Any
fake = Faker()

DATABASE_NAME = "real_estate.db"

def init_database():
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS countries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name VARCHAR(100) NOT NULL,
        code VARCHAR(3) NOT NULL UNIQUE,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS states (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        country_id INTEGER NOT NULL,
        name VARCHAR(100) NOT NULL,
        code VARCHAR(10),
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (country_id) REFERENCES countries(id)
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS cities (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        state_id INTEGER NOT NULL,
        name VARCHAR(100) NOT NULL,
        latitude DECIMAL(10, 8),
        longitude DECIMAL(11, 8),
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (state_id) REFERENCES states(id)
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS property_types (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name VARCHAR(50) NOT NULL,
        description TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS property_status (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name VARCHAR(30) NOT NULL,
        description TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS agents (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name VARCHAR(100) NOT NULL,
        email VARCHAR(100),
        phone VARCHAR(20),
        company VARCHAR(100),
        license_number VARCHAR(50),
        is_active BOOLEAN DEFAULT 1,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS properties (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title VARCHAR(200) NOT NULL,
        description TEXT,
        property_type_id INTEGER NOT NULL,
        city_id INTEGER NOT NULL,
        address VARCHAR(300),
        neighborhood VARCHAR(100),
        zip_code VARCHAR(20),
        price DECIMAL(15, 2) NOT NULL,
        currency VARCHAR(3) DEFAULT 'USD',
        price_per_sqm DECIMAL(10, 2),
        bedrooms INTEGER,
        bathrooms INTEGER,
        half_bathrooms INTEGER DEFAULT 0,
        total_area_sqm DECIMAL(10, 2),
        covered_area_sqm DECIMAL(10, 2),
        uncovered_area_sqm DECIMAL(10, 2),
        lot_area_sqm DECIMAL(10, 2),
        construction_year INTEGER,
        floors INTEGER DEFAULT 1,
        floor_number INTEGER,
        parking_spaces INTEGER DEFAULT 0,
        property_status_id INTEGER NOT NULL,
        is_furnished BOOLEAN DEFAULT 0,
        is_new_construction BOOLEAN DEFAULT 0,
        immediate_availability BOOLEAN DEFAULT 1,
        agent_id INTEGER,
        latitude DECIMAL(10, 8),
        longitude DECIMAL(11, 8),
        published_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        expires_at DATETIME,
        FOREIGN KEY (property_type_id) REFERENCES property_types(id),
        FOREIGN KEY (city_id) REFERENCES cities(id),
        FOREIGN KEY (property_status_id) REFERENCES property_status(id),
        FOREIGN KEY (agent_id) REFERENCES agents(id)
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS property_features (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        property_id INTEGER NOT NULL,
        feature_name VARCHAR(100) NOT NULL,
        feature_value VARCHAR(200),
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (property_id) REFERENCES properties(id) ON DELETE CASCADE
    )
    """)
    
    property_types_data = [
        ('House', 'Single family house'),
        ('Apartment', 'Apartment in building'),
        ('Townhouse', 'Townhouse property'),
        ('Commercial', 'Commercial property'),
        ('Office', 'Office space'),
        ('Warehouse', 'Warehouse facility'),
        ('Land', 'Empty land lot'),
        ('Villa', 'Villa property')
    ]
    
    cursor.executemany("INSERT OR IGNORE INTO property_types (name, description) VALUES (?, ?)", property_types_data)
    
    property_status_data = [
        ('Active', 'Property is available'),
        ('Sold', 'Property has been sold'),
        ('Rented', 'Property has been rented'),
        ('Suspended', 'Property listing suspended'),
        ('Reserved', 'Property is reserved')
    ]
    
    cursor.executemany("INSERT OR IGNORE INTO property_status (name, description) VALUES (?, ?)", property_status_data)
    
    conn.commit()
    conn.close()

def clear_sample_data():
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    
    cursor.execute("DELETE FROM property_features")
    cursor.execute("DELETE FROM properties")
    cursor.execute("DELETE FROM agents")
    cursor.execute("DELETE FROM cities")
    cursor.execute("DELETE FROM states")
    cursor.execute("DELETE FROM countries")
    
    conn.commit()
    conn.close()

def create_countries():
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    
    countries_data = [
        ('Argentina', 'ARG'),
        ('Paraguay', 'PRY'),
        ('Uruguay', 'URY')
    ]
    
    for name, code in countries_data:
        cursor.execute("INSERT INTO countries (name, code) VALUES (?, ?)", (name, code))
    
    conn.commit()
    conn.close()

def create_states():
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    
    cursor.execute("SELECT id, name FROM countries")
    countries = cursor.fetchall()
    
    states_data = [
        (1, 'Buenos Aires', 'BA'),
        (1, 'Córdoba', 'CB'),
        (1, 'Santa Fe', 'SF'),
        (2, 'Central', 'CE'),
        (2, 'Alto Paraná', 'AP'),
        (2, 'Itapúa', 'IT'),
        (3, 'Montevideo', 'MO'),
        (3, 'Canelones', 'CA'),
        (3, 'Maldonado', 'MA')
    ]
    
    cursor.executemany("INSERT INTO states (country_id, name, code) VALUES (?, ?, ?)", states_data)
    
    conn.commit()
    conn.close()

def create_cities():
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    
    cities_data = [
        (1, 'Buenos Aires', -34.6118, -58.3960),
        (1, 'La Plata', -34.9205, -57.9536),
        (2, 'Córdoba', -31.4201, -64.1888),
        (2, 'Villa Carlos Paz', -31.4240, -64.4978),
        (3, 'Rosario', -32.9442, -60.6505),
        (3, 'Santa Fe', -31.6333, -60.7000),
        (4, 'Asunción', -25.2637, -57.5759),
        (4, 'San Lorenzo', -25.3407, -57.5089),
        (5, 'Ciudad del Este', -25.5095, -54.6162),
        (5, 'Hernandarias', -25.4089, -54.6355),
        (6, 'Encarnación', -27.3389, -55.8655),
        (6, 'Capitán Miranda', -27.2167, -55.8333),
        (7, 'Montevideo', -34.9011, -56.1645),
        (7, 'Las Piedras', -34.7306, -56.2139),
        (8, 'Canelones', -34.5225, -56.2775),
        (8, 'Santa Lucía', -34.4533, -56.3903),
        (9, 'Punta del Este', -34.9489, -54.9574),
        (9, 'Maldonado', -34.9000, -54.9583)
    ]
    
    cursor.executemany("INSERT INTO cities (state_id, name, latitude, longitude) VALUES (?, ?, ?, ?)", cities_data)
    
    conn.commit()
    conn.close()

def create_agents():
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    
    companies = ['Premium Real Estate', 'Golden Properties', 'Elite Realty', 'Urban Living', 'Coastal Properties']
    
    for i in range(20):
        name = fake.name()
        email = fake.email()
        phone = fake.phone_number()
        company = random.choice(companies)
        license_number = f"RE{random.randint(10000, 99999)}"
        is_active = 1
        
        cursor.execute("""
            INSERT INTO agents (name, email, phone, company, license_number, is_active)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (name, email, phone, company, license_number, is_active))
    
    conn.commit()
    conn.close()

def create_properties():
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    
    cursor.execute("SELECT id FROM property_types")
    property_types = [row[0] for row in cursor.fetchall()]
    
    cursor.execute("SELECT id FROM property_status")
    property_statuses = [row[0] for row in cursor.fetchall()]
    
    cursor.execute("SELECT id FROM cities")
    cities = [row[0] for row in cursor.fetchall()]
    
    cursor.execute("SELECT id FROM agents")
    agents = [row[0] for row in cursor.fetchall()]
    
    currencies = ['USD', 'ARS', 'PYG', 'UYU']
    neighborhoods = ['Downtown', 'Residential', 'Suburban', 'Waterfront', 'Historic District', 'Business District']
    
    for i in range(400):
        title = f"{fake.word().title()} {random.choice(['House', 'Apartment', 'Villa', 'Condo'])}"
        description = fake.text(max_nb_chars=500)
        property_type_id = random.choice(property_types)
        city_id = random.choice(cities)
        address = fake.address().replace('\n', ', ')
        neighborhood = random.choice(neighborhoods)
        zip_code = fake.zipcode()
        
        price = round(random.uniform(50000, 1000000), 2)
        currency = random.choice(currencies)
        
        total_area = round(random.uniform(50, 500), 2)
        covered_area = round(total_area * random.uniform(0.7, 0.95), 2)
        uncovered_area = round(total_area - covered_area, 2)
        lot_area = round(total_area * random.uniform(1.0, 2.0), 2)
        price_per_sqm = round(price / total_area, 2)
        
        bedrooms = random.randint(1, 6)
        bathrooms = random.randint(1, 4)
        half_bathrooms = random.randint(0, 2)
        
        construction_year = random.randint(1950, 2024)
        floors = random.randint(1, 3)
        floor_number = random.randint(1, 20) if property_type_id == 2 else None
        parking_spaces = random.randint(0, 4)
        
        property_status_id = random.choice(property_statuses)
        is_furnished = random.choice([0, 1])
        is_new_construction = 1 if construction_year >= 2020 else 0
        immediate_availability = random.choice([0, 1])
        
        agent_id = random.choice(agents)
        
        latitude = round(random.uniform(-35, -23), 8)
        longitude = round(random.uniform(-65, -50), 8)
        
        published_at = fake.date_time_between(start_date='-2y', end_date='now')
        updated_at = published_at + timedelta(days=random.randint(0, 30))
        expires_at = published_at + timedelta(days=random.randint(90, 365))
        
        cursor.execute("""
            INSERT INTO properties (
                title, description, property_type_id, city_id, address, neighborhood, zip_code,
                price, currency, price_per_sqm, bedrooms, bathrooms, half_bathrooms,
                total_area_sqm, covered_area_sqm, uncovered_area_sqm, lot_area_sqm,
                construction_year, floors, floor_number, parking_spaces,
                property_status_id, is_furnished, is_new_construction, immediate_availability,
                agent_id, latitude, longitude, published_at, updated_at, expires_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            title, description, property_type_id, city_id, address, neighborhood, zip_code,
            price, currency, price_per_sqm, bedrooms, bathrooms, half_bathrooms,
            total_area, covered_area, uncovered_area, lot_area,
            construction_year, floors, floor_number, parking_spaces,
            property_status_id, is_furnished, is_new_construction, immediate_availability,
            agent_id, latitude, longitude, published_at, updated_at, expires_at
        ))
    
    conn.commit()
    conn.close()

def create_new_properties():
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    
    cursor.execute("SELECT id FROM property_types")
    property_types = [row[0] for row in cursor.fetchall()]
    
    cursor.execute("SELECT id FROM property_status")
    property_statuses = [row[0] for row in cursor.fetchall()]
    
    cursor.execute("SELECT id FROM cities")
    cities = [row[0] for row in cursor.fetchall()]
    
    cursor.execute("SELECT id FROM agents")
    agents = [row[0] for row in cursor.fetchall()]
    
    if not property_types or not property_statuses or not cities or not agents:
        raise Exception("Database not initialized. Please call /init endpoint first.")
    
    currencies = ['USD', 'ARS', 'PYG', 'UYU']
    neighborhoods = ['Downtown', 'Residential', 'Suburban', 'Waterfront', 'Historic District', 'Business District']
    
    current_time = datetime.now()
    properties_created = []
    
    for i in range(40):
        title = f"{fake.word().title()} {random.choice(['House', 'Apartment', 'Villa', 'Condo'])}"
        description = fake.text(max_nb_chars=500)
        property_type_id = random.choice(property_types)
        city_id = random.choice(cities)
        address = fake.address().replace('\n', ', ')
        neighborhood = random.choice(neighborhoods)
        zip_code = fake.zipcode()
        
        price = round(random.uniform(50000, 1000000), 2)
        currency = random.choice(currencies)
        
        total_area = round(random.uniform(50, 500), 2)
        covered_area = round(total_area * random.uniform(0.7, 0.95), 2)
        uncovered_area = round(total_area - covered_area, 2)
        lot_area = round(total_area * random.uniform(1.0, 2.0), 2)
        price_per_sqm = round(price / total_area, 2)
        
        bedrooms = random.randint(1, 6)
        bathrooms = random.randint(1, 4)
        half_bathrooms = random.randint(0, 2)
        
        construction_year = random.randint(1950, 2024)
        floors = random.randint(1, 3)
        floor_number = random.randint(1, 20) if property_type_id == 2 else None
        parking_spaces = random.randint(0, 4)
        
        property_status_id = random.choice(property_statuses)
        is_furnished = random.choice([0, 1])
        is_new_construction = 1 if construction_year >= 2020 else 0
        immediate_availability = random.choice([0, 1])
        
        agent_id = random.choice(agents)
        
        latitude = round(random.uniform(-35, -23), 8)
        longitude = round(random.uniform(-65, -50), 8)
        
        expires_at = current_time + timedelta(days=random.randint(90, 365))
        
        cursor.execute("""
            INSERT INTO properties (
                title, description, property_type_id, city_id, address, neighborhood, zip_code,
                price, currency, price_per_sqm, bedrooms, bathrooms, half_bathrooms,
                total_area_sqm, covered_area_sqm, uncovered_area_sqm, lot_area_sqm,
                construction_year, floors, floor_number, parking_spaces,
                property_status_id, is_furnished, is_new_construction, immediate_availability,
                agent_id, latitude, longitude, published_at, updated_at, expires_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            title, description, property_type_id, city_id, address, neighborhood, zip_code,
            price, currency, price_per_sqm, bedrooms, bathrooms, half_bathrooms,
            total_area, covered_area, uncovered_area, lot_area,
            construction_year, floors, floor_number, parking_spaces,
            property_status_id, is_furnished, is_new_construction, immediate_availability,
            agent_id, latitude, longitude, current_time, current_time, expires_at
        ))
        
        property_id = cursor.lastrowid
        properties_created.append({
            "id": property_id,
            "title": title,
            "price": price,
            "currency": currency,
            "published_at": current_time.isoformat()
        })
    
    conn.commit()
    conn.close()
    
    return properties_created
  

def get_properties_by_date_range(from_date: str, to_date: str) -> List[Dict[str, Any]]:
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()

    try:
        try:
            from_datetime = datetime.strptime(from_date, "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            from_datetime = datetime.strptime(from_date, "%Y-%m-%d")

        try:
            to_datetime = datetime.strptime(to_date, "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            to_datetime = datetime.strptime(to_date, "%Y-%m-%d")
            to_datetime = to_datetime.replace(hour=23, minute=59, second=59)
    except ValueError:
        raise ValueError("Invalid date format. Use YYYY-MM-DD or YYYY-MM-DD HH:MM:SS format.")
    
    if from_datetime > to_datetime:
        raise ValueError("from_date must be earlier than or equal to to_date.")
    
    query = """
    SELECT 
        p.id,
        p.title,
        p.description,
        pt.name as property_type,
        c.name as city_name,
        s.name as state_name,
        co.name as country_name,
        p.address,
        p.neighborhood,
        p.zip_code,
        p.price,
        p.currency,
        p.price_per_sqm,
        p.bedrooms,
        p.bathrooms,
        p.half_bathrooms,
        p.total_area_sqm,
        p.covered_area_sqm,
        p.uncovered_area_sqm,
        p.lot_area_sqm,
        p.construction_year,
        p.floors,
        p.floor_number,
        p.parking_spaces,
        ps.name as property_status,
        p.is_furnished,
        p.is_new_construction,
        p.immediate_availability,
        a.name as agent_name,
        a.email as agent_email,
        a.phone as agent_phone,
        a.company as agent_company,
        p.latitude,
        p.longitude,
        p.published_at,
        p.updated_at,
        p.expires_at
    FROM properties p
    JOIN property_types pt ON p.property_type_id = pt.id
    JOIN cities c ON p.city_id = c.id
    JOIN states s ON c.state_id = s.id
    JOIN countries co ON s.country_id = co.id
    JOIN property_status ps ON p.property_status_id = ps.id
    LEFT JOIN agents a ON p.agent_id = a.id
    WHERE p.published_at BETWEEN ? AND ?
    ORDER BY p.published_at DESC
    """
    cursor.execute(query, (from_datetime.isoformat(sep=" "), to_datetime.isoformat(sep=" ")))
    rows = cursor.fetchall()
    
    properties = []
    for row in rows:
        property_data = {
            "id": row[0],
            "title": row[1],
            "description": row[2],
            "property_type": row[3],
            "location": {
                "city": row[4],
                "state": row[5],
                "country": row[6],
                "address": row[7],
                "neighborhood": row[8],
                "zip_code": row[9],
                "coordinates": {
                    "latitude": float(row[32]) if row[32] else None,
                    "longitude": float(row[33]) if row[33] else None
                }
            },
            "pricing": {
                "price": float(row[10]),
                "currency": row[11],
                "price_per_sqm": float(row[12]) if row[12] else None
            },
            "features": {
                "bedrooms": row[13],
                "bathrooms": row[14],
                "half_bathrooms": row[15],
                "total_area_sqm": float(row[16]) if row[16] else None,
                "covered_area_sqm": float(row[17]) if row[17] else None,
                "uncovered_area_sqm": float(row[18]) if row[18] else None,
                "lot_area_sqm": float(row[19]) if row[19] else None,
                "construction_year": row[20],
                "floors": row[21],
                "floor_number": row[22],
                "parking_spaces": row[23]
            },
            "status": {
                "property_status": row[24],
                "is_furnished": bool(row[25]),
                "is_new_construction": bool(row[26]),
                "immediate_availability": bool(row[27])
            },
            "agent": {
                "name": row[28],
                "email": row[29],
                "phone": row[30],
                "company": row[31]
            } if row[28] else None,
            "dates": {
                "published_at": row[34],
                "updated_at": row[35],
                "expires_at": row[36]
            }
        }
        properties.append(property_data)
    
    conn.close()
    return properties

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_database()
    yield

app = FastAPI(lifespan=lifespan)

@app.get("/")
async def root():
    return {"message": "Real Estate API"}

@app.get("/init")
async def initialize_data():
    try:
        clear_sample_data()
        create_countries()
        create_states()
        create_cities()
        create_agents()
        create_properties()
        
        return {
            "message": "Database initialized successfully",
            "data_created": {
                "countries": 3,
                "states": 9,
                "cities": 18,
                "agents": 20,
                "properties": 400
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error initializing database: {str(e)}")
      
@app.get("/new_houses")
async def create_new_houses():
    try:
        properties = create_new_properties()
        
        return {
            "message": "New properties created successfully",
            "properties_created": 40,
            "timestamp": datetime.now().isoformat(),
            "properties": properties
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating new properties: {str(e)}")
      
      
  
@app.get("/houses/{from_date}/{to_date}")
async def get_houses_by_date_range(from_date: str, to_date: str):
    try:
        properties = get_properties_by_date_range(from_date, to_date)
        
        if not properties:
            return {
                "message": "No properties found in the specified date range",
                "date_range": {
                    "from": from_date,
                    "to": to_date
                },
                "total_properties": 0,
                "properties": []
            }
        
        return {
            "message": "Properties retrieved successfully",
            "date_range": {
                "from": from_date,
                "to": to_date
            },
            "total_properties": len(properties),
            "properties": properties
        }
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving properties: {str(e)}")