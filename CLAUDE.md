# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Real Estate API application built with FastAPI and Python. The application manages property listings across Argentina, Paraguay, and Uruguay with support for multiple property types, agents, and comprehensive property details.

## Architecture

### Core Components
- **FastAPI Application** (`api/main.py`): Main application with database initialization, property management, and API endpoints
- **SQLite Database** (`api/real_estate.db`): Local database storing all property and location data
- **Data Generation**: Uses Faker library for generating sample property data

### Database Schema
The application uses a relational SQLite database with the following key tables:
- `countries` → `states` → `cities` (geographic hierarchy)
- `property_types`, `property_status` (lookup tables)
- `agents` (real estate agents)
- `properties` (main property listings with foreign keys to all above)
- `property_features` (additional property characteristics)

## Development Commands

### Starting the Application
```bash
cd api
python server.py
```
This starts the FastAPI server on http://0.0.0.0:8000 with auto-reload enabled.

### Installing Dependencies
```bash
cd api
pip install -r requirements.txt
```

### Database Operations
- **Initialize database with sample data**: GET `/init`
- **Create new properties**: GET `/new_houses`
- **Query properties by date range**: GET `/houses/{from_date}/{to_date}`

## Key API Endpoints

- `GET /` - API health check
- `GET /init` - Initialize database with sample data (3 countries, 9 states, 18 cities, 20 agents, 400 properties)
- `GET /new_houses` - Create 40 new properties with current timestamp
- `GET /houses/{from_date}/{to_date}` - Retrieve properties by publication date range (YYYY-MM-DD format)

## Data Model Features

Properties support:
- Multiple currencies (USD, ARS, PYG, UYU)
- Detailed area measurements (total, covered, uncovered, lot area)
- Property characteristics (bedrooms, bathrooms, parking, floors)
- Geographic coordinates and location hierarchy
- Agent assignment and property status tracking
- Date-based filtering and lifecycle management

## File Structure

```
api/
├── main.py           # Main FastAPI application and database logic
├── server.py         # Uvicorn server runner
├── requirements.txt  # Python dependencies
├── real_estate.db    # SQLite database (auto-generated)
├── properties.csv    # Property data (if exists)
```




Tengo que desarrollar un proceso ETL desarrollar un pipeline ETL que ingeste datos de una API en un Data Lake y el diseño de ese pipeline debe utilizar carga incremental.
Para esto necesito desarrollar el API que va a proveer de datos al proceso ETL, esta API debe estar desarrollada en python con FastAPI. Se trata de una app de una empresa inmobiliaria, te voy a proveer de las tablas SQLite que necesita esta API.

La Api debe contar con tres enpoints:
El primer endopoint crea ejemplos de propiedades al menos de 3 países (Argentina, Paraguay, Uruguay), 9  estados de dichos países, 18 ciudades de estos estados, 20 agentes inmobiliarios y 400 propiedades aleatorios falsas asociadas a estos datos previamente declarados, y las fechas de publicación son aleatorias. Nombre del endpoint: localhost.com:8000/init
El segundo endpoint generara solamente 40 propiedades con datos aleatorios y pero las fechas de publicación son el current_timestamp. Nombre del endpoint: localhost.com:8000/new_houses
El tercer endpoint devolvería todas las propiedades disponibles según un filtro de rango de fecha de publicación. Nombre del endpoint: localhost.com:8000/houses/{from_date}/{to_date} 

Actualmente esos endpoints ya se encuentran creados por lo que no hay que tocar nada en la api.

Debemos desarrollar el proceso etl. 

  
Quiero que utilices python para desarrollar el etl y las principales  librerías a utilizar deberían ser requests, pandas and deltalake.

El datalake debe tener dos capas: bronze y silver.
La capa bronze debe contener guardar los datos en formato json de la extracción de que se realiza a la api. Se debe particionar los datos por la fecha de publicación de las propiedades utilizando deltalake partition_by. El directorio de bronze es ../datalake/bronze/realestateapi/.
Se debe verificar que el directorio existe, si no es asi se crea y se carga los datos de la api desde 1990-01-01 hasta el dia de hoy.
Si el directorio existe se carga los registros desde la fecha de publicación maxima guardada en bronze.

En silver se guarda en formato tabla delta, se crea el esquema de la tabla, se crea la tabla si no existe utilizar deltalake.table.TableMerger API para insertar o actualizar los registros utilizando las columnas id y published_at para como predicado y directorio donde se guarda es ../datalake/silver/realestateapi/

No se crean comentarios en el código y todo el código se redacta en ingles.

CREATE TABLE countries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(100) NOT NULL,
    code VARCHAR(3) NOT NULL UNIQUE, -- ISO code (ARG, USA, etc.)
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE states (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    country_id INTEGER NOT NULL,
    name VARCHAR(100) NOT NULL,
    code VARCHAR(10),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (country_id) REFERENCES countries(id)
);


CREATE TABLE cities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    state_id INTEGER NOT NULL,
    name VARCHAR(100) NOT NULL,
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (state_id) REFERENCES states(id)
);


CREATE TABLE property_types (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(50) NOT NULL, -- Casa, Departamento, Oficina, Local, etc.
    description TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE property_status (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(30) NOT NULL, -- Activa, Vendida, Suspendida, Reservada
    description TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE agents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100),
    phone VARCHAR(20),
    company VARCHAR(100),
    license_number VARCHAR(50),
    is_active BOOLEAN DEFAULT 1,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE properties (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title VARCHAR(200) NOT NULL,
    description TEXT,
    property_type_id INTEGER NOT NULL,
    city_id INTEGER NOT NULL,
    address VARCHAR(300),
    neighborhood VARCHAR(100),
    zip_code VARCHAR(20),
    
    -- Precio y moneda
    price DECIMAL(15, 2) NOT NULL,
    currency VARCHAR(3) DEFAULT 'USD', -- USD, ARS, EUR, etc.
    price_per_sqm DECIMAL(10, 2), -- Precio por metro cuadrado
    
    -- Características físicas
    bedrooms INTEGER,
    bathrooms INTEGER,
    half_bathrooms INTEGER DEFAULT 0,
    total_area_sqm DECIMAL(10, 2), -- Superficie total
    covered_area_sqm DECIMAL(10, 2), -- Superficie cubierta
    uncovered_area_sqm DECIMAL(10, 2), -- Superficie descubierta
    lot_area_sqm DECIMAL(10, 2), -- Superficie del lote
    
    -- Características de construcción
    construction_year INTEGER,
    floors INTEGER DEFAULT 1,
    floor_number INTEGER, -- Para departamentos
    parking_spaces INTEGER DEFAULT 0,
    
    -- Estado y disponibilidad
    property_status_id INTEGER NOT NULL,
    is_furnished BOOLEAN DEFAULT 0,
    is_new_construction BOOLEAN DEFAULT 0,
    immediate_availability BOOLEAN DEFAULT 1,
    
    -- Información del agente
    agent_id INTEGER,
    
    -- Coordenadas exactas
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    
    -- Fechas importantes
    published_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    expires_at DATETIME,
    
    FOREIGN KEY (property_type_id) REFERENCES property_types(id),
    FOREIGN KEY (city_id) REFERENCES cities(id),
    FOREIGN KEY (property_status_id) REFERENCES property_status(id),
    FOREIGN KEY (agent_id) REFERENCES agents(id)
);


CREATE TABLE property_features (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    property_id INTEGER NOT NULL,
    feature_name VARCHAR(100) NOT NULL, -- Pool, Garden, Elevator, etc.
    feature_value VARCHAR(200), -- Para valores específicos
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (property_id) REFERENCES properties(id) ON DELETE CASCADE
);


-- Tipos de propiedad básicos
INSERT INTO property_types (name) VALUES 
('Casa'), ('Departamento'), ('PH'), ('Local Comercial'), 
('Oficina'), ('Galpón'), ('Terreno'), ('Quinta');

-- Estados de propiedad
INSERT INTO property_status (name) VALUES 
('Activa'), ('Vendida'), ('Alquilada'), ('Suspendida'), ('Reservada');