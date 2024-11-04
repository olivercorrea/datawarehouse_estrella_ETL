import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import random

class InventoryETLSetup:
    def __init__(self):
        """
        Inicializa la conexión a PostgreSQL usando los parámetros del docker-compose
        """
        self.connection_string = "postgresql://usuario:contraseña_segura@localhost:5432/mi_base_datos"
        self.engine = create_engine(self.connection_string)
        
    def create_source_tables(self):
        """
        Crea las tablas fuente en la base de datos
        """
        with self.engine.connect() as conn:
            # Crear tabla de productos
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS source_products (
                    product_id VARCHAR(10) PRIMARY KEY,
                    product_name VARCHAR(100),
                    description TEXT,
                    category VARCHAR(50),
                    subcategory VARCHAR(50),
                    brand VARCHAR(50),
                    unit_measure VARCHAR(20),
                    retail_price DECIMAL(10,2),
                    perishable BOOLEAN,
                    shelf_life_days INTEGER
                )
            """))
            
            # Crear tabla de tiendas
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS source_stores (
                    location_id VARCHAR(10) PRIMARY KEY,
                    store_name VARCHAR(100),
                    store_type VARCHAR(50),
                    address VARCHAR(200),
                    city VARCHAR(100),
                    state VARCHAR(50),
                    country VARCHAR(50),
                    zone VARCHAR(50),
                    storage_capacity INTEGER
                )
            """))
            
            # Crear tabla de proveedores
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS source_suppliers (
                    supplier_id VARCHAR(10) PRIMARY KEY,
                    supplier_name VARCHAR(100),
                    contact_person VARCHAR(100),
                    contact_email VARCHAR(100),
                    phone VARCHAR(20),
                    address VARCHAR(200),
                    city VARCHAR(100),
                    country VARCHAR(50),
                    supply_category VARCHAR(50),
                    lead_time_days INTEGER
                )
            """))
            
            # Crear tabla de inventario
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS source_inventory (
                    inventory_id SERIAL PRIMARY KEY,
                    product_id VARCHAR(10),
                    location_id VARCHAR(10),
                    supplier_id VARCHAR(10),
                    transaction_date DATE,
                    quantity_on_hand INTEGER,
                    unit_cost DECIMAL(10,2),
                    minimum_stock INTEGER,
                    maximum_stock INTEGER,
                    reorder_point INTEGER,
                    units_sold INTEGER,
                    units_received INTEGER,
                    FOREIGN KEY (product_id) REFERENCES source_products(product_id),
                    FOREIGN KEY (location_id) REFERENCES source_stores(location_id),
                    FOREIGN KEY (supplier_id) REFERENCES source_suppliers(supplier_id)
                )
            """))
            
            conn.commit()

    def generate_sample_data(self):
        """
        Genera datos de ejemplo para las tablas fuente
        """
        # Datos de productos
        categories = ['Abarrotes', 'Lácteos', 'Carnes', 'Bebidas', 'Limpieza']
        brands = ['Marca A', 'Marca B', 'Marca C', 'Marca D', 'Marca E']
        
        products_data = []
        for i in range(50):  # 50 productos
            category = random.choice(categories)
            products_data.append({
                'product_id': f'P{i:03d}',
                'product_name': f'Producto {i}',
                'description': f'Descripción del producto {i}',
                'category': category,
                'subcategory': f'Sub-{category}',
                'brand': random.choice(brands),
                'unit_measure': random.choice(['UN', 'KG', 'L']),
                'retail_price': round(random.uniform(10, 1000), 2),
                'perishable': random.choice([True, False]),
                'shelf_life_days': random.randint(7, 365)
            })
        
        # Datos de tiendas
        cities = ['Lima', 'Arequipa', 'Trujillo', 'Cusco', 'Piura']
        stores_data = []
        for i in range(10):  # 10 tiendas
            city = random.choice(cities)
            stores_data.append({
                'location_id': f'L{i:03d}',
                'store_name': f'Tienda {city} {i}',
                'store_type': random.choice(['Principal', 'Sucursal']),
                'address': f'Dirección {i}',
                'city': city,
                'state': city,
                'country': 'Perú',
                'zone': random.choice(['Norte', 'Sur', 'Este', 'Oeste']),
                'storage_capacity': random.randint(1000, 5000)
            })
        
        # Datos de proveedores
        suppliers_data = []
        for i in range(20):  # 20 proveedores
            city = random.choice(cities)
            suppliers_data.append({
                'supplier_id': f'S{i:03d}',
                'supplier_name': f'Proveedor {i}',
                'contact_person': f'Contacto {i}',
                'contact_email': f'contacto{i}@proveedor.com',
                'phone': f'9{random.randint(10000000, 99999999)}',
                'address': f'Dirección Proveedor {i}',
                'city': city,
                'country': 'Perú',
                'supply_category': random.choice(categories),
                'lead_time_days': random.randint(1, 30)
            })
        
        # Insertar datos en las tablas
        with self.engine.connect() as conn:
            # Limpiar datos existentes
            conn.execute(text("TRUNCATE TABLE source_inventory CASCADE"))
            conn.execute(text("TRUNCATE TABLE source_products CASCADE"))
            conn.execute(text("TRUNCATE TABLE source_stores CASCADE"))
            conn.execute(text("TRUNCATE TABLE source_suppliers CASCADE"))
            
            # Insertar nuevos datos
            for product in products_data:
                conn.execute(
                    text("""
                        INSERT INTO source_products VALUES 
                        (:product_id, :product_name, :description, :category, 
                         :subcategory, :brand, :unit_measure, :retail_price, 
                         :perishable, :shelf_life_days)
                    """),
                    product
                )
            
            for store in stores_data:
                conn.execute(
                    text("""
                        INSERT INTO source_stores VALUES 
                        (:location_id, :store_name, :store_type, :address, 
                         :city, :state, :country, :zone, :storage_capacity)
                    """),
                    store
                )
            
            for supplier in suppliers_data:
                conn.execute(
                    text("""
                        INSERT INTO source_suppliers VALUES 
                        (:supplier_id, :supplier_name, :contact_person, :contact_email, 
                         :phone, :address, :city, :country, :supply_category, :lead_time_days)
                    """),
                    supplier
                )
            
            # Generar datos de inventario
            start_date = datetime(2023, 1, 1)
            end_date = datetime(2023, 12, 31)
            
            for _ in range(1000):  # 1000 registros de inventario
                date = start_date + timedelta(days=random.randint(0, 364))
                product = random.choice(products_data)
                store = random.choice(stores_data)
                supplier = random.choice(suppliers_data)
                
                quantity = random.randint(10, 1000)
                conn.execute(
                    text("""
                        INSERT INTO source_inventory 
                        (product_id, location_id, supplier_id, transaction_date, 
                         quantity_on_hand, unit_cost, minimum_stock, maximum_stock, 
                         reorder_point, units_sold, units_received)
                        VALUES 
                        (:product_id, :location_id, :supplier_id, :transaction_date,
                         :quantity_on_hand, :unit_cost, :minimum_stock, :maximum_stock,
                         :reorder_point, :units_sold, :units_received)
                    """),
                    {
                        'product_id': product['product_id'],
                        'location_id': store['location_id'],
                        'supplier_id': supplier['supplier_id'],
                        'transaction_date': date,
                        'quantity_on_hand': quantity,
                        'unit_cost': round(product['retail_price'] * 0.7, 2),
                        'minimum_stock': int(quantity * 0.2),
                        'maximum_stock': int(quantity * 2),
                        'reorder_point': int(quantity * 0.3),
                        'units_sold': random.randint(0, int(quantity * 0.5)),
                        'units_received': random.randint(0, int(quantity * 0.3))
                    }
                )
            
            conn.commit()

    def create_dw_tables(self):
        """
        Crea las tablas del Data Warehouse
        """
        with self.engine.connect() as conn:
            # Dimensión Producto
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS dim_product (
                    product_key SERIAL PRIMARY KEY,
                    product_id VARCHAR(10),
                    product_name VARCHAR(100),
                    product_description TEXT,
                    category VARCHAR(50),
                    subcategory VARCHAR(50),
                    brand VARCHAR(50),
                    unit_measure VARCHAR(20),
                    retail_price DECIMAL(10,2),
                    perishable BOOLEAN,
                    shelf_life_days INTEGER
                )
            """))
            
            # Dimensión Ubicación
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS dim_location (
                    location_key SERIAL PRIMARY KEY,
                    location_id VARCHAR(10),
                    store_name VARCHAR(100),
                    store_type VARCHAR(50),
                    address VARCHAR(200),
                    city VARCHAR(100),
                    state VARCHAR(50),
                    country VARCHAR(50),
                    zone VARCHAR(50),
                    storage_capacity INTEGER
                )
            """))
            
            # Dimensión Fecha
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS dim_date (
                    date_key INTEGER PRIMARY KEY,
                    full_date DATE,
                    year INTEGER,
                    quarter INTEGER,
                    month INTEGER,
                    month_name VARCHAR(20),
                    week INTEGER,
                    day INTEGER,
                    day_name VARCHAR(20),
                    is_holiday BOOLEAN,
                    season VARCHAR(20)
                )
            """))
            
            # Dimensión Proveedor
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS dim_supplier (
                    supplier_key SERIAL PRIMARY KEY,
                    supplier_id VARCHAR(10),
                    supplier_name VARCHAR(100),
                    contact_person VARCHAR(100),
                    contact_email VARCHAR(100),
                    phone VARCHAR(20),
                    address VARCHAR(200),
                    city VARCHAR(100),
                    country VARCHAR(50),
                    supply_category VARCHAR(50),
                    lead_time_days INTEGER
                )
            """))
            
            # Tabla de Hechos Inventario
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS fact_inventory (
                    inventory_key SERIAL PRIMARY KEY,
                    product_key INTEGER REFERENCES dim_product(product_key),
                    location_key INTEGER REFERENCES dim_location(location_key),
                    date_key INTEGER REFERENCES dim_date(date_key),
                    supplier_key INTEGER REFERENCES dim_supplier(supplier_key),
                    quantity_on_hand INTEGER,
                    unit_cost DECIMAL(10,2),
                    total_value DECIMAL(10,2),
                    minimum_stock_level INTEGER,
                    maximum_stock_level INTEGER,
                    reorder_point INTEGER,
                    units_sold INTEGER,
                    units_received INTEGER
                )
            """))
            
            conn.commit()

    def run_setup(self):
        """
        Ejecuta todo el proceso de configuración
        """
        print("1. Creando tablas fuente...")
        self.create_source_tables()
        
        print("2. Generando datos de ejemplo...")
        self.generate_sample_data()
        
        print("3. Creando tablas del Data Warehouse...")
        self.create_dw_tables()
        
        print("Setup completado! Ahora puedes ejecutar el proceso ETL.")

if __name__ == "__main__":
    setup = InventoryETLSetup()
    setup.run_setup()
