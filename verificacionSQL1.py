import psycopg2
import pandas as pd
from sqlalchemy import create_engine

def verify_etl():
    connection_string = "postgresql://usuario:contraseña_segura@localhost:5432/mi_base_datos"
    engine = create_engine(connection_string)

    queries = {
        "Conteo de productos": "SELECT COUNT(*) FROM dim_product",
        "Conteo de ubicaciones": "SELECT COUNT(*) FROM dim_location",
        "Conteo de proveedores": "SELECT COUNT(*) FROM dim_supplier",
        "Conteo de fechas": "SELECT COUNT(*) FROM dim_date",
        "Conteo de registros de inventario": "SELECT COUNT(*) FROM fact_inventory",
        "Muestra de productos": "SELECT * FROM dim_product LIMIT 5",
        "Muestra de inventario": "SELECT * FROM fact_inventory LIMIT 5",
        "Integridad referencial": """
            SELECT COUNT(*)
            FROM fact_inventory f
            LEFT JOIN dim_product p ON f.product_key = p.product_key
            LEFT JOIN dim_location l ON f.location_key = l.location_key
            LEFT JOIN dim_supplier s ON f.supplier_key = s.supplier_key
            LEFT JOIN dim_date d ON f.date_key = d.date_key
            WHERE p.product_key IS NULL OR l.location_key IS NULL OR s.supplier_key IS NULL OR d.date_key IS NULL
        """
    }

    with engine.connect() as conn:
        for description, query in queries.items():
            result = pd.read_sql(query, conn)
            print(f"\n{description}:")
            print(result)

if __name__ == "__main__":
    verify_etl()
