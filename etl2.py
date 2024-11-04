import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import numpy as np

class InventoryETL:
    def __init__(self):
        """
        Inicializa la conexión a PostgreSQL usando los parámetros del docker-compose
        """
        self.connection_string = "postgresql://usuario:contraseña_segura@localhost:5432/mi_base_datos"
        self.engine = create_engine(self.connection_string)

    def validate_columns(self): # New
        """
        Valida que las columnas coincidan antes de la carga
        """
        with self.engine.connect() as conn:
            # Obtener columnas de la tabla destino
            table_columns = pd.read_sql("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'dim_product'
            """, conn)['column_name'].tolist()

            # Verificar que todas las columnas necesarias existen
            missing_columns = set(self.products_df.columns) - set(table_columns)
            if missing_columns:
                raise ValueError(f"Columnas faltantes en la tabla destino: {missing_columns}")

    def extract_source_data(self):
        """
        Extrae datos de las tablas fuente
        """
        print("Extrayendo datos de las tablas fuente...")

        with self.engine.connect() as conn:
            # Extraer datos de productos
            self.products_df = pd.read_sql_query(
                "SELECT * FROM source_products",
                conn
            )
            print(f"Productos extraídos: {len(self.products_df)}")

            # Extraer datos de ubicaciones
            self.locations_df = pd.read_sql_query(
                "SELECT * FROM source_stores",
                conn
            )
            print(f"Ubicaciones extraídas: {len(self.locations_df)}")

            # Extraer datos de proveedores
            self.suppliers_df = pd.read_sql_query(
                "SELECT * FROM source_suppliers",
                conn
            )
            print(f"Proveedores extraídos: {len(self.suppliers_df)}")

            # Extraer datos de inventario
            self.inventory_df = pd.read_sql_query(
                "SELECT * FROM source_inventory",
                conn
            )
            print(f"Registros de inventario extraídos: {len(self.inventory_df)}")

    def transform_date_dimension(self):
        """
        Crea y transforma la dimensión de fecha
        """
        print("Transformando dimensión fecha...")

        # Obtener rango de fechas del inventario
        min_date = self.inventory_df['transaction_date'].min()
        max_date = self.inventory_df['transaction_date'].max()

        # Generar todas las fechas en el rango
        dates = pd.date_range(start=min_date, end=max_date, freq='D')

        self.dates_df = pd.DataFrame({
            'full_date': dates,
            'year': dates.year,
            'quarter': dates.quarter,
            'month': dates.month,
            'month_name': dates.strftime('%B'),
            'week': dates.isocalendar().week,
            'day': dates.day,
            'day_name': dates.strftime('%A'),
            'is_holiday': False,  # Simplificado para el ejemplo
            'season': dates.month.map({
                12:'Verano', 1:'Verano', 2:'Verano',
                3:'Otoño', 4:'Otoño', 5:'Otoño',
                6:'Invierno', 7:'Invierno', 8:'Invierno',
                9:'Primavera', 10:'Primavera', 11:'Primavera'
            })
        })

        # Crear date_key como YYYYMMDD
        self.dates_df['date_key'] = self.dates_df['full_date'].dt.strftime('%Y%m%d').astype(int)
        print(f"Fechas generadas: {len(self.dates_df)}")

    def transform_dimensions(self):
        """
        Transforma las dimensiones aplicando limpieza y reglas de negocio
        """
        print("Transformando dimensiones...")

        # Transformar dimensión producto
        self.products_df = self.products_df.rename(columns={
            'description': 'product_description'  # Renombrar la columna para que coincida
        })
        self.products_df['product_name'] = self.products_df['product_name'].str.upper()
        self.products_df['category'] = self.products_df['category'].str.title()
        #self.products_df['product_key'] = range(1, len(self.products_df) + 1)
        self.products_df['product_key'] = self.products_df.index + 1
        print(f"Rango de product_keys generados: {self.products_df['product_key'].min()} - {self.products_df['product_key'].max()}")

        # Transformar dimensión ubicación
        self.locations_df['city'] = self.locations_df['city'].str.title()
        self.locations_df['country'] = self.locations_df['country'].str.upper()
        self.locations_df['location_key'] = range(1, len(self.locations_df) + 1)

        # Transformar dimensión proveedor
        self.suppliers_df['supplier_name'] = self.suppliers_df['supplier_name'].str.upper()
        self.suppliers_df['contact_email'] = self.suppliers_df['contact_email'].str.lower()
        self.suppliers_df['supplier_key'] = range(1, len(self.suppliers_df) + 1)

    def transform_facts(self):
        """
        Transforma la tabla de hechos aplicando las claves subrogadas y cálculos
        """
        print("Transformando tabla de hechos...")

        # Crear date_key para la tabla de hechos
        self.inventory_df['date_key'] = pd.to_datetime(
            self.inventory_df['transaction_date']
        ).dt.strftime('%Y%m%d').astype(int)

        # Merge con dimensiones para obtener las claves subrogadas
        self.fact_inventory = self.inventory_df.merge(
            self.products_df[['product_id', 'product_key']],
            on='product_id'
        ).merge(
            self.locations_df[['location_id', 'location_key']],
            on='location_id'
        ).merge(
            self.suppliers_df[['supplier_id', 'supplier_key']],
            on='supplier_id'
        )

        # Calcular total_value
        self.fact_inventory['total_value'] = (
            self.fact_inventory['quantity_on_hand'] *
            self.fact_inventory['unit_cost']
        )

        # Seleccionar y renombrar columnas finales
        self.fact_inventory = self.fact_inventory[[
            'product_key', 'location_key', 'date_key', 'supplier_key',
            'quantity_on_hand', 'unit_cost', 'total_value',
            'minimum_stock', 'maximum_stock', 'reorder_point',
            'units_sold', 'units_received'
        ]].rename(columns={
            'minimum_stock': 'minimum_stock_level',
            'maximum_stock': 'maximum_stock_level'
        })

        print(f"Registros de hechos transformados: {len(self.fact_inventory)}")

        # -------------
        # Obtén todos los product_key válidos de dim_product
        with self.engine.connect() as conn:
            valid_product_keys = pd.read_sql("SELECT product_key FROM dim_product", conn)['product_key'].tolist()

        # Filtra fact_inventory para incluir solo product_key válidos
        self.fact_inventory = self.fact_inventory[self.fact_inventory['product_key'].isin(valid_product_keys)]

        print(f"Registros de hechos transformados y filtrados: {len(self.fact_inventory)}")


    def load_dimensions(self):
        """
        Carga las dimensiones en el data warehouse
        """
        print("Cargando dimensiones...")

        self.validate_columns()

        with self.engine.connect() as conn:
            # Limpiar tablas dimensionales
            conn.execute(text("TRUNCATE TABLE fact_inventory CASCADE"))
            conn.execute(text("TRUNCATE TABLE dim_product CASCADE"))
            conn.execute(text("TRUNCATE TABLE dim_location CASCADE"))
            conn.execute(text("TRUNCATE TABLE dim_date CASCADE"))
            conn.execute(text("TRUNCATE TABLE dim_supplier CASCADE"))

            # Cargar dimensiones
            self.products_df.to_sql('dim_product', conn, if_exists='append', index=False)
            print(f"Productos cargados: {len(self.products_df)}")

            self.locations_df.to_sql('dim_location', conn, if_exists='append', index=False)
            print(f"Ubicaciones cargadas: {len(self.locations_df)}")

            self.dates_df.to_sql('dim_date', conn, if_exists='append', index=False)
            print(f"Fechas cargadas: {len(self.dates_df)}")

            self.suppliers_df.to_sql('dim_supplier', conn, if_exists='append', index=False)
            print(f"Proveedores cargados: {len(self.suppliers_df)}")

            # Verificar que los datos se cargaron correctamente
            dim_product_count = pd.read_sql("SELECT COUNT(*) FROM dim_product", conn).iloc[0,0]
            print(f"Número de registros en dim_product después de la carga: {dim_product_count}")

    def load_facts(self):
        """
        Carga la tabla de hechos en el data warehouse
        """
        print("Cargando tabla de hechos...")

    # Verifica si hay registros para cargar
        if len(self.fact_inventory) == 0:
            print("No hay registros válidos para cargar en fact_inventory")
            return

        with self.engine.connect() as conn:
            # Intenta cargar los datos en lotes más pequeños
            chunk_size = 100  # Ajusta este valor según sea necesario
            for i in range(0, len(self.fact_inventory), chunk_size):
                chunk = self.fact_inventory.iloc[i:i+chunk_size]
                chunk.to_sql('fact_inventory', conn, if_exists='append', index=False)

            print(f"Registros de hechos cargados: {len(self.fact_inventory)}")

    def validate_data(self):
        """
        Realiza validaciones básicas de los datos cargados
        """
        print("\nValidando datos cargados...")

        with self.engine.connect() as conn:
            # Verificar conteos
            dim_product_count = pd.read_sql("SELECT COUNT(*) FROM dim_product", conn).iloc[0,0]
            dim_location_count = pd.read_sql("SELECT COUNT(*) FROM dim_location", conn).iloc[0,0]
            dim_supplier_count = pd.read_sql("SELECT COUNT(*) FROM dim_supplier", conn).iloc[0,0]
            fact_count = pd.read_sql("SELECT COUNT(*) FROM fact_inventory", conn).iloc[0,0]

            print(f"""
            Resumen de datos cargados:
            - Productos: {dim_product_count}
            - Ubicaciones: {dim_location_count}
            - Proveedores: {dim_supplier_count}
            - Registros de inventario: {fact_count}
            """)

            # Verificar integridad referencial
            orphan_products = pd.read_sql("""
                SELECT COUNT(*) FROM fact_inventory f
                LEFT JOIN dim_product p ON f.product_key = p.product_key
                WHERE p.product_key IS NULL
            """, conn).iloc[0,0]

            if orphan_products == 0:
                print("✓ Integridad referencial verificada")
            else:
                print("✗ Se encontraron problemas de integridad referencial")

    def run_etl(self):
        """
        Ejecuta el proceso ETL completo
        """
        try:
            print("Iniciando proceso ETL...")

            # Extracción
            self.extract_source_data()

            # Transformación
            self.transform_date_dimension()
            self.transform_dimensions()
            self.transform_facts()

            # Carga
            self.load_dimensions()
            self.load_facts()

            # Validación
            self.validate_data()

            print("\nProceso ETL completado exitosamente!")

        except Exception as e:
            print(f"Error en el proceso ETL: {str(e)}")
            raise

# Ejecutar el ETL
if __name__ == "__main__":
    etl = InventoryETL()
    etl.run_etl()
