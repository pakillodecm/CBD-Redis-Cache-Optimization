import os
import random
import time

from sqlalchemy import create_engine, text

# --- CONFIGURACIÓN ---
DB_HOST = os.getenv("DB_HOST", "almacen-datos")
DB_USER = "user_cbd"
DB_PASS = "password_cbd"
DB_NAME = "bd_proyecto_cbd"

DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_NAME}"


def seed():
    print(f"🚀 Conectando a {DB_HOST}...")
    # Reintento simple por si la DB aún está arrancando
    engine = None
    for i in range(10):
        try:
            engine = create_engine(DB_URL)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            break
        except Exception:
            print(f"⌛ Esperando a la base de datos... ({i + 1}/10)")
            time.sleep(2)

    if not engine:
        print("❌ No se pudo conectar a la base de datos.")
        return

    with engine.connect() as conn:
        print("🧹 Limpiando tabla anterior...")
        conn.execute(text("DROP TABLE IF EXISTS productos"))
        conn.execute(
            text("""
            CREATE TABLE productos (
                id SERIAL PRIMARY KEY,
                nombre VARCHAR(100),
                categoria VARCHAR(50),
                precio DECIMAL(10,2),
                descripcion TEXT
            )
        """)
        )
        conn.commit()

        print("📦 Insertando 100,000 registros en lotes...")
        total_records = 100000
        batch_size = 10000

        categorias = ["Electrónica", "Hogar", "Jardín", "Libros", "Ropa", "Deportes"]

        for i in range(0, total_records, batch_size):
            values = []
            for j in range(batch_size):
                num = i + j
                nombre = f"Producto Pro {num}"
                cat = random.choice(categorias)
                precio = round(random.uniform(5.0, 1500.0), 2)
                desc = (
                    f"Esta es una descripción detallada para el producto {num}. " * 10
                )
                values.append(f"('{nombre}', '{cat}', {precio}, '{desc}')")

            # Inserción masiva para ganar velocidad
            query = f"INSERT INTO productos (nombre, categoria, precio, descripcion) VALUES {','.join(values)}"
            conn.execute(text(query))
            conn.commit()
            print(f"✅ Registros insertados: {i + batch_size}")

    print("\n✨ ¡Base de datos poblada con éxito!")


if __name__ == "__main__":
    seed()
