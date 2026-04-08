import os
import random
import time

from faker import Faker
from sqlalchemy import create_engine, text

# --- CONFIG ---
DB_HOST = os.getenv("DB_HOST", "almacen-datos")
DB_USER = os.getenv("DB_USER", "user_cbd")
DB_PASS = os.getenv("DB_PASS", "password_cbd")
DB_NAME = os.getenv("DB_NAME", "bd_proyecto_cbd")

DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_NAME}"

fake = Faker("es_ES")  # Data generator in spanish


def seed():
    print(f"🚀 Conectando a {DB_HOST}...")
    # Wait for DB to be ready (retry logic)
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
        print("🧹 Limpiando catálogo anterior...")
        conn.execute(text("DROP TABLE IF EXISTS films"))
        conn.execute(
            text("""
            CREATE TABLE films (
                id SERIAL PRIMARY KEY,
                title VARCHAR(255),
                genre VARCHAR(100),
                release_year INTEGER,
                rating DECIMAL(3,1),
                director VARCHAR(100),
                synopsis TEXT
            )
        """)
        )
        conn.commit()

        print("🎬 Generando datos aleatorios con Faker...")
        total_records = 100000
        batch_size = 6250

        genres = [
            "Acción",
            "Drama",
            "Comedia",
            "Ciencia Ficción",
            "Terror",
            "Documental",
            "Suspense",
            "Aventura",
        ]

        for i in range(0, total_records, batch_size):
            values = []
            for _ in range(batch_size):
                # Keep generated text raw and use parameterized SQL below.
                title = (
                    fake.sentence(nb_words=random.randint(2, 5))
                    .title()
                    .replace(".", "")
                )
                genre = random.choice(genres)
                release_year = random.randint(1950, 2026)
                rating = round(random.uniform(1.0, 10.0), 1)
                director = fake.name()
                synopsis = fake.paragraph(nb_sentences=3)

                values.append(
                    {
                        "title": title,
                        "genre": genre,
                        "release_year": release_year,
                        "rating": rating,
                        "director": director,
                        "synopsis": synopsis,
                    }
                )

            query = text("""
                INSERT INTO films (title, genre, release_year, rating, director, synopsis)
                VALUES (:title, :genre, :release_year, :rating, :director, :synopsis)
            """)

            conn.execute(query, values)
            conn.commit()
            percentage_completed = ((i + batch_size) / total_records) * 100
            print(
                f"\r🎲 Registrando películas... ({percentage_completed:.0f}%)",
                end="",
                flush=True,
            )

    engine.dispose()
    print("\n🎫 ¡Catálogo de 100,000 películas listo para el análisis!")


if __name__ == "__main__":
    seed()
