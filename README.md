# CBD: Optimización de rendimiento mediante el patrón Cache-aside utilizando Redis sobre un motor relacional

Este proyecto es una Prueba de Concepto (PoC) para la asignatura Complementos de Bases de Datos. Su objetivo es demostrar la reducción de latencia mediante el patrón Cache-Aside con Redis sobre PostgreSQL.

## 🛠️ Manual de Despliegue e Instalación

Para ejecutar este entorno es necesario tener instalado Docker Desktop.

### 1. Levantar la infraestructura
Construye y arranca los servicios en segundo plano desde la raíz del proyecto:

    docker compose up -d --build

### 2. Población de la Base de Datos
Inyecta los 100.000 registros sintéticos ejecutando el script de seeding:

    docker compose exec app python seed.py

### 3. Acceso al sistema
* Frontend (Dashboard): http://localhost:8501
* Backend (API Docs): http://localhost:8000/docs

---

## 📖 Manual de Usuario

La interfaz de Streamlit permite interactuar con el sistema a través de tres pestañas:

1. **Explorar:** Realizar búsquedas por ID, Texto o Género, con indicación posterior del origen de obtención del dato (DB o Caché).
2. **Gestión (CUD):** CRUD de películas con invalidación de caché asíncrona tras cada cambio.
3. **Telemetría:** Análisis de pruebas (con batería automatizaza que ejecuta 24 peticiones secuenciales) con métricas de aceleración.

---

## 📄 Tecnologías Utilizadas
* PostgreSQL 15 (Persistencia)
* Redis 7 (Caché RAM)
* FastAPI (Backend asíncrono)
* Streamlit (Frontend y Telemetría)
* Docker & Docker Compose (Orquestación)