import fastapi
from yoyo import read_migrations
from yoyo import get_backend
from .sensors.controller import router as sensorsRouter

app = fastapi.FastAPI(title="Senser", version="0.1.0-alpha.1")

#TODO: Apply new TS migrations using Yoyo
#Read docs: https://ollycope.com/software/yoyo/latest/

DATABASE_URI = "postgresql://timescale:timescale@timescale:5433/timescale"

MIGRATIONS_DIR = "migrations_ts"

db = get_backend(DATABASE_URI)

migrations = read_migrations(MIGRATIONS_DIR)

with db.lock():
    db.apply_migrations(db.to_apply(migrations))
    db.rollback_migrations(db.to_rollback(migrations))


app.include_router(sensorsRouter)

@app.get("/")
def index():
    #Return the api name and version
    return {"name": app.title, "version": app.version}
