from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base

# PostgreSQL connection
DATABASE_URL = "postgresql://postgres:123@localhost/codal"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Create tables
Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()