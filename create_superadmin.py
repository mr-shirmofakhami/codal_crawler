# create_superadmin.py
from sqlalchemy.orm import Session
from database import SessionLocal, engine
from models import Base, User
from auth.auth import AuthService

# Create tables
Base.metadata.create_all(bind=engine)


def create_superadmin():
    db = SessionLocal()

    try:
        # Check if superadmin already exists
        existing_admin = db.query(User).filter(User.is_superadmin == True).first()
        if existing_admin:
            print("✅ Superadmin already exists!")
            print(f"Username: {existing_admin.username}")
            return

        # Create superadmin
        hashed_password = AuthService.get_password_hash("admin123")  # Change this password!
        superadmin = User(
            username="admin",
            email="admin@company.com",
            hashed_password=hashed_password,
            is_active=True,
            is_superadmin=True
        )

        db.add(superadmin)
        db.commit()
        print("✅ Superadmin created successfully!")
        print("Username: admin")
        print("Password: admin123")
        print("⚠️  Please change the password after first login!")

    except Exception as e:
        print(f"❌ Error creating superadmin: {e}")
        db.rollback()
    finally:
        db.close()


if __name__ == "__main__":
    create_superadmin()