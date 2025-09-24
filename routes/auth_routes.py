# # /routes/auth_routes.py
# from fastapi import APIRouter, Depends, HTTPException, status
# from sqlalchemy.orm import Session
# from datetime import datetime, timedelta
# from typing import List
#
# from database import get_db
# from models import User
# from schemas.schemas import Token, LoginRequest, UserCreate, UserResponse
# from auth.auth import AuthService, get_current_user, get_current_superadmin
#
# router = APIRouter()
#
#
# @router.post("/login", response_model=Token)
# async def login(login_data: LoginRequest, db: Session = Depends(get_db)):
#     # Find user
#     user = db.query(User).filter(User.username == login_data.username).first()
#
#     if not user or not AuthService.verify_password(login_data.password, user.hashed_password):
#         raise HTTPException(
#             status_code=status.HTTP_401_UNAUTHORIZED,
#             detail="نام کاربری یا رمز عبور اشتباه است"
#         )
#
#     if not user.is_active:
#         raise HTTPException(
#             status_code=status.HTTP_401_UNAUTHORIZED,
#             detail="حساب کاربری غیرفعال است"
#         )
#
#     # Update last login
#     user.last_login = datetime.utcnow()
#     db.commit()
#
#     # Create access token
#     access_token_expires = timedelta(minutes=30)
#     access_token = AuthService.create_access_token(
#         data={"sub": user.username}, expires_delta=access_token_expires
#     )
#
#     return {"access_token": access_token, "token_type": "bearer"}
#
#
# @router.get("/me", response_model=UserResponse)
# async def get_current_user_info(current_user: User = Depends(get_current_user)):
#     return current_user
#
#
# @router.post("/users", response_model=UserResponse)
# async def create_user(
#         user_data: UserCreate,
#         db: Session = Depends(get_db),
#         current_user: User = Depends(get_current_superadmin)
# ):
#     # Check if user already exists
#     if db.query(User).filter(User.username == user_data.username).first():
#         raise HTTPException(
#             status_code=status.HTTP_400_BAD_REQUEST,
#             detail="این نام کاربری قبلاً ثبت شده است"
#         )
#
#     if db.query(User).filter(User.email == user_data.email).first():
#         raise HTTPException(
#             status_code=status.HTTP_400_BAD_REQUEST,
#             detail="این ایمیل قبلاً ثبت شده است"
#         )
#
#     # Create new user
#     hashed_password = AuthService.get_password_hash(user_data.password)
#     db_user = User(
#         username=user_data.username,
#         email=user_data.email,
#         hashed_password=hashed_password,
#         is_active=True,
#         is_superadmin=False
#     )
#
#     db.add(db_user)
#     db.commit()
#     db.refresh(db_user)
#
#     return db_user
#
#
# @router.get("/users", response_model=List[UserResponse])
# async def get_all_users(
#         db: Session = Depends(get_db),
#         current_user: User = Depends(get_current_superadmin)
# ):
#     users = db.query(User).all()
#     return users
#
#
# @router.put("/users/{user_id}/toggle-active")
# async def toggle_user_active(
#         user_id: int,
#         db: Session = Depends(get_db),
#         current_user: User = Depends(get_current_superadmin)
# ):
#     user = db.query(User).filter(User.id == user_id).first()
#     if not user:
#         raise HTTPException(
#             status_code=status.HTTP_404_NOT_FOUND,
#             detail="کاربر یافت نشد"
#         )
#
#     user.is_active = not user.is_active
#     db.commit()
#
#     status_text = "فعال" if user.is_active else "غیرفعال"
#     return {"message": f"کاربر با موفقیت {status_text} شد"}
#
#
# @router.delete("/users/{user_id}")
# async def delete_user(
#         user_id: int,
#         db: Session = Depends(get_db),
#         current_user: User = Depends(get_current_superadmin)
# ):
#     user = db.query(User).filter(User.id == user_id).first()
#     if not user:
#         raise HTTPException(
#             status_code=status.HTTP_404_NOT_FOUND,
#             detail="کاربر یافت نشد"
#         )
#
#     if user.id == current_user.id:
#         raise HTTPException(
#             status_code=status.HTTP_400_BAD_REQUEST,
#             detail="نمی‌توانید خودتان را حذف کنید"
#         )
#
#     db.delete(user)
#     db.commit()
#
#     return {"message": "کاربر با موفقیت حذف شد"}


# /routes/auth_routes.py
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
from typing import List

from database import get_db
from models import User
from schemas.schemas import Token, LoginRequest, UserCreate, UserResponse, UserUpdate, PasswordChange
from auth.auth import AuthService, get_current_user, get_current_superadmin

router = APIRouter()


@router.post("/login", response_model=Token)
async def login(login_data: LoginRequest, db: Session = Depends(get_db)):
    # Find user
    user = db.query(User).filter(User.username == login_data.username).first()

    if not user or not AuthService.verify_password(login_data.password, user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="نام کاربری یا رمز عبور اشتباه است"
        )

    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="حساب کاربری غیرفعال است"
        )

    # Update last login
    user.last_login = datetime.utcnow()
    db.commit()

    # Create access token
    access_token_expires = timedelta(minutes=30)
    access_token = AuthService.create_access_token(
        data={"sub": user.username}, expires_delta=access_token_expires
    )

    return {"access_token": access_token, "token_type": "bearer"}


@router.get("/me", response_model=UserResponse)
async def get_current_user_info(current_user: User = Depends(get_current_user)):
    return current_user


# ============================================================================
# USER MANAGEMENT ROUTES (SUPERADMIN ONLY)
# ============================================================================

@router.post("/users", response_model=UserResponse)
async def create_user(
        user_data: UserCreate,
        db: Session = Depends(get_db),
        current_user: User = Depends(get_current_superadmin)
):
    # Check if user already exists
    if db.query(User).filter(User.username == user_data.username).first():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="این نام کاربری قبلاً ثبت شده است"
        )

    if db.query(User).filter(User.email == user_data.email).first():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="این ایمیل قبلاً ثبت شده است"
        )

    # Create new user
    hashed_password = AuthService.get_password_hash(user_data.password)
    db_user = User(
        username=user_data.username,
        email=user_data.email,
        hashed_password=hashed_password,
        is_active=True,
        is_superadmin=user_data.is_superadmin  # Use the provided value
    )

    db.add(db_user)
    db.commit()
    db.refresh(db_user)

    return db_user


@router.get("/users", response_model=List[UserResponse])
async def get_all_users(
        db: Session = Depends(get_db),
        current_user: User = Depends(get_current_superadmin)
):
    users = db.query(User).all()
    return users


@router.get("/users/{user_id}", response_model=UserResponse)
async def get_user_by_id(
        user_id: int,
        db: Session = Depends(get_db),
        current_user: User = Depends(get_current_superadmin)
):
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="کاربر یافت نشد"
        )
    return user


@router.put("/users/{user_id}", response_model=dict)
async def update_user(
        user_id: int,
        user_update: UserUpdate,
        db: Session = Depends(get_db),
        current_user: User = Depends(get_current_superadmin)
):
    # Find user
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="کاربر یافت نشد"
        )

    # Prevent superadmin from deactivating themselves
    if user_id == current_user.id and user_update.is_active is False:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="نمی‌توانید خودتان را غیرفعال کنید"
        )

    # Update fields
    if user_update.username is not None:
        # Check if new username already exists
        existing = db.query(User).filter(
            User.username == user_update.username,
            User.id != user_id
        ).first()
        if existing:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="این نام کاربری قبلاً ثبت شده است"
            )
        user.username = user_update.username

    if user_update.email is not None:
        # Check if new email already exists
        existing = db.query(User).filter(
            User.email == user_update.email,
            User.id != user_id
        ).first()
        if existing:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="این ایمیل قبلاً ثبت شده است"
            )
        user.email = user_update.email

    if user_update.is_active is not None:
        user.is_active = user_update.is_active

    if user_update.is_superadmin is not None:
        user.is_superadmin = user_update.is_superadmin

    db.commit()
    db.refresh(user)

    return {
        "message": "اطلاعات کاربر با موفقیت بروزرسانی شد",
        "user_id": user.id
    }


@router.put("/users/{user_id}/toggle-active", response_model=dict)
async def toggle_user_active(
        user_id: int,
        db: Session = Depends(get_db),
        current_user: User = Depends(get_current_superadmin)
):
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="کاربر یافت نشد"
        )

    # Prevent superadmin from deactivating themselves
    if user_id == current_user.id and user.is_active:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="نمی‌توانید خودتان را غیرفعال کنید"
        )

    user.is_active = not user.is_active
    db.commit()

    status_text = "فعال" if user.is_active else "غیرفعال"
    return {"message": f"کاربر با موفقیت {status_text} شد"}


@router.post("/users/{user_id}/change-password", response_model=dict)
async def change_user_password(
        user_id: int,
        password_data: PasswordChange,
        db: Session = Depends(get_db),
        current_user: User = Depends(get_current_superadmin)
):
    # Find user
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="کاربر یافت نشد"
        )

    # Hash new password
    hashed_password = AuthService.get_password_hash(password_data.new_password)
    user.hashed_password = hashed_password

    db.commit()

    return {
        "message": f"رمز عبور کاربر {user.username} با موفقیت تغییر یافت"
    }


@router.delete("/users/{user_id}", response_model=dict)
async def delete_user(
        user_id: int,
        db: Session = Depends(get_db),
        current_user: User = Depends(get_current_superadmin)
):
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="کاربر یافت نشد"
        )

    if user.id == current_user.id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="نمی‌توانید خودتان را حذف کنید"
        )

    db.delete(user)
    db.commit()

    return {
        "message": f"کاربر {user.username} با موفقیت حذف شد",
        "deleted_user_id": user_id
    }