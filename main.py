# ═══════════════════════════════════════════════════════════════════════
# ILT PLATFORM — MIKEY CDMX BACKEND v2.0
# FastAPI + PostgreSQL + Redis
# © 2026 James-Michael Prieto Corbin / Infinite Logistic Technologies
# Patent pending. All rights reserved.
#
# DEPLOYMENT:
#   pip install -r requirements.txt
#   uvicorn main:app --host 0.0.0.0 --port 8000
#
# ENV VARS (.env — never commit this file):
#   DATABASE_URL=postgresql://user:pass@localhost/ilt_cdmx
#   REDIS_URL=redis://localhost:6379
#   STRIPE_SECRET_KEY=sk_live_...
#   STRIPE_WEBHOOK_SECRET=whsec_...
#   JAMES_EMAIL=james@luxtourtravel.com
#   OPS_EMAIL=ops@mexicocityblackcar.com
#   O365_SMTP_HOST=smtp.office365.com
#   O365_SMTP_PORT=587
#   O365_SMTP_USER=ops@mexicocityblackcar.com
#   O365_SMTP_PASSWORD=your-app-password
#   ANTHROPIC_API_KEY=sk-ant-...
#   JWT_SECRET=your-long-random-secret
# ═══════════════════════════════════════════════════════════════════════

import os
import json
import random
import stripe
import anthropic
from passlib.context import CryptContext
from jose import JWTError, jwt
from fastapi.security import OAuth2PasswordBearer
from datetime import timedelta, datetime, timezone
from typing import Optional, List
from enum import Enum

import redis.asyncio as aioredis
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request, BackgroundTasks, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel
from sqlalchemy import Column, String, Float, Integer, Boolean, DateTime, Text, ForeignKey
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.future import select

try:
    from email_o365 import router as email_router
    EMAIL_ENABLED = True
except Exception as e:
    email_router = None
    EMAIL_ENABLED = False
    print(f"[ILT] email_o365 not loaded: {e}")

load_dotenv()

# ─── CONFIG ────────────────────────────────────────────────────────────
DATABASE_URL       = os.getenv("DATABASE_URL", "postgresql://postgres:password@localhost/ilt_cdmx")
REDIS_URL          = os.getenv("REDIS_URL", "redis://localhost:6379")
STRIPE_SECRET      = os.getenv("STRIPE_SECRET_KEY", "")
STRIPE_WEBHOOK_SEC = os.getenv("STRIPE_WEBHOOK_SECRET", "")
JAMES_EMAIL        = os.getenv("JAMES_EMAIL", "james@luxtourtravel.com")
OPS_EMAIL          = os.getenv("OPS_EMAIL", "ops@mexicocityblackcar.com")
ANTHROPIC_API_KEY  = os.getenv("ANTHROPIC_API_KEY", "")
JWT_SECRET         = os.getenv("JWT_SECRET", "ilt-change-this-secret")
JWT_ALGORITHM      = "HS256"
JWT_EXPIRE_MINUTES = 60 * 24 * 7  # 7 days

stripe.api_key   = STRIPE_SECRET
anthropic_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
pwd_ctx          = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme    = OAuth2PasswordBearer(tokenUrl="/auth/login", auto_error=False)

# ─── DATABASE ──────────────────────────────────────────────────────────
engine            = create_async_engine(DATABASE_URL, echo=False)
AsyncSessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
Base              = declarative_base()

# ─── REDIS ─────────────────────────────────────────────────────────────
redis_client: aioredis.Redis = None

# ─── APP ───────────────────────────────────────────────────────────────
app = FastAPI(
    title="ILT Mikey CDMX API",
    description="Booking, CRM, dispatch, and Stripe webhook handler for Mexico City Black Car",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://www.mexicocityblackcar.com",
        "https://mexicocityblackcar.com",
        "https://www.luxtourtravel.com",
        "http://localhost:3000",
        "https://ops.mexicocityblackcar.com",
        "https://driver.mexicocityblackcar.com",
        "https://login.mexicocityblackcar.com",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

if email_router:
    app.include_router(email_router)

# ─── STATIC HTML ───────────────────────────────────────────────────────
@app.get("/mikey.html", include_in_schema=False)
async def serve_mikey_html():
    return FileResponse("mikey.html")

@app.get("/mikey", include_in_schema=False)
async def serve_mikey():
    return FileResponse("mikey.html")


# ═══════════════════════════════════════════════════════════════════════
# MODELS
# ═══════════════════════════════════════════════════════════════════════

class BookingStatus(str, Enum):
    QUOTE      = "quote"
    CONFIRMED  = "confirmed"
    PAID       = "paid"
    DISPATCHED = "dispatched"
    COMPLETED  = "completed"
    CANCELLED  = "cancelled"


class Client(Base):
    __tablename__ = "clients"

    id                   = Column(String(36), primary_key=True, default=lambda: str(__import__('uuid').uuid4()))
    email                = Column(String(255), unique=True, index=True, nullable=False)
    name                 = Column(String(255), nullable=True)
    phone                = Column(String(50), nullable=True)
    preferred_lang       = Column(String(10), default="en")
    pax_typical          = Column(Integer, default=1)
    is_vip               = Column(Boolean, default=False)
    vip_reason           = Column(Text, nullable=True)
    source               = Column(String(50), default="mikey-cdmx")
    market               = Column(String(20), default="cdmx")
    total_lifetime_spend = Column(Float, default=0.0)
    booking_count        = Column(Integer, default=0)
    last_seen            = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    created_at           = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    notes                = Column(Text, nullable=True)

    bookings = relationship("Booking", back_populates="client", lazy="selectin")

    def to_memory_string(self) -> str:
        parts = [f"Client {self.name or 'unknown'}"]
        if self.is_vip:
            parts.append("VIP client")
        if self.booking_count:
            parts.append(f"{self.booking_count} prior bookings, lifetime spend ${self.total_lifetime_spend:.0f}")
        if self.pax_typical and self.pax_typical > 1:
            parts.append(f"typically travels with {self.pax_typical} passengers")
        recent = sorted(self.bookings, key=lambda b: b.created_at or datetime.min, reverse=True)
        if recent:
            last = recent[0]
            parts.append(f"last booking: {last.service_type} · {last.vehicle_key} · ${last.total_usd:.0f}")
        if self.notes:
            parts.append(f"ops note: {self.notes}")
        return ". ".join(parts) + "."


class Booking(Base):
    __tablename__ = "bookings"

    id                    = Column(String(36), primary_key=True, default=lambda: str(__import__('uuid').uuid4()))
    confirmation_number   = Column(String(50), unique=True, index=True, nullable=False)
    client_id             = Column(String(36), ForeignKey("clients.id"), nullable=True)
    client_email          = Column(String(255), index=True, nullable=True)
    client_name           = Column(String(255), nullable=True)
    vehicle_key           = Column(String(20), nullable=True)
    service_type          = Column(String(30), nullable=True)
    hours                 = Column(Float, default=0)
    miles                 = Column(Float, default=0)
    total_usd             = Column(Float, nullable=False)
    session_total_usd     = Column(Float, default=0)
    status                = Column(String(20), default=BookingStatus.QUOTE)
    is_vip                = Column(Boolean, default=False)
    market                = Column(String(20), default="cdmx")
    driver_assigned       = Column(String(100), nullable=True)
    stripe_session_id     = Column(String(200), nullable=True)
    stripe_payment_intent = Column(String(200), nullable=True)
    paid_at               = Column(DateTime(timezone=True), nullable=True)
    dispatched_at         = Column(DateTime(timezone=True), nullable=True)
    completed_at          = Column(DateTime(timezone=True), nullable=True)
    notes                 = Column(Text, nullable=True)
    created_at            = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at            = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc),
                                   onupdate=lambda: datetime.now(timezone.utc))

    client        = relationship("Client", back_populates="bookings")
    dispatch_logs = relationship("DispatchLog", back_populates="booking", lazy="selectin")


class Driver(Base):
    __tablename__ = "drivers"

    id           = Column(String(36), primary_key=True, default=lambda: str(__import__('uuid').uuid4()))
    name         = Column(String(100), unique=True, nullable=False)
    initials     = Column(String(4), nullable=True)
    phone        = Column(String(50), nullable=True)
    whatsapp     = Column(String(50), nullable=True)
    specialty    = Column(String(200), nullable=True)
    zones        = Column(String(200), nullable=True)
    languages    = Column(String(100), default="en,es")
    vehicle_key  = Column(String(20), nullable=True)
    is_active    = Column(Boolean, default=True)
    is_available = Column(Boolean, default=True)
    current_lat  = Column(Float, nullable=True)
    current_lng  = Column(Float, nullable=True)
    last_gps_at  = Column(DateTime(timezone=True), nullable=True)
    created_at   = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    dispatch_logs = relationship("DispatchLog", back_populates="driver", lazy="selectin")


class DispatchLog(Base):
    __tablename__ = "dispatch_logs"

    id          = Column(String(36), primary_key=True, default=lambda: str(__import__('uuid').uuid4()))
    booking_id  = Column(String(36), ForeignKey("bookings.id"), nullable=False)
    driver_id   = Column(String(36), ForeignKey("drivers.id"), nullable=False)
    assigned_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    eta_minutes = Column(Integer, nullable=True)
    notes       = Column(Text, nullable=True)
    status      = Column(String(20), default="assigned")

    booking = relationship("Booking", back_populates="dispatch_logs")
    driver  = relationship("Driver", back_populates="dispatch_logs")


class ILTUser(Base):
    __tablename__ = "ilt_users"

    id            = Column(String(36), primary_key=True, default=lambda: str(__import__('uuid').uuid4()))
    email         = Column(String(255), unique=True, index=True, nullable=False)
    name          = Column(String(255), nullable=False)
    role          = Column(String(50),  nullable=False)
    password_hash = Column(String(255), nullable=False)
    is_active     = Column(Boolean, default=True)
    created_at    = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    last_login    = Column(DateTime(timezone=True), nullable=True)


# ═══════════════════════════════════════════════════════════════════════
# SCHEMAS
# ═══════════════════════════════════════════════════════════════════════

class ClientCreate(BaseModel):
    email:         str
    name:          Optional[str] = None
    phone:         Optional[str] = None
    pax:           Optional[int] = 1
    source:        Optional[str] = "mikey-cdmx"
    market:        Optional[str] = "cdmx"
    is_vip:        Optional[bool] = False
    session_total: Optional[float] = 0.0

class ClientResponse(BaseModel):
    id:                   str
    email:                str
    name:                 Optional[str]
    is_vip:               bool
    booking_count:        int
    total_lifetime_spend: float
    memory_string:        Optional[str] = None
    class Config:
        from_attributes = True

class BookingCreate(BaseModel):
    confirmation_number: str
    client_email:        Optional[str] = None
    client_name:         Optional[str] = None
    vehicle_key:         Optional[str] = None
    service_type:        Optional[str] = None
    total_usd:           float
    session_total:       Optional[float] = 0.0
    is_vip:              Optional[bool] = False
    market:              Optional[str] = "cdmx"
    status:              Optional[str] = "confirmed"

class BookingResponse(BaseModel):
    id:                  str
    confirmation_number: str
    status:              str
    total_usd:           float
    is_vip:              bool
    driver_assigned:     Optional[str]
    class Config:
        from_attributes = True

class DriverCreate(BaseModel):
    name:      str
    initials:  Optional[str] = None
    phone:     Optional[str] = None
    whatsapp:  Optional[str] = None
    specialty: Optional[str] = None
    zones:     Optional[str] = None
    languages: Optional[str] = "en,es"

class DriverUpdate(BaseModel):
    is_available: Optional[bool] = None
    current_lat:  Optional[float] = None
    current_lng:  Optional[float] = None

class DispatchRequest(BaseModel):
    booking_confirmation: str
    driver_name:          Optional[str] = None
    eta_minutes:          Optional[int] = None
    notes:                Optional[str] = None

class DispatchResponse(BaseModel):
    booking_confirmation: str
    driver_name:          str
    eta_minutes:          int
    vehicle:              Optional[str]

class VIPAlert(BaseModel):
    client_email:  Optional[str]
    client_name:   Optional[str]
    reason:        str
    session_total: Optional[float]
    market:        str = "cdmx"

class ChatRequest(BaseModel):
    messages: List[dict]
    system:   Optional[str] = ""

# ─── AUTH SCHEMAS ──────────────────────────────────────────────────────

VALID_ROLES = {"owner", "manager", "driver", "client", "affiliate"}

ROLE_SCOPES = {
    "driver":    ["jobs:own", "checkin:write", "schedule:own"],
    "manager":   ["jobs:all", "drivers:all", "clients:all",
                  "financials:read", "analytics:read"],
    "owner":     ["jobs:all", "drivers:all", "clients:all",
                  "financials:all", "analytics:all",
                  "affiliates:all", "settings:all", "users:manage"],
    "client":    ["bookings:own", "documents:own"],
    "affiliate": ["revenue:own", "referrals:own", "reports:own"],
}

class UserLogin(BaseModel):
    email:    str
    password: str
    role:     str

class UserCreate(BaseModel):
    email:    str
    password: str
    name:     str
    role:     str

class TokenResponse(BaseModel):
    access_token: str
    token_type:   str = "bearer"
    user_name:    str
    user_role:    str
    scopes:       List[str]
    expires_in:   int

class VerifyResponse(BaseModel):
    valid:      bool
    user_id:    str
    user_name:  str
    user_email: str
    user_role:  str
    scopes:     List[str]


# ═══════════════════════════════════════════════════════════════════════
# STARTUP / SHUTDOWN
# ═══════════════════════════════════════════════════════════════════════

@app.on_event("startup")
async def startup():
    global redis_client
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
    print("ILT Backend v2.0 started ✓")

@app.on_event("shutdown")
async def shutdown():
    if redis_client:
        await redis_client.close()

async def get_db() -> AsyncSession:
    async with AsyncSessionLocal() as session:
        yield session


# ═══════════════════════════════════════════════════════════════════════
# HEALTH
# ═══════════════════════════════════════════════════════════════════════

@app.get("/health")
async def health():
    return {"status": "ok", "service": "ILT Mikey CDMX API", "version": "2.0.0"}


# ═══════════════════════════════════════════════════════════════════════
# AUTH
# ═══════════════════════════════════════════════════════════════════════

async def get_current_user(
    token: str = Depends(oauth2_scheme),
    db: AsyncSession = Depends(get_db)
) -> ILTUser:
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        user_id: str = payload.get("sub")
        if not user_id:
            raise HTTPException(status_code=401, detail="Invalid token")
    except JWTError:
        raise HTTPException(status_code=401, detail="Token invalid or expired")
    result = await db.execute(select(ILTUser).where(ILTUser.id == user_id))
    user   = result.scalar_one_or_none()
    if not user or not user.is_active:
        raise HTTPException(status_code=401, detail="User not found or inactive")
    return user

async def require_owner(user: ILTUser = Depends(get_current_user)) -> ILTUser:
    if user.role != "owner":
        raise HTTPException(status_code=403, detail="Owner access required")
    return user


@app.post("/auth/login", response_model=TokenResponse)
async def auth_login(data: UserLogin, db: AsyncSession = Depends(get_db)):
    if data.role not in VALID_ROLES:
        raise HTTPException(status_code=400, detail="Invalid role")
    result = await db.execute(
        select(ILTUser).where(ILTUser.email == data.email.lower().strip())
    )
    user = result.scalar_one_or_none()
    if not user or not user.is_active or user.role != data.role:
        pwd_ctx.hash("dummy")
        raise HTTPException(status_code=401, detail="Invalid credentials")
    if not pwd_ctx.verify(data.password, user.password_hash):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    user.last_login = datetime.now(timezone.utc)
    await db.commit()
    scopes  = ROLE_SCOPES[user.role]
    expires = datetime.now(timezone.utc) + timedelta(minutes=JWT_EXPIRE_MINUTES)
    token   = jwt.encode(
        {"sub": user.id, "email": user.email, "name": user.name,
         "role": user.role, "scope": scopes, "exp": expires},
        JWT_SECRET, algorithm=JWT_ALGORITHM,
    )
    return TokenResponse(
        access_token=token, user_name=user.name, user_role=user.role,
        scopes=scopes, expires_in=JWT_EXPIRE_MINUTES * 60,
    )


@app.get("/auth/verify", response_model=VerifyResponse)
async def auth_verify(user: ILTUser = Depends(get_current_user)):
    return VerifyResponse(
        valid=True, user_id=user.id, user_name=user.name,
        user_email=user.email, user_role=user.role,
        scopes=ROLE_SCOPES[user.role],
    )


@app.post("/auth/create-user", status_code=201)
async def auth_create_user(
    data: UserCreate,
    # BOOTSTRAP MODE — uncomment below once all accounts are created:
    # owner: ILTUser = Depends(require_owner),
    db: AsyncSession = Depends(get_db),
):
    if data.role not in VALID_ROLES:
        raise HTTPException(status_code=400, detail="Invalid role")
    existing = await db.execute(
        select(ILTUser).where(ILTUser.email == data.email.lower().strip())
    )
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=409, detail="Email already registered")
    user = ILTUser(
        email=data.email.lower().strip(), name=data.name,
        role=data.role, password_hash=pwd_ctx.hash(data.password),
        is_active=True,
    )
    db.add(user)
    await db.commit()
    return {"status": "created", "email": user.email, "role": user.role}


@app.get("/auth/users", dependencies=[Depends(require_owner)])
async def auth_list_users(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(ILTUser).order_by(ILTUser.role, ILTUser.name))
    users  = result.scalars().all()
    return [{"id": u.id, "email": u.email, "name": u.name, "role": u.role,
             "is_active": u.is_active,
             "last_login": u.last_login.isoformat() if u.last_login else None}
            for u in users]


@app.patch("/auth/users/{user_id}/deactivate", dependencies=[Depends(require_owner)])
async def auth_deactivate_user(user_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(ILTUser).where(ILTUser.id == user_id))
    user   = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    user.is_active = False
    await db.commit()
    return {"status": "deactivated", "email": user.email}


@app.delete("/auth/users/reset-all")
async def auth_reset_all_users(db: AsyncSession = Depends(get_db)):
    await db.execute(__import__('sqlalchemy').text("DELETE FROM ilt_users"))
    await db.commit()
    return {"status": "all users deleted"}


class PasswordReset(BaseModel):
    email:        str
    new_password: str

@app.post("/auth/users/reset-password")
async def auth_reset_password(data: PasswordReset, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(ILTUser).where(ILTUser.email == data.email.lower().strip()))
    user   = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    user.password_hash = pwd_ctx.hash(data.new_password)
    await db.commit()
    return {"status": "password updated", "email": user.email}


# ═══════════════════════════════════════════════════════════════════════
# MIKEY CHAT — Proxy to Anthropic (key stays server-side)
# ═══════════════════════════════════════════════════════════════════════

@app.post("/api/chat")
async def mikey_chat(data: ChatRequest):
    if not ANTHROPIC_API_KEY:
        raise HTTPException(status_code=500, detail="Anthropic API key not configured")
    try:
        response = anthropic_client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1000,
            system=data.system or "You are Mikey, the AI operations intelligence engine for ILT and Lux Tour Travel in Mexico City. Be concise, professional, and operationally precise.",
            messages=data.messages
        )
        return {"content": response.content[0].text}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ═══════════════════════════════════════════════════════════════════════
# DRIVERS
# ═══════════════════════════════════════════════════════════════════════

@app.post("/api/drivers")
async def add_driver(data: DriverCreate, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Driver).where(Driver.name == data.name))
    if result.scalar_one_or_none():
        raise HTTPException(status_code=409, detail="Driver already exists")
    driver = Driver(**data.dict())
    db.add(driver)
    await db.commit()
    await db.refresh(driver)
    return {"status": "created", "driver": driver.name, "id": driver.id}

@app.get("/api/drivers")
async def list_drivers(db: AsyncSession = Depends(get_db)):
    result  = await db.execute(select(Driver).where(Driver.is_active == True))
    drivers = result.scalars().all()
    return [{
        "id": d.id, "name": d.name, "initials": d.initials,
        "specialty": d.specialty, "zones": d.zones, "languages": d.languages,
        "is_available": d.is_available, "current_lat": d.current_lat,
        "current_lng": d.current_lng,
        "last_gps_at": d.last_gps_at.isoformat() if d.last_gps_at else None,
    } for d in drivers]

@app.patch("/api/drivers/{driver_name}")
async def update_driver(driver_name: str, data: DriverUpdate, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Driver).where(Driver.name == driver_name))
    driver = result.scalar_one_or_none()
    if not driver:
        raise HTTPException(status_code=404, detail="Driver not found")
    if data.is_available is not None:
        driver.is_available = data.is_available
    if data.current_lat is not None:
        driver.current_lat = data.current_lat
        driver.current_lng = data.current_lng
        driver.last_gps_at = datetime.now(timezone.utc)
    await db.commit()
    if redis_client:
        await redis_client.publish(f"driver_gps:{driver_name}", json.dumps({
            "name": driver.name, "lat": driver.current_lat, "lng": driver.current_lng,
            "available": driver.is_available, "ts": datetime.now(timezone.utc).isoformat()
        }))
    return {"status": "ok", "driver": driver.name}

@app.delete("/api/drivers/{driver_name}")
async def deactivate_driver(driver_name: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Driver).where(Driver.name == driver_name))
    driver = result.scalar_one_or_none()
    if not driver:
        raise HTTPException(status_code=404, detail="Driver not found")
    driver.is_active = False
    await db.commit()
    return {"status": "deactivated", "driver": driver_name}


# ═══════════════════════════════════════════════════════════════════════
# CLIENTS
# ═══════════════════════════════════════════════════════════════════════

@app.post("/api/clients", response_model=ClientResponse)
async def upsert_client(data: ClientCreate, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Client).where(Client.email == data.email))
    client = result.scalar_one_or_none()
    if client:
        if data.name and not client.name:     client.name = data.name
        if data.phone and not client.phone:   client.phone = data.phone
        if data.pax:                          client.pax_typical = data.pax
        if data.is_vip and not client.is_vip: client.is_vip = True
        client.last_seen = datetime.now(timezone.utc)
    else:
        client = Client(
            email=data.email, name=data.name, phone=data.phone,
            pax_typical=data.pax or 1, is_vip=data.is_vip or False,
            source=data.source or "mikey-cdmx", market=data.market or "cdmx",
        )
        db.add(client)
    await db.commit()
    await db.refresh(client)
    memory = client.to_memory_string()
    if redis_client:
        await redis_client.setex(f"memory:{data.email}", 900, memory)
    return ClientResponse(
        id=client.id, email=client.email, name=client.name, is_vip=client.is_vip,
        booking_count=client.booking_count, total_lifetime_spend=client.total_lifetime_spend,
        memory_string=memory,
    )

@app.get("/api/clients/{email}", response_model=ClientResponse)
async def get_client(email: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Client).where(Client.email == email))
    client = result.scalar_one_or_none()
    if not client:
        raise HTTPException(status_code=404, detail="Client not found")
    memory = client.to_memory_string()
    return ClientResponse(
        id=client.id, email=client.email, name=client.name, is_vip=client.is_vip,
        booking_count=client.booking_count, total_lifetime_spend=client.total_lifetime_spend,
        memory_string=memory,
    )


# ═══════════════════════════════════════════════════════════════════════
# BOOKINGS
# ═══════════════════════════════════════════════════════════════════════

@app.post("/api/bookings", response_model=BookingResponse)
async def create_booking(data: BookingCreate, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Booking).where(Booking.confirmation_number == data.confirmation_number))
    existing = result.scalar_one_or_none()
    if existing:
        existing.status = data.status or existing.status
        await db.commit()
        await db.refresh(existing)
        return BookingResponse(
            id=existing.id, confirmation_number=existing.confirmation_number,
            status=existing.status, total_usd=existing.total_usd,
            is_vip=existing.is_vip, driver_assigned=existing.driver_assigned,
        )
    client_id = None
    if data.client_email:
        result = await db.execute(select(Client).where(Client.email == data.client_email))
        client = result.scalar_one_or_none()
        if client:
            client_id = client.id
            client.booking_count        += 1
            client.total_lifetime_spend += data.total_usd
            if data.is_vip: client.is_vip = True
    booking = Booking(
        confirmation_number=data.confirmation_number,
        client_id=client_id, client_email=data.client_email,
        client_name=data.client_name, vehicle_key=data.vehicle_key,
        service_type=data.service_type, total_usd=data.total_usd,
        session_total_usd=data.session_total or data.total_usd,
        status=data.status or BookingStatus.CONFIRMED,
        is_vip=data.is_vip or False, market=data.market or "cdmx",
    )
    db.add(booking)
    await db.commit()
    await db.refresh(booking)
    return BookingResponse(
        id=booking.id, confirmation_number=booking.confirmation_number,
        status=booking.status, total_usd=booking.total_usd,
        is_vip=booking.is_vip, driver_assigned=booking.driver_assigned,
    )

@app.get("/api/bookings", response_model=List[BookingResponse])
async def list_bookings(
    status: Optional[str] = None,
    market: Optional[str] = "cdmx",
    limit:  int = 50,
    db: AsyncSession = Depends(get_db)
):
    query = select(Booking).where(Booking.market == market).order_by(Booking.created_at.desc()).limit(limit)
    if status:
        query = query.where(Booking.status == status)
    result   = await db.execute(query)
    bookings = result.scalars().all()
    return [BookingResponse(
        id=b.id, confirmation_number=b.confirmation_number, status=b.status,
        total_usd=b.total_usd, is_vip=b.is_vip, driver_assigned=b.driver_assigned,
    ) for b in bookings]


# ═══════════════════════════════════════════════════════════════════════
# DISPATCH
# ═══════════════════════════════════════════════════════════════════════

@app.post("/api/dispatch", response_model=DispatchResponse)
async def dispatch_driver(data: DispatchRequest, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Booking).where(Booking.confirmation_number == data.booking_confirmation))
    booking = result.scalar_one_or_none()
    if not booking:
        raise HTTPException(status_code=404, detail=f"Booking {data.booking_confirmation} not found")
    if data.driver_name:
        result = await db.execute(select(Driver).where(Driver.name == data.driver_name, Driver.is_active == True))
        driver = result.scalar_one_or_none()
        if not driver:
            raise HTTPException(status_code=404, detail=f"Driver {data.driver_name} not found")
    else:
        result = await db.execute(select(Driver).where(Driver.is_active == True, Driver.is_available == True))
        driver = result.scalars().first()
        if not driver:
            raise HTTPException(status_code=503, detail="No drivers available")
    eta = data.eta_minutes or random.randint(8, 18)
    booking.driver_assigned = driver.name
    booking.status          = BookingStatus.DISPATCHED
    booking.dispatched_at   = datetime.now(timezone.utc)
    log = DispatchLog(booking_id=booking.id, driver_id=driver.id, eta_minutes=eta, notes=data.notes, status="assigned")
    db.add(log)
    driver.is_available = False
    await db.commit()
    return DispatchResponse(
        booking_confirmation=data.booking_confirmation,
        driver_name=driver.name, eta_minutes=eta, vehicle=driver.vehicle_key,
    )


# ═══════════════════════════════════════════════════════════════════════
# VIP ALERT
# ═══════════════════════════════════════════════════════════════════════

@app.post("/api/vip-alert")
async def vip_alert(data: VIPAlert, db: AsyncSession = Depends(get_db)):
    if data.client_email:
        result = await db.execute(select(Client).where(Client.email == data.client_email))
        client = result.scalar_one_or_none()
        if client and not client.is_vip:
            client.is_vip     = True
            client.vip_reason = data.reason
            await db.commit()
    print(f"[VIP] {data.client_name or data.client_email} — {data.reason} — ${data.session_total or 0:.0f}")
    return {"status": "vip_flagged"}


# ═══════════════════════════════════════════════════════════════════════
# MEMORY
# ═══════════════════════════════════════════════════════════════════════

@app.get("/api/memory/{email}")
async def get_client_memory(email: str, db: AsyncSession = Depends(get_db)):
    if redis_client:
        cached = await redis_client.get(f"memory:{email}")
        if cached:
            return {"memory": cached}
    result = await db.execute(select(Client).where(Client.email == email))
    client = result.scalar_one_or_none()
    if not client:
        return {"memory": None}
    memory = client.to_memory_string()
    if redis_client:
        await redis_client.setex(f"memory:{email}", 900, memory)
    return {"memory": memory}


# ═══════════════════════════════════════════════════════════════════════
# STRIPE WEBHOOK
# ═══════════════════════════════════════════════════════════════════════

@app.post("/webhooks/stripe")
async def stripe_webhook(
    request: Request,
    background: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    stripe_signature: str = Header(None)
):
    payload = await request.body()
    try:
        event = stripe.Webhook.construct_event(payload, stripe_signature, STRIPE_WEBHOOK_SEC)
    except stripe.error.SignatureVerificationError:
        raise HTTPException(status_code=400, detail="Invalid Stripe signature")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    event_type = event["type"]
    event_data = event["data"]["object"]

    if event_type == "checkout.session.completed":
        conf_num       = event_data.get("metadata", {}).get("confirmation_number")
        stripe_sess_id = event_data.get("id")
        payment_intent = event_data.get("payment_intent")
        customer_email = event_data.get("customer_email") or event_data.get("customer_details", {}).get("email")
        amount_total   = event_data.get("amount_total", 0) / 100
        is_vip         = event_data.get("metadata", {}).get("is_vip", "false").lower() == "true"
        if conf_num:
            result = await db.execute(select(Booking).where(Booking.confirmation_number == conf_num))
            booking = result.scalar_one_or_none()
            if booking:
                booking.status                = BookingStatus.PAID
                booking.stripe_session_id     = stripe_sess_id
                booking.stripe_payment_intent = payment_intent
                booking.paid_at               = datetime.now(timezone.utc)
                booking.is_vip                = booking.is_vip or is_vip
                await db.commit()
                if customer_email:
                    result2 = await db.execute(select(Client).where(Client.email == customer_email))
                    client  = result2.scalar_one_or_none()
                    if client:
                        client.total_lifetime_spend += amount_total
                        if is_vip: client.is_vip = True
                        await db.commit()
                background.add_task(auto_dispatch_after_payment, conf_num, booking.client_name)
        print(f"[STRIPE] checkout.session.completed — {conf_num} · ${amount_total:.2f}")

    elif event_type == "payment_intent.payment_failed":
        conf_num  = event_data.get("metadata", {}).get("confirmation_number", "unknown")
        error_msg = event_data.get("last_payment_error", {}).get("message", "Unknown")
        print(f"[STRIPE] Payment FAILED — {conf_num}: {error_msg}")

    elif event_type == "charge.refunded":
        print(f"[STRIPE] Refund — {event_data.get('id')}")

    return {"received": True}


async def auto_dispatch_after_payment(conf_num: str, client_name: str):
    async with AsyncSessionLocal() as db:
        result = await db.execute(select(Driver).where(Driver.is_active == True, Driver.is_available == True))
        driver = result.scalars().first()
        if not driver:
            print(f"[DISPATCH] No available drivers for {conf_num}")
            return
        result2 = await db.execute(select(Booking).where(Booking.confirmation_number == conf_num))
        booking  = result2.scalar_one_or_none()
        if booking:
            eta = random.randint(8, 15)
            booking.driver_assigned = driver.name
            booking.status          = BookingStatus.DISPATCHED
            booking.dispatched_at   = datetime.now(timezone.utc)
            log = DispatchLog(booking_id=booking.id, driver_id=driver.id, eta_minutes=eta, status="assigned")
            db.add(log)
            driver.is_available = False
            await db.commit()
            print(f"[DISPATCH] {driver.name} → {conf_num} · ETA {eta}min")

# ═══════════════════════════════════════════════════════════════════════
# RUN: uvicorn main:app --host 0.0.0.0 --port 8000
# ═══════════════════════════════════════════════════════════════════════
