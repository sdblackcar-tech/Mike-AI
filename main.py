# ═══════════════════════════════════════════════════════════════════════
# ILT PLATFORM — MIKEY CDMX BACKEND v1.0
# FastAPI + PostgreSQL + Redis
# © 2026 James-Michael Prieto Corbin / Infinite Logistic Technologies
# Patent pending. All rights reserved.
#
# COVERS:
#   1. Booking persistence (quotes + confirmed bookings)
#   2. Client CRM (name, email, travel history, VIP flag)
#   3. Driver dispatch + assignment log
#   4. Stripe webhook → booking confirmed flow
#
# IT DEPLOYMENT:
#   pip install fastapi uvicorn sqlalchemy asyncpg redis psycopg2-binary
#               stripe python-dotenv alembic pydantic[email]
#   uvicorn main:app --host 0.0.0.0 --port 8000 --reload
#
# ENV VARS (create .env file):
#   DATABASE_URL=postgresql+asyncpg://user:pass@localhost/ilt_cdmx
#   REDIS_URL=redis://localhost:6379
#   STRIPE_SECRET_KEY=sk_live_...
#   STRIPE_WEBHOOK_SECRET=whsec_...
#   JAMES_EMAIL=james@luxtourtravel.com
#   OPS_EMAIL=ops@mexicocityblackcar.com
#   JWT_SECRET=your_strong_random_secret
# ═══════════════════════════════════════════════════════════════════════

import os
import json
import hmac
import hashlib
import stripe
from datetime import datetime, timezone
from typing import Optional, List
from enum import Enum

import redis.asyncio as aioredis
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request, BackgroundTasks, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, EmailStr
from sqlalchemy import (
    Column, String, Float, Integer, Boolean, DateTime,
    Text, Enum as SAEnum, ForeignKey, create_engine
)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from sqlalchemy.future import select

load_dotenv()

# ─── CONFIG ────────────────────────────────────────────────────────────
DATABASE_URL       = os.getenv("DATABASE_URL", "postgresql+asyncpg://postgres:password@localhost/ilt_cdmx")
REDIS_URL          = os.getenv("REDIS_URL", "redis://localhost:6379")
STRIPE_SECRET      = os.getenv("STRIPE_SECRET_KEY", "")
STRIPE_WEBHOOK_SEC = os.getenv("STRIPE_WEBHOOK_SECRET", "")
JAMES_EMAIL        = os.getenv("JAMES_EMAIL", "james@luxtourtravel.com")
OPS_EMAIL          = os.getenv("OPS_EMAIL", "ops@mexicocityblackcar.com")

stripe.api_key = STRIPE_SECRET

# ─── DATABASE ──────────────────────────────────────────────────────────
engine = create_async_engine(DATABASE_URL, echo=False)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()

# ─── REDIS ─────────────────────────────────────────────────────────────
redis_client: aioredis.Redis = None

# ─── FASTAPI APP ───────────────────────────────────────────────────────
app = FastAPI(
    title="ILT Mikey CDMX API",
    description="Booking, CRM, dispatch, and Stripe webhook handler for Mexico City Black Car",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://www.mexicocityblackcar.com",
        "https://mexicocityblackcar.com",
        "https://www.luxtourtravel.com",
        "http://localhost:3000",  # dev
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ═══════════════════════════════════════════════════════════════════════
# DATABASE MODELS
# ═══════════════════════════════════════════════════════════════════════

class BookingStatus(str, Enum):
    QUOTE     = "quote"
    CONFIRMED = "confirmed"
    PAID      = "paid"
    DISPATCHED= "dispatched"
    COMPLETED = "completed"
    CANCELLED = "cancelled"

class VehicleKey(str, Enum):
    SEDAN   = "sedan"
    SUV     = "suv"
    VAN     = "van"
    MINIBUS = "minibus"
    BUS     = "bus"
    LUXURY  = "luxury"

class ServiceType(str, Enum):
    HOURLY       = "hourly"
    TRANSFER     = "transfer"
    AIRPORT_AICM = "airport_aicm"
    AIRPORT_AIFA = "airport_aifa"


class Client(Base):
    """
    CRM — every client who interacts with Mikey and provides contact info.
    Supports returning client memory injection into system prompt.
    """
    __tablename__ = "clients"

    id              = Column(String(36), primary_key=True, default=lambda: str(__import__('uuid').uuid4()))
    email           = Column(String(255), unique=True, index=True, nullable=False)
    name            = Column(String(255), nullable=True)
    phone           = Column(String(50), nullable=True)
    preferred_lang  = Column(String(10), default="en")
    pax_typical     = Column(Integer, default=1)
    is_vip          = Column(Boolean, default=False)
    vip_reason      = Column(Text, nullable=True)
    source          = Column(String(50), default="mikey-cdmx")   # mikey-cdmx | api | manual
    market          = Column(String(20), default="cdmx")
    total_lifetime_spend = Column(Float, default=0.0)
    booking_count   = Column(Integer, default=0)
    last_seen       = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    created_at      = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    notes           = Column(Text, nullable=True)  # Ops notes — never shown to client

    bookings        = relationship("Booking", back_populates="client", lazy="selectin")

    def to_memory_string(self) -> str:
        """
        Returns a concise memory string for injection into Mikey system prompt.
        Used to personalize returning client experience.
        Example: "Client Maria González, VIP, prefers Sedan, last booking was AICM transfer,
                  lifetime spend $2,450 across 3 bookings."
        """
        parts = [f"Client {self.name or 'unknown name'}"]
        if self.is_vip:
            parts.append("VIP client")
        if self.booking_count:
            parts.append(f"{self.booking_count} prior bookings, lifetime spend ${self.total_lifetime_spend:.0f}")
        if self.pax_typical and self.pax_typical > 1:
            parts.append(f"typically travels with {self.pax_typical} passengers")
        recent = sorted(self.bookings, key=lambda b: b.created_at or datetime.min, reverse=True)
        if recent:
            last = recent[0]
            parts.append(f"last booking was {last.service_type} with {last.vehicle_key} (${last.total_usd:.0f})")
        if self.notes:
            parts.append(f"ops note: {self.notes}")
        return ". ".join(parts) + "."


class Booking(Base):
    """
    Booking persistence — every quote AND confirmed booking.
    Drives dispatch, VIP escalation, and Stripe reconciliation.
    """
    __tablename__ = "bookings"

    id                  = Column(String(36), primary_key=True, default=lambda: str(__import__('uuid').uuid4()))
    confirmation_number = Column(String(50), unique=True, index=True, nullable=False)
    client_id           = Column(String(36), ForeignKey("clients.id"), nullable=True)
    client_email        = Column(String(255), index=True, nullable=True)
    client_name         = Column(String(255), nullable=True)
    vehicle_key         = Column(String(20), nullable=True)
    service_type        = Column(String(30), nullable=True)
    hours               = Column(Float, default=0)
    miles               = Column(Float, default=0)
    total_usd           = Column(Float, nullable=False)
    session_total_usd   = Column(Float, default=0)  # multi-leg running total
    status              = Column(String(20), default=BookingStatus.QUOTE)
    is_vip              = Column(Boolean, default=False)
    market              = Column(String(20), default="cdmx")
    driver_assigned     = Column(String(100), nullable=True)
    stripe_session_id   = Column(String(200), nullable=True)
    stripe_payment_intent = Column(String(200), nullable=True)
    paid_at             = Column(DateTime(timezone=True), nullable=True)
    dispatched_at       = Column(DateTime(timezone=True), nullable=True)
    completed_at        = Column(DateTime(timezone=True), nullable=True)
    notes               = Column(Text, nullable=True)
    created_at          = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at          = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc),
                                  onupdate=lambda: datetime.now(timezone.utc))

    client              = relationship("Client", back_populates="bookings")
    dispatch_logs       = relationship("DispatchLog", back_populates="booking", lazy="selectin")


class Driver(Base):
    """
    The four CDMX drivers. Extend as the fleet grows.
    """
    __tablename__ = "drivers"

    id          = Column(String(36), primary_key=True, default=lambda: str(__import__('uuid').uuid4()))
    name        = Column(String(100), unique=True, nullable=False)
    initials    = Column(String(4), nullable=True)
    phone       = Column(String(50), nullable=True)
    whatsapp    = Column(String(50), nullable=True)
    specialty   = Column(String(200), nullable=True)
    zones       = Column(String(200), nullable=True)       # comma-separated preferred zones
    languages   = Column(String(100), default="en,es")
    vehicle_key = Column(String(20), nullable=True)        # their primary vehicle
    is_active   = Column(Boolean, default=True)
    is_available= Column(Boolean, default=True)            # real-time toggle
    current_lat = Column(Float, nullable=True)             # GPS — updated by driver PWA
    current_lng = Column(Float, nullable=True)
    last_gps_at = Column(DateTime(timezone=True), nullable=True)
    created_at  = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    dispatch_logs = relationship("DispatchLog", back_populates="driver", lazy="selectin")


class DispatchLog(Base):
    """
    Every driver assignment event. Links booking to driver.
    Powers the [[DRIVER_ASSIGN]] signal response.
    """
    __tablename__ = "dispatch_logs"

    id          = Column(String(36), primary_key=True, default=lambda: str(__import__('uuid').uuid4()))
    booking_id  = Column(String(36), ForeignKey("bookings.id"), nullable=False)
    driver_id   = Column(String(36), ForeignKey("drivers.id"), nullable=False)
    assigned_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    eta_minutes = Column(Integer, nullable=True)
    notes       = Column(Text, nullable=True)
    status      = Column(String(20), default="assigned")  # assigned | en_route | arrived | completed

    booking     = relationship("Booking", back_populates="dispatch_logs")
    driver      = relationship("Driver", back_populates="dispatch_logs")


# ═══════════════════════════════════════════════════════════════════════
# PYDANTIC SCHEMAS
# ═══════════════════════════════════════════════════════════════════════

class ClientCreate(BaseModel):
    email:         str
    name:          Optional[str] = None
    phone:         Optional[str] = None
    pax:           Optional[int] = 1
    source:        Optional[str] = "mikey-cdmx"
    market:        Optional[str] = "cdmx"
    session_id:    Optional[str] = None
    is_vip:        Optional[bool] = False
    session_total: Optional[float] = 0.0
    timestamp:     Optional[str] = None

class ClientResponse(BaseModel):
    id:             str
    email:          str
    name:           Optional[str]
    is_vip:         bool
    booking_count:  int
    total_lifetime_spend: float
    memory_string:  Optional[str] = None  # injected for Mikey system prompt
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
    timestamp:           Optional[str] = None

class BookingResponse(BaseModel):
    id:                  str
    confirmation_number: str
    status:              str
    total_usd:           float
    is_vip:              bool
    driver_assigned:     Optional[str]
    class Config:
        from_attributes = True

class DispatchRequest(BaseModel):
    booking_confirmation: str
    driver_name:          Optional[str] = None   # if None, auto-assign
    eta_minutes:          Optional[int] = None
    notes:                Optional[str] = None

class DispatchResponse(BaseModel):
    booking_confirmation: str
    driver_name:          str
    eta_minutes:          int
    vehicle:              Optional[str]

class DriverUpdate(BaseModel):
    is_available: Optional[bool] = None
    current_lat:  Optional[float] = None
    current_lng:  Optional[float] = None

class VIPAlert(BaseModel):
    client_email:  Optional[str]
    client_name:   Optional[str]
    reason:        str
    session_total: Optional[float]
    market:        str = "cdmx"


# ═══════════════════════════════════════════════════════════════════════
# STARTUP / SHUTDOWN
# ═══════════════════════════════════════════════════════════════════════

@app.on_event("startup")
async def startup():
    global redis_client
    # Create tables
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    # Connect Redis
    redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
    # Seed drivers if not already present
    await seed_drivers()
    print("ILT Backend started ✓")

@app.on_event("shutdown")
async def shutdown():
    if redis_client:
        await redis_client.close()

async def get_db() -> AsyncSession:
    async with AsyncSessionLocal() as session:
        yield session


async def seed_drivers():
    """Insert the 4 CDMX drivers on first boot if they don't exist."""
    DRIVER_DATA = [
        {"name":"Enrique Cota",       "initials":"EC","specialty":"Corporate & diplomatic protocol","zones":"polanco,lomas,santafe","languages":"en,es"},
        {"name":"Borris Liokumovich", "initials":"BL","specialty":"International VIPs · EN/ES/RU",  "zones":"all",                 "languages":"en,es,ru"},
        {"name":"Oscar Pena",         "initials":"OP","specialty":"AICM/AIFA airport ops",           "zones":"airport,polanco,roma", "languages":"en,es"},
        {"name":"Victor Nakada",      "initials":"VN","specialty":"Nightlife & entertainment",       "zones":"condesa,roma,polanco", "languages":"en,es,ja"},
    ]
    async with AsyncSessionLocal() as db:
        for d in DRIVER_DATA:
            result = await db.execute(select(Driver).where(Driver.name == d["name"]))
            if not result.scalar_one_or_none():
                db.add(Driver(**d))
        await db.commit()


# ═══════════════════════════════════════════════════════════════════════
# HEALTH CHECK
# ═══════════════════════════════════════════════════════════════════════

@app.get("/health")
async def health():
    return {"status": "ok", "service": "ILT Mikey CDMX API", "version": "1.0.0"}


# ═══════════════════════════════════════════════════════════════════════
# CLIENTS — CRM
# ═══════════════════════════════════════════════════════════════════════

@app.post("/api/clients", response_model=ClientResponse)
async def upsert_client(data: ClientCreate, db: AsyncSession = Depends(get_db)):
    """
    Called by Mikey frontend on [[SAVE_CLIENT]] signal.
    Creates or updates the client record. Returns memory_string for prompt injection.
    """
    result = await db.execute(select(Client).where(Client.email == data.email))
    client = result.scalar_one_or_none()

    if client:
        # Update existing
        if data.name and not client.name:
            client.name = data.name
        if data.phone and not client.phone:
            client.phone = data.phone
        if data.pax:
            client.pax_typical = data.pax
        if data.is_vip and not client.is_vip:
            client.is_vip = True
        client.last_seen = datetime.now(timezone.utc)
    else:
        # Create new
        client = Client(
            email=data.email,
            name=data.name,
            phone=data.phone,
            pax_typical=data.pax or 1,
            is_vip=data.is_vip or False,
            source=data.source or "mikey-cdmx",
            market=data.market or "cdmx",
        )
        db.add(client)

    await db.commit()
    await db.refresh(client)

    # Cache memory string in Redis (15min TTL) for fast prompt injection
    memory = client.to_memory_string()
    if redis_client:
        await redis_client.setex(f"memory:{data.email}", 900, memory)

    return ClientResponse(
        id=client.id,
        email=client.email,
        name=client.name,
        is_vip=client.is_vip,
        booking_count=client.booking_count,
        total_lifetime_spend=client.total_lifetime_spend,
        memory_string=memory,
    )


@app.get("/api/clients/{email}", response_model=ClientResponse)
async def get_client(email: str, db: AsyncSession = Depends(get_db)):
    """
    Lookup returning client — called on session start to inject memory.
    Frontend can call: GET /api/clients/user@email.com
    Returns memory_string → inject into buildSystemPrompt(lang, memory)
    """
    # Try Redis cache first
    if redis_client:
        cached = await redis_client.get(f"memory:{email}")
        if cached:
            result = await db.execute(select(Client).where(Client.email == email))
            client = result.scalar_one_or_none()
            if client:
                return ClientResponse(
                    id=client.id, email=client.email, name=client.name,
                    is_vip=client.is_vip, booking_count=client.booking_count,
                    total_lifetime_spend=client.total_lifetime_spend,
                    memory_string=cached,
                )

    result = await db.execute(select(Client).where(Client.email == email))
    client = result.scalar_one_or_none()
    if not client:
        raise HTTPException(status_code=404, detail="Client not found")

    memory = client.to_memory_string()
    return ClientResponse(
        id=client.id, email=client.email, name=client.name,
        is_vip=client.is_vip, booking_count=client.booking_count,
        total_lifetime_spend=client.total_lifetime_spend,
        memory_string=memory,
    )


# ═══════════════════════════════════════════════════════════════════════
# BOOKINGS
# ═══════════════════════════════════════════════════════════════════════

@app.post("/api/bookings", response_model=BookingResponse)
async def create_booking(data: BookingCreate, db: AsyncSession = Depends(get_db)):
    """
    Called by Mikey frontend after triggerBookingFlow() completes.
    Also called by Stripe webhook on payment_intent.succeeded.
    """
    # Check for duplicate
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

    # Lookup or create client linkage
    client_id = None
    if data.client_email:
        result = await db.execute(select(Client).where(Client.email == data.client_email))
        client = result.scalar_one_or_none()
        if client:
            client_id = client.id
            client.booking_count += 1
            client.total_lifetime_spend += data.total_usd
            if data.is_vip:
                client.is_vip = True

    booking = Booking(
        confirmation_number=data.confirmation_number,
        client_id=client_id,
        client_email=data.client_email,
        client_name=data.client_name,
        vehicle_key=data.vehicle_key,
        service_type=data.service_type,
        total_usd=data.total_usd,
        session_total_usd=data.session_total or data.total_usd,
        status=data.status or BookingStatus.CONFIRMED,
        is_vip=data.is_vip or False,
        market=data.market or "cdmx",
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
    limit: int = 50,
    db: AsyncSession = Depends(get_db)
):
    """List bookings — for ops dashboard."""
    query = select(Booking).where(Booking.market == market).order_by(Booking.created_at.desc()).limit(limit)
    if status:
        query = query.where(Booking.status == status)
    result = await db.execute(query)
    bookings = result.scalars().all()
    return [BookingResponse(
        id=b.id, confirmation_number=b.confirmation_number, status=b.status,
        total_usd=b.total_usd, is_vip=b.is_vip, driver_assigned=b.driver_assigned,
    ) for b in bookings]


# ═══════════════════════════════════════════════════════════════════════
# DRIVERS + DISPATCH
# ═══════════════════════════════════════════════════════════════════════

@app.get("/api/drivers")
async def list_drivers(db: AsyncSession = Depends(get_db)):
    """Return all active drivers with availability."""
    result = await db.execute(select(Driver).where(Driver.is_active == True))
    drivers = result.scalars().all()
    return [{
        "id": d.id,
        "name": d.name,
        "initials": d.initials,
        "specialty": d.specialty,
        "zones": d.zones,
        "languages": d.languages,
        "is_available": d.is_available,
        "current_lat": d.current_lat,
        "current_lng": d.current_lng,
        "last_gps_at": d.last_gps_at.isoformat() if d.last_gps_at else None,
    } for d in drivers]


@app.patch("/api/drivers/{driver_name}")
async def update_driver(driver_name: str, data: DriverUpdate, db: AsyncSession = Depends(get_db)):
    """Driver PWA calls this to update GPS location and availability."""
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
    # Broadcast to WebSocket subscribers (Phase 2 — GPS live map)
    if redis_client:
        await redis_client.publish(f"driver_gps:{driver_name}", json.dumps({
            "name": driver.name,
            "lat": driver.current_lat,
            "lng": driver.current_lng,
            "available": driver.is_available,
            "ts": datetime.now(timezone.utc).isoformat()
        }))
    return {"status": "ok", "driver": driver.name}


@app.post("/api/dispatch", response_model=DispatchResponse)
async def dispatch_driver(data: DispatchRequest, background: BackgroundTasks, db: AsyncSession = Depends(get_db)):
    """
    Assign a driver to a booking.
    If driver_name not specified, auto-assigns based on:
      - Service type (Oscar for airport, Victor for nightlife, Enrique for corporate, Borris for international)
      - Availability
    Called manually by ops OR automatically triggered by Stripe webhook on payment success.
    """
    # Find booking
    result = await db.execute(select(Booking).where(Booking.confirmation_number == data.booking_confirmation))
    booking = result.scalar_one_or_none()
    if not booking:
        raise HTTPException(status_code=404, detail=f"Booking {data.booking_confirmation} not found")

    # Auto-assign logic
    if data.driver_name:
        driver_name = data.driver_name
    else:
        driver_name = _auto_assign_driver(booking.service_type, booking.is_vip)

    # Find driver record
    result = await db.execute(select(Driver).where(Driver.name == driver_name, Driver.is_active == True))
    driver = result.scalar_one_or_none()
    if not driver:
        raise HTTPException(status_code=404, detail=f"Driver {driver_name} not found or inactive")

    if not driver.is_available:
        # Fallback to next available
        result = await db.execute(select(Driver).where(Driver.is_active == True, Driver.is_available == True))
        driver = result.scalars().first()
        if not driver:
            raise HTTPException(status_code=503, detail="No drivers available")
        driver_name = driver.name

    # Calculate ETA
    import random
    eta_ranges = {
        "Enrique Cota": (8, 15),
        "Borris Liokumovich": (10, 18),
        "Oscar Pena": (5, 12),
        "Victor Nakada": (10, 20),
    }
    lo, hi = eta_ranges.get(driver_name, (8, 18))
    eta = data.eta_minutes or random.randint(lo, hi)

    # Update booking
    booking.driver_assigned = driver_name
    booking.status = BookingStatus.DISPATCHED
    booking.dispatched_at = datetime.now(timezone.utc)

    # Create dispatch log
    log = DispatchLog(
        booking_id=booking.id,
        driver_id=driver.id,
        eta_minutes=eta,
        notes=data.notes,
        status="assigned",
    )
    db.add(log)

    # Mark driver busy
    driver.is_available = False

    await db.commit()

    # Background: send WhatsApp to driver (Phase 2 — Twilio)
    background.add_task(notify_driver_whatsapp, driver_name, booking.confirmation_number, booking.client_name, eta)

    return DispatchResponse(
        booking_confirmation=data.booking_confirmation,
        driver_name=driver_name,
        eta_minutes=eta,
        vehicle=driver.vehicle_key,
    )


def _auto_assign_driver(service_type: Optional[str], is_vip: bool) -> str:
    """
    Rule-based auto-assignment.
    In Phase 2, replace with ML scoring using the 6-factor dispatch engine.
    """
    if service_type in ("airport_aicm", "airport_aifa"):
        return "Oscar Pena"
    if is_vip:
        return "Borris Liokumovich"
    if service_type == "hourly":
        return "Enrique Cota"
    return "Victor Nakada"  # Default for transfers + nightlife


async def notify_driver_whatsapp(driver_name: str, conf: str, client_name: str, eta: int):
    """
    Placeholder — wire to Twilio WhatsApp in Phase 2.
    Sends booking details to driver's WhatsApp number.
    """
    print(f"[DISPATCH] WhatsApp → {driver_name}: Booking {conf} for {client_name}. ETA acknowledged: {eta}min")
    # TODO Phase 2:
    # from twilio.rest import Client as TwilioClient
    # twilio = TwilioClient(TWILIO_SID, TWILIO_TOKEN)
    # twilio.messages.create(body=f"...", from_="whatsapp:+14155238886", to=f"whatsapp:{driver_whatsapp}")


# ═══════════════════════════════════════════════════════════════════════
# VIP ALERT
# ═══════════════════════════════════════════════════════════════════════

@app.post("/api/vip-alert")
async def vip_alert(data: VIPAlert, background: BackgroundTasks, db: AsyncSession = Depends(get_db)):
    """
    Called silently by frontend on [[VIP_FLAG]] signal.
    Marks client as VIP in DB, triggers email to James.
    """
    if data.client_email:
        result = await db.execute(select(Client).where(Client.email == data.client_email))
        client = result.scalar_one_or_none()
        if client and not client.is_vip:
            client.is_vip = True
            client.vip_reason = data.reason
            await db.commit()

    background.add_task(send_vip_email_to_james, data)
    return {"status": "vip_flagged"}


async def send_vip_email_to_james(data: VIPAlert):
    """Email James immediately when a VIP is detected."""
    print(f"[VIP ALERT] Emailing {JAMES_EMAIL}: {data.client_name or data.client_email} — {data.reason}, ${data.session_total or 0:.0f}")
    # TODO: wire to SendGrid
    # sg = sendgrid.SendGridAPIClient(SENDGRID_KEY)
    # ...


# ═══════════════════════════════════════════════════════════════════════
# STRIPE WEBHOOK — Payment → Booking Confirmed Flow
# ═══════════════════════════════════════════════════════════════════════

@app.post("/webhooks/stripe")
async def stripe_webhook(
    request: Request,
    background: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    stripe_signature: str = Header(None)
):
    """
    Stripe sends events here after payment.
    Key events:
      - checkout.session.completed → mark booking paid, auto-dispatch driver
      - payment_intent.succeeded   → redundant confirmation
      - payment_intent.payment_failed → alert ops
    
    In Stripe dashboard, set webhook URL to:
      https://api.mexicocityblackcar.com/webhooks/stripe
    """
    payload = await request.body()

    # Verify Stripe signature
    try:
        event = stripe.Webhook.construct_event(payload, stripe_signature, STRIPE_WEBHOOK_SEC)
    except stripe.error.SignatureVerificationError:
        raise HTTPException(status_code=400, detail="Invalid Stripe signature")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    event_type = event["type"]
    event_data = event["data"]["object"]

    # ── Checkout Session Completed ────────────────────────────────────
    if event_type == "checkout.session.completed":
        conf_num = event_data.get("metadata", {}).get("confirmation_number")
        stripe_session_id = event_data.get("id")
        payment_intent = event_data.get("payment_intent")
        customer_email = event_data.get("customer_email") or event_data.get("customer_details", {}).get("email")
        amount_total = event_data.get("amount_total", 0) / 100  # cents → USD
        is_vip = event_data.get("metadata", {}).get("is_vip", "false").lower() == "true"

        if conf_num:
            # Update booking to PAID
            result = await db.execute(select(Booking).where(Booking.confirmation_number == conf_num))
            booking = result.scalar_one_or_none()
            if booking:
                booking.status = BookingStatus.PAID
                booking.stripe_session_id = stripe_session_id
                booking.stripe_payment_intent = payment_intent
                booking.paid_at = datetime.now(timezone.utc)
                booking.is_vip = booking.is_vip or is_vip
                await db.commit()

                # Update client lifetime spend
                if customer_email:
                    result2 = await db.execute(select(Client).where(Client.email == customer_email))
                    client = result2.scalar_one_or_none()
                    if client:
                        client.total_lifetime_spend += amount_total
                        if is_vip:
                            client.is_vip = True
                        await db.commit()

                # Auto-dispatch driver
                background.add_task(
                    auto_dispatch_after_payment,
                    conf_num, booking.service_type, booking.is_vip,
                    booking.client_name, db
                )

        print(f"[STRIPE] checkout.session.completed — {conf_num} · ${amount_total:.2f}")

    # ── Payment Failed ────────────────────────────────────────────────
    elif event_type == "payment_intent.payment_failed":
        conf_num = event_data.get("metadata", {}).get("confirmation_number", "unknown")
        error_msg = event_data.get("last_payment_error", {}).get("message", "Unknown error")
        print(f"[STRIPE] Payment FAILED — {conf_num}: {error_msg}")
        # TODO: alert ops via email/WhatsApp

    # ── Refund ────────────────────────────────────────────────────────
    elif event_type == "charge.refunded":
        print(f"[STRIPE] Refund — {event_data.get('id')}")
        # TODO: update booking status to CANCELLED

    return {"received": True}


async def auto_dispatch_after_payment(conf_num: str, service_type: str, is_vip: bool, client_name: str, db: AsyncSession):
    """
    Triggered in background after Stripe payment confirmed.
    Auto-assigns the best available driver.
    """
    driver_name = _auto_assign_driver(service_type, is_vip)
    print(f"[DISPATCH] Auto-assigning {driver_name} to {conf_num} for {client_name}")

    result = await db.execute(select(Driver).where(Driver.name == driver_name, Driver.is_active == True))
    driver = result.scalar_one_or_none()
    if not driver or not driver.is_available:
        result = await db.execute(select(Driver).where(Driver.is_active == True, Driver.is_available == True))
        driver = result.scalars().first()

    if driver:
        result = await db.execute(select(Booking).where(Booking.confirmation_number == conf_num))
        booking = result.scalar_one_or_none()
        if booking:
            import random
            eta = random.randint(8, 15)
            booking.driver_assigned = driver.name
            booking.status = BookingStatus.DISPATCHED
            booking.dispatched_at = datetime.now(timezone.utc)
            log = DispatchLog(booking_id=booking.id, driver_id=driver.id, eta_minutes=eta, status="assigned")
            db.add(log)
            driver.is_available = False
            await db.commit()
            print(f"[DISPATCH] {driver.name} dispatched → {conf_num} · ETA {eta}min")


# ═══════════════════════════════════════════════════════════════════════
# MEMORY ENDPOINT — for frontend prompt injection
# ═══════════════════════════════════════════════════════════════════════

@app.get("/api/memory/{email}")
async def get_client_memory(email: str, db: AsyncSession = Depends(get_db)):
    """
    Lightweight endpoint — returns only the memory string.
    Frontend calls this on load if client email is stored in localStorage.
    Response goes directly into buildSystemPrompt(lang, memory).
    """
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
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════
# Run with: uvicorn main:app --host 0.0.0.0 --port 8000
# Production: gunicorn main:app -w 4 -k uvicorn.workers.UvicornWorker
