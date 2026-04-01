"""
email_o365.py — ILT Platform · Office 365 SMTP Email Router
============================================================
FastAPI router handling all transactional email for Mikey / Mexico City Black Car.
Replaces SendGrid. Uses Office 365 SMTP relay with aiosmtplib (async).

SETUP
-----
1. Install deps:
       pip install aiosmtplib jinja2

2. Office 365 SMTP credentials (set as environment variables or in .env):
       O365_SMTP_HOST     = smtp.office365.com
       O365_SMTP_PORT     = 587
       O365_SMTP_USER     = your-sender@yourdomain.com  (e.g. ops@mexicocityblackcar.com)
       O365_SMTP_PASSWORD = your-app-password-or-account-password

   ⚠️  If your O365 tenant has MFA enabled, create an App Password:
       Microsoft 365 Admin → Users → [user] → Mail → App passwords

3. Mount in main FastAPI app (server.py):
       from email_o365 import router as email_router
       app.include_router(email_router)

4. Frontend CONFIG:
       BACKEND_URL: 'https://api.mexicocityblackcar.com'
   All sends go to POST /api/email

EMAIL TYPES HANDLED
-------------------
  client_confirmation   → client receipt after booking
  exec_copy             → James copy of every booking
  ops_alert             → ops@ dispatched booking alert
  venue_reservation     → partner venue reservation request (IATA)
  commission_notification → ops@ commission tracking for venue bookings
"""

import os
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Dict, Optional

import aiosmtplib
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, EmailStr

logger = logging.getLogger("ilt.email")

router = APIRouter(prefix="/api/email", tags=["email"])

# ─── O365 SMTP CONFIG ────────────────────────────────────────────────────────
SMTP_HOST = os.getenv("O365_SMTP_HOST", "smtp.office365.com")
SMTP_PORT = int(os.getenv("O365_SMTP_PORT", "587"))
SMTP_USER = os.getenv("O365_SMTP_USER", "")       # sender address
SMTP_PASS = os.getenv("O365_SMTP_PASSWORD", "")

# Reply-to / display name overrides
FROM_NAME = os.getenv("O365_FROM_NAME", "Mexico City Black Car")
FROM_ADDR = SMTP_USER  # must match authenticated account in O365


# ─── REQUEST MODEL ────────────────────────────────────────────────────────────
class EmailRequest(BaseModel):
    type: str                          # email type key (see HANDLERS below)
    to_email: str
    to_name: Optional[str] = "Guest"

    # Booking fields (client_confirmation / exec_copy / ops_alert)
    confirmation_number: Optional[str] = None
    client_name: Optional[str] = None
    client_email: Optional[str] = None
    client_phone: Optional[str] = None
    vehicle: Optional[str] = None
    service_label: Optional[str] = None
    total_amount: Optional[str] = None
    trip_summary: Optional[str] = None
    booking_date: Optional[str] = None
    is_vip: Optional[str] = "Standard"
    pax: Optional[Any] = None

    # Company fields
    company: Optional[str] = "Mexico City Black Car"
    phone: Optional[str] = "+1 858 999 1895"
    website: Optional[str] = "https://www.mexicocityblackcar.com"

    # Venue fields
    venue_name: Optional[str] = None
    client_count: Optional[Any] = None
    iata_number: Optional[str] = None
    commission_rate: Optional[str] = None
    estimated_spend: Optional[str] = None
    commission_estimate: Optional[str] = None


# ─── TEMPLATE BUILDER ────────────────────────────────────────────────────────
def _base_html(title: str, body_rows: str) -> str:
    """Minimal branded HTML email wrapper."""
    return f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>{title}</title>
<style>
  body{{margin:0;padding:0;background:#f5f5f5;font-family:'Helvetica Neue',Arial,sans-serif;}}
  .wrap{{max-width:580px;margin:32px auto;background:#ffffff;border-radius:6px;overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,0.08);}}
  .hdr{{background:#0e0e0e;padding:24px 28px;border-bottom:3px solid #C9A96E;}}
  .hdr-title{{color:#C9A96E;font-size:20px;font-weight:600;letter-spacing:0.06em;margin:0;}}
  .hdr-sub{{color:#888;font-size:11px;letter-spacing:0.12em;text-transform:uppercase;margin:4px 0 0;}}
  .body{{padding:28px;color:#1a1a1a;font-size:14px;line-height:1.7;}}
  .row{{display:flex;justify-content:space-between;padding:7px 0;border-bottom:1px solid #f0f0f0;}}
  .lbl{{color:#888;font-size:12px;font-family:monospace;letter-spacing:0.04em;}}
  .val{{color:#111;font-size:13px;font-weight:500;text-align:right;}}
  .badge{{display:inline-block;background:#C9A96E;color:#0e0e0e;font-size:11px;font-weight:600;
          letter-spacing:0.1em;padding:3px 10px;border-radius:3px;text-transform:uppercase;}}
  .badge.vip{{background:#b8860b;color:#fff;}}
  .badge.ops{{background:#00d4aa;color:#0e0e0e;}}
  .ftr{{background:#f9f9f9;padding:16px 28px;font-size:11px;color:#aaa;border-top:1px solid #eee;}}
  .ftr a{{color:#C9A96E;text-decoration:none;}}
</style>
</head>
<body>
<div class="wrap">
  <div class="hdr">
    <p class="hdr-title">Mexico City Black Car</p>
    <p class="hdr-sub">Powered by Mikey AI · Lux Tour Travel · ILT</p>
  </div>
  <div class="body">
    {body_rows}
  </div>
  <div class="ftr">
    © 2026 Infinite Logistic Technologies · <a href="https://www.mexicocityblackcar.com">mexicocityblackcar.com</a>
    · <a href="https://www.luxtourtravel.com">luxtourtravel.com</a>
  </div>
</div>
</body></html>"""


def build_client_confirmation(r: EmailRequest) -> tuple[str, str, str]:
    subject = f"Your Booking is Confirmed — {r.confirmation_number}"
    vip_badge = '<span class="badge vip">⬥ VIP Priority</span><br/><br/>' if "YES" in (r.is_vip or "") else ""
    body = f"""
{vip_badge}
<p>Dear {r.client_name},</p>
<p>Your reservation with <strong>Mexico City Black Car</strong> is confirmed. Details below.</p>
<div class="row"><span class="lbl">Confirmation</span><span class="val"><strong>{r.confirmation_number}</strong></span></div>
<div class="row"><span class="lbl">Vehicle</span><span class="val">{r.vehicle}</span></div>
<div class="row"><span class="lbl">Service</span><span class="val">{r.service_label}</span></div>
<div class="row"><span class="lbl">Total</span><span class="val"><strong>{r.total_amount}</strong></span></div>
<div class="row"><span class="lbl">Date</span><span class="val">{r.booking_date}</span></div>
<div class="row"><span class="lbl">Service Level</span><span class="val">{r.is_vip}</span></div>
<br/>
<p>Our team will be in touch with driver details and ETA as your transfer approaches.
For any changes or questions, WhatsApp us directly at <strong>{r.phone}</strong>
or visit <a href="{r.website}">{r.website}</a>.</p>
<p style="color:#888;font-size:12px;">Please save your confirmation number: <strong>{r.confirmation_number}</strong></p>
"""
    return subject, body, _base_html(subject, body)


def build_exec_copy(r: EmailRequest) -> tuple[str, str, str]:
    subject = f"[BOOKING] {r.confirmation_number} — {r.client_name} · {r.vehicle}"
    vip_tag = ' <span class="badge vip">VIP</span>' if "YES" in (r.is_vip or "") else ""
    body = f"""
<p><strong>New booking received.</strong>{vip_tag}</p>
<div class="row"><span class="lbl">Confirmation</span><span class="val">{r.confirmation_number}</span></div>
<div class="row"><span class="lbl">Client</span><span class="val">{r.client_name}</span></div>
<div class="row"><span class="lbl">Email</span><span class="val">{r.client_email}</span></div>
<div class="row"><span class="lbl">Phone</span><span class="val">{r.client_phone}</span></div>
<div class="row"><span class="lbl">Vehicle</span><span class="val">{r.vehicle}</span></div>
<div class="row"><span class="lbl">Service</span><span class="val">{r.service_label}</span></div>
<div class="row"><span class="lbl">Total</span><span class="val"><strong>{r.total_amount}</strong></span></div>
<div class="row"><span class="lbl">Date</span><span class="val">{r.booking_date}</span></div>
<div class="row"><span class="lbl">VIP</span><span class="val">{r.is_vip}</span></div>
"""
    return subject, body, _base_html(subject, body)


def build_ops_alert(r: EmailRequest) -> tuple[str, str, str]:
    subject = f"[OPS DISPATCH] {r.confirmation_number} — {r.vehicle}"
    vip_tag = ' <span class="badge vip">VIP</span>' if r.is_vip == "YES" else ""
    body = f"""
<p><span class="badge ops">Dispatch Required</span>{vip_tag}</p>
<br/>
<div class="row"><span class="lbl">Confirmation</span><span class="val">{r.confirmation_number}</span></div>
<div class="row"><span class="lbl">Client</span><span class="val">{r.client_name}</span></div>
<div class="row"><span class="lbl">Pax</span><span class="val">{r.pax}</span></div>
<div class="row"><span class="lbl">Vehicle</span><span class="val">{r.vehicle}</span></div>
<div class="row"><span class="lbl">Service</span><span class="val">{r.service_label}</span></div>
<div class="row"><span class="lbl">Date</span><span class="val">{r.booking_date}</span></div>
"""
    return subject, body, _base_html(subject, body)


def build_venue_reservation(r: EmailRequest) -> tuple[str, str, str]:
    subject = f"Reservation Request — {r.client_name} · via {r.company}"
    body = f"""
<p>Dear {r.to_name},</p>
<p>We would like to request a reservation on behalf of our client:</p>
<div class="row"><span class="lbl">Client</span><span class="val">{r.client_name}</span></div>
<div class="row"><span class="lbl">Party Size</span><span class="val">{r.client_count} guests</span></div>
<div class="row"><span class="lbl">Requested By</span><span class="val">{r.company}</span></div>
<div class="row"><span class="lbl">IATA #</span><span class="val">{r.iata_number}</span></div>
<div class="row"><span class="lbl">Commission Rate</span><span class="val">{r.commission_rate}</span></div>
<br/>
<p>Please confirm availability and advise on any requirements.
We appreciate your continued partnership.</p>
<p>— The {r.company} Concierge Team</p>
"""
    return subject, body, _base_html(subject, body)


def build_commission_notification(r: EmailRequest) -> tuple[str, str, str]:
    subject = f"[COMMISSION] {r.venue_name} — Est. {r.commission_estimate}"
    body = f"""
<p><strong>Venue booking commission tracking:</strong></p>
<div class="row"><span class="lbl">Venue</span><span class="val">{r.venue_name}</span></div>
<div class="row"><span class="lbl">Est. Client Spend</span><span class="val">{r.estimated_spend}</span></div>
<div class="row"><span class="lbl">Commission Rate</span><span class="val">{r.commission_rate}</span></div>
<div class="row"><span class="lbl">Commission Est.</span><span class="val"><strong>{r.commission_estimate}</strong></span></div>
"""
    return subject, body, _base_html(subject, body)


# ─── TYPE DISPATCH ────────────────────────────────────────────────────────────
HANDLERS = {
    "client_confirmation":    build_client_confirmation,
    "exec_copy":              build_exec_copy,
    "ops_alert":              build_ops_alert,
    "venue_reservation":      build_venue_reservation,
    "commission_notification": build_commission_notification,
}


# ─── SMTP SENDER ──────────────────────────────────────────────────────────────
async def send_o365(to_email: str, to_name: str, subject: str, body_text: str, body_html: str):
    """Send via Office 365 SMTP using STARTTLS on port 587."""
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = f"{FROM_NAME} <{FROM_ADDR}>"
    msg["To"]      = f"{to_name} <{to_email}>"
    msg["Reply-To"] = FROM_ADDR

    msg.attach(MIMEText(body_text, "plain", "utf-8"))
    msg.attach(MIMEText(body_html, "html",  "utf-8"))

    await aiosmtplib.send(
        msg,
        hostname=SMTP_HOST,
        port=SMTP_PORT,
        username=SMTP_USER,
        password=SMTP_PASS,
        start_tls=True,   # Office 365 requires STARTTLS on 587
    )
    logger.info(f"[ILT EMAIL] Sent '{subject}' → {to_email}")


# ─── ENDPOINT ─────────────────────────────────────────────────────────────────
@router.post("")
async def send_email(req: EmailRequest):
    handler = HANDLERS.get(req.type)
    if not handler:
        raise HTTPException(status_code=400, detail=f"Unknown email type: {req.type!r}")

    if not SMTP_USER or not SMTP_PASS:
        logger.error("[ILT EMAIL] O365_SMTP_USER / O365_SMTP_PASSWORD not configured")
        raise HTTPException(status_code=503, detail="Email service not configured")

    subject, body_text_raw, body_html = handler(req)

    # Plain-text fallback: strip HTML tags naively
    import re
    body_text = re.sub(r"<[^>]+>", "", body_text_raw).strip()

    try:
        await send_o365(req.to_email, req.to_name or "Guest", subject, body_text, body_html)
    except aiosmtplib.SMTPException as e:
        logger.error(f"[ILT EMAIL] SMTP error: {e}")
        raise HTTPException(status_code=502, detail=f"SMTP send failed: {e}")
    except Exception as e:
        logger.error(f"[ILT EMAIL] Unexpected error: {e}")
        raise HTTPException(status_code=500, detail="Email send error")

    return {"status": "sent", "to": req.to_email, "type": req.type}
