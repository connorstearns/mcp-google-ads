from __future__ import annotations

import datetime
import json
import logging
import os
import random
import re
import time
import uuid
from difflib import get_close_matches
from typing import Any, Dict, List, Optional, Set

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import Message


# Google Ads
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

APP_NAME = "mcp-google-ads"
APP_VER  = "0.1.0"
MCP_PROTO_DEFAULT = "2024-11-05"
SUPPORTED_MCP_VERSIONS: List[str] = ["2024-11-05"]

def _latest_supported_protocol() -> str:
    return SUPPORTED_MCP_VERSIONS[-1]

def _validate_protocol_version_string(version: str) -> str:
    datetime.date.fromisoformat(version)  # raises if bad
    return version

def _negotiate_protocol_version(requested: Optional[str]) -> Optional[str]:
    if requested is None:
        return _latest_supported_protocol()
    # pick the newest supported <= requested
    for v in sorted(SUPPORTED_MCP_VERSIONS, reverse=True):
        if v <= requested:
            return v
    return None

# ---------- Logging & shared-secret ----------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(APP_NAME)
MCP_SHARED_KEY = os.getenv("MCP_SHARED_KEY", "").strip()
require_key = bool(MCP_SHARED_KEY)  # stays False if the var is empty

# ---------- Env & Ads client factory ----------
DEV_TOKEN         = os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN", "")
CLIENT_ID         = os.getenv("GOOGLE_ADS_CLIENT_ID", "")
CLIENT_SECRET     = os.getenv("GOOGLE_ADS_CLIENT_SECRET", "")
REFRESH_TOKEN     = os.getenv("GOOGLE_ADS_REFRESH_TOKEN", "")
LOGIN_CUSTOMER_ID = (os.getenv("GOOGLE_ADS_LOGIN_CUSTOMER_ID", "") or "").replace("-", "").strip()

def _require_env():
    missing = [k for k,v in [
        ("GOOGLE_ADS_DEVELOPER_TOKEN", DEV_TOKEN),
        ("GOOGLE_ADS_CLIENT_ID",       CLIENT_ID),
        ("GOOGLE_ADS_CLIENT_SECRET",   CLIENT_SECRET),
        ("GOOGLE_ADS_REFRESH_TOKEN",   REFRESH_TOKEN),
    ] if not v]
    if missing:
        raise RuntimeError(f"Missing required env: {', '.join(missing)}")

def _new_ads_client(login_cid: Optional[str] = None) -> GoogleAdsClient:
    _require_env()
    cfg = {
        "developer_token": DEV_TOKEN,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "refresh_token": REFRESH_TOKEN,
        "use_proto_plus": True,
    }
    arg_login = (login_cid or "").replace("-", "").strip()
    final_login = arg_login or LOGIN_CUSTOMER_ID
    if final_login:
        cfg["login_customer_id"] = final_login
    log.info("GoogleAds login_customer_id in use: %r (arg=%r env=%r)",
             cfg.get("login_customer_id", ""), arg_login, LOGIN_CUSTOMER_ID)
    return GoogleAdsClient.load_from_dict(cfg)

def _ga_search(svc, customer_id: str, query: str, page_size: Optional[int] = None, page_token: Optional[str] = None):
    # Newer google-ads clients use a fixed server page size (10,000). Do not send page_size.
    req = {"customer_id": str(customer_id), "query": query}
    if page_token:
        req["page_token"] = page_token
    return _ads_call(lambda: svc.search(request=req))

# ---------- FastAPI base ----------
app = FastAPI()

# 1) CORS (outermost)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,  # must be False when allow_origins=["*"]
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=["MCP-Protocol-Version", "Mcp-Session-Id", "X-Request-ID"],
)

# 2) Request ID (make a request-scoped id available to everything)
class RequestId(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        rid = request.headers.get("X-Request-ID") or uuid.uuid4().hex[:12]
        request.state.request_id = rid
        response = await call_next(request)
        # Echo so Cloud Run / clients can correlate
        response.headers["X-Request-ID"] = rid
        return response

app.add_middleware(RequestId)

# 3) RPC audit logging (reads body once, reinjects it; logs UA and auth headers presence)
class RPCAudit(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if request.url.path == "/" and request.method == "POST":
            body_bytes = await request.body()  # read once

            # Re-inject the body for downstream handlers (Starlette-safe)
            async def receive() -> Message:
                return {"type": "http.request", "body": body_bytes, "more_body": False}
            request._receive = receive  # type: ignore[attr-defined]

            method = "unknown"
            try:
                payload = json.loads(body_bytes.decode("utf-8") or "{}")
                method = "batch" if isinstance(payload, list) else (payload.get("method") or "").lower()
            except Exception:
                pass

            ua = request.headers.get("user-agent", "")
            has_x = "X-MCP-Key" in request.headers
            auth = request.headers.get("authorization", "")
            has_bearer = auth.lower().startswith("bearer ")
            rid = getattr(request.state, "request_id", "-")

            log.info(
                "RPC method=%s ua=%s key:x=%s bearer=%s rid=%s",
                method, ua, has_x, has_bearer, rid
            )

        return await call_next(request)

app.add_middleware(RPCAudit)

# 4) MCP protocol header (innermost; finalizes header based on negotiation)
class MCPProtocolHeader(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        final_proto = getattr(request.state, "mcp_protocol_version", None) or _latest_supported_protocol()
        response.headers["MCP-Protocol-Version"] = final_proto
        return response

app.add_middleware(MCPProtocolHeader)

# ---------- MCP tooling helpers ----------
def mcp_ok_json(_title: str, data: Any) -> Dict[str, Any]:
    """Return exactly one JSON content item (strict MCP clients prefer this)."""
    return {"content": [{"type": "json", "json": data}]}

def mcp_err(message: str, data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Standardized JSON-RPC tool error payload."""
    return {"code": -32000, "message": message, "data": data or {}}

def _validate_protocol_version_string(version: str) -> str:
    """Ensure the protocol version is ISO formatted (YYYY-MM-DD)."""
    try:
        datetime.date.fromisoformat(version)
    except Exception as exc:
        raise ValueError("Invalid protocol version format") from exc
    return version

def _negotiate_protocol_version(requested: Optional[str]) -> Optional[str]:
    """Pick the newest supported version that does not exceed the request."""
    if requested is None:
        return _latest_supported_protocol()
    for version in reversed(SUPPORTED_MCP_VERSIONS):
        if version <= requested:
            return version
    return None

class RequestId(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Trust a client-provided ID if present; otherwise generate one
        rid = request.headers.get("X-Request-ID") or uuid.uuid4().hex[:12]
        request.state.request_id = rid

        response = await call_next(request)

        # Echo it back so clients & Cloud Run request logs can correlate
        response.headers["X-Request-ID"] = rid
        return response

app.add_middleware(RequestId)


# ---------- Helpers (segments, entities) ----------
SEGMENT_MAP = {
    "date": "segments.date",
    "device": "segments.device",
    "network": "segments.ad_network_type",
    "hour": "segments.hour",
    "day_of_week": "segments.day_of_week",
    "quarter": "segments.quarter",
}

ENTITY_FROM = {
    "account": "customer",
    "campaign": "campaign",
    "ad_group": "ad_group",
    "ad": "ad_group_ad",
}

# ---------- Client name → customer_id resolution ----------
GADS_CLIENT_MAP_RAW = os.getenv("GADS_CLIENT_MAP", "").strip()
try:
    _STATIC_CLIENT_MAP = json.loads(GADS_CLIENT_MAP_RAW) if GADS_CLIENT_MAP_RAW else {}
except Exception:
    _STATIC_CLIENT_MAP = {}
    log.warning("Invalid GADS_CLIENT_MAP JSON; ignoring.")

def _norm_key(s: str) -> str:
    s = s or ""
    s = s.casefold()
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s

def _build_static_lookup() -> Dict[str, str]:
    out = {}
    for k, v in _STATIC_CLIENT_MAP.items():
        if not v:
            continue
        out[_norm_key(k)] = str(v).replace("-", "")
    return out

_STATIC_LOOKUP = _build_static_lookup()
_ACCOUNT_CACHE: Dict[str, Dict[str, str]] = {}

def _refresh_account_cache(root_mcc: Optional[str]) -> None:
    if not root_mcc:
        return
    try:
        client = _new_ads_client(login_cid=root_mcc.replace("-", ""))
        svc = client.get_service("GoogleAdsService")
        q = """
        SELECT
          customer_client.client_customer,
          customer_client.level,
          customer_client.descriptive_name
        FROM customer_client
        WHERE customer_client.level <= 10
        """
        rows = _ads_call(lambda: svc.search(request={
            "customer_id": str(root_mcc).replace("-", ""),
            "query": q
        }))
        local = {}
        for r in rows:
            name = r.customer_client.descriptive_name or ""
            cid  = str(r.customer_client.client_customer or r.customer_client.id or "")
            if cid:
                local[_norm_key(name)] = {"id": cid.replace("-", ""), "raw": name}
        _ACCOUNT_CACHE.clear()
        _ACCOUNT_CACHE.update(local)
        log.info("Account cache refreshed: %d names", len(_ACCOUNT_CACHE))
    except Exception as e:
        log.warning("Failed to refresh account cache: %s", e)

def _resolve_customer_id(name_or_id: str, root_mcc: Optional[str] = None) -> Optional[str]:
    if not name_or_id:
        return None
    s = str(name_or_id).strip()
    digits = re.sub(r"[^0-9]", "", s)
    # Looks like an ID already
    if re.fullmatch(r"\d{8,20}", digits):
        return digits

    key = _norm_key(s)

    # 1) Static map first
    if key in _STATIC_LOOKUP:
        return _STATIC_LOOKUP[key]

    # 2) Cache from MCC
    if not _ACCOUNT_CACHE and (root_mcc or LOGIN_CUSTOMER_ID):
        _refresh_account_cache(root_mcc or LOGIN_CUSTOMER_ID)

    if key in _ACCOUNT_CACHE:
        return _ACCOUNT_CACHE[key]["id"]

    # 3) Fuzzy match
    candidates = list(_STATIC_LOOKUP.keys() | _ACCOUNT_CACHE.keys())
    matches = get_close_matches(key, candidates, n=1, cutoff=0.8)
    if matches:
        m = matches[0]
        return _STATIC_LOOKUP.get(m) or (_ACCOUNT_CACHE.get(m) or {}).get("id")

    return None

def _ensure_customer_id_arg(args: Dict[str, Any], *, root_mcc: Optional[str] = None) -> str:
    """
    Resolve args['customer_id'] from args['customer'] or args['customer_name'] if needed.
    Mutates args to set 'customer_id'. Raises ValueError if cannot resolve.
    """
    cid = args.get("customer_id")
    cname = args.get("customer") or args.get("customer_name")
    if cid:
        args["customer_id"] = str(cid).replace("-", "")
        return args["customer_id"]
    if cname:
        resolved = _resolve_customer_id(str(cname), root_mcc)
        if not resolved:
            raise ValueError(f"Could not resolve client '{cname}' to a Google Ads customer ID")
        args["customer_id"] = resolved
        return resolved
    raise ValueError("Missing required param: customer_id (or provide 'customer'/'customer_name')")

# ---------- Time / retries / money ----------
def _normalize_time(params: Dict[str, Any]) -> Dict[str, str]:
    pr = (params.get("date_preset") or "").strip().upper()
    tr = params.get("time_range") or {}
    if pr and tr:
        pr = None  # prefer explicit range
    if pr:
        allow = {"TODAY","YESTERDAY","LAST_7_DAYS","LAST_30_DAYS","THIS_MONTH","LAST_MONTH"}
        if pr not in allow:
            raise ValueError(f"Unsupported date_preset '{pr}'")
        return {"preset": pr}
    if tr.get("since") and tr.get("until"):
        return {"start": tr["since"], "end": tr["until"]}
    # sensible default
    return {"preset": "LAST_30_DAYS"}

TRANSIENTS = {"UNAVAILABLE","DEADLINE_EXCEEDED","INTERNAL","RESOURCE_EXHAUSTED","ABORTED"}

def _ads_call(fn, attempts=5):
    for i in range(1, attempts+1):
        try:
            return fn()
        except GoogleAdsException as e:
            status = e.error.code().name if hasattr(e, "error") else "UNKNOWN"
            rid = getattr(e, "request_id", None)
            if status in TRANSIENTS and i < attempts:
                time.sleep(min(2**i, 20) + random.random())
                continue
            details = {
                "status": status,
                "request_id": rid,
                "errors": [{"message": err.message, "code": err.error_code.__class__.__name__}
                           for err in (getattr(e, "failure", None).errors or [])] if getattr(e, "failure", None) else []
            }
            if status == "PERMISSION_DENIED":
                details["hint"] = ("Set login_customer_id to your MCC (e.g. 9000159936) "
                                   "or configure GOOGLE_ADS_LOGIN_CUSTOMER_ID on the server.")
            raise RuntimeError(json.dumps(details))

def _gaql_where_for_time(tnorm: Dict[str,str]) -> str:
    if "preset" in tnorm:
        today = datetime.date.today()
        if tnorm["preset"] == "TODAY":
            start = end = today
        elif tnorm["preset"] == "YESTERDAY":
            d = today - datetime.timedelta(days=1); start=end=d
        elif tnorm["preset"] == "LAST_7_DAYS":
            end=today; start=today - datetime.timedelta(days=6)
        elif tnorm["preset"] == "LAST_30_DAYS":
            end=today; start=today - datetime.timedelta(days=29)
        elif tnorm["preset"] == "THIS_MONTH":
            start = today.replace(day=1); end = today
        elif tnorm["preset"] == "LAST_MONTH":
            first_this = today.replace(day=1)
            last_month_last_day = first_this - datetime.timedelta(days=1)
            start = last_month_last_day.replace(day=1)
            end   = last_month_last_day
        else:
            raise ValueError("Unsupported preset")
        return f" segments.date BETWEEN '{start:%Y-%m-%d}' AND '{end:%Y-%m-%d}' "
    else:
        return f" segments.date BETWEEN '{tnorm['start']}' AND '{tnorm['end']}' "

def _money(micros: Optional[int]) -> float:
    return round((micros or 0)/1_000_000, 6)

# ---------- TOOLS (schemas updated with limits & patterns) ----------
TOOLS = [
    {
        "name": "fetch_account_tree",
        "description": "List accessible customer hierarchy (manager → clients).",
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "root_customer_id": {
                    "type": "string",
                    "description": "Manager (MCC) customer id (digits or dashes)",
                    "maxLength": 20,
                    "pattern": "^[0-9-]+$"
                },
                "depth": {"type": "integer", "minimum": 1, "maximum": 10, "default": 2}
            },
            "required": ["root_customer_id"]
        }
    },
    {
        "name": "fetch_metrics",
        "description": "Generic metrics for account/campaign/ad_group/ad with optional segments. Arguments must be minimal and relevant.",
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "customer_id":   {"type": "string", "description": "Digits only; or provide 'customer'/'customer_name'", "maxLength": 20, "pattern": "^[0-9-]*$"},
                "customer":      {"type": "string", "description": "Client name or alias", "maxLength": 120},
                "customer_name": {"type": "string", "description": "Client name (alternative)", "maxLength": 120},
                "entity": {"type": "string", "enum": ["account","campaign","ad_group","ad"], "default": "campaign"},
                "ids": {
                    "type": "array",
                    "description": "Filter to specific IDs for the chosen entity",
                    "maxItems": 200,
                    "items": {"type": "string", "maxLength": 30, "pattern": "^[0-9-]*$"}
                },
                "fields": {
                    "type": "array",
                    "maxItems": 100,
                    "items": {"type": "string", "maxLength": 64},
                    "default": ["metrics.cost_micros","metrics.clicks","metrics.impressions","metrics.conversions","metrics.conversions_value"]
                },
                "segments": {
                    "type": "array",
                    "description": "Optional segments: date, device, network, hour, day_of_week, quarter",
                    "maxItems": 6,
                    "items": {"type": "string", "enum": ["date","device","network","hour","day_of_week","quarter"]}
                },
                "date_preset": {
                    "type": "string",
                    "enum": ["TODAY","YESTERDAY","LAST_7_DAYS","LAST_30_DAYS","THIS_MONTH","LAST_MONTH"]
                },
                "time_range": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "since": {"type": "string", "maxLength": 10, "pattern": "^\\d{4}-\\d{2}-\\d{2}$"},
                        "until": {"type": "string", "maxLength": 10, "pattern": "^\\d{4}-\\d{2}-\\d{2}$"}
                    }
                },
                "page_size":  {
                    "type": "integer",
                    "description": "DEPRECATED/IGNORED: Google Ads search uses a fixed server page size.",
                    "minimum": 1, "maximum": 10000, "default": 1000
                },
                "page_token": {"type": ["string","null"], "maxLength": 200},
                "login_customer_id": {"type": "string", "description": "Manager id for header (optional)", "maxLength": 20, "pattern": "^[0-9-]*$"}
            }
        }
    },
    {
        "name": "fetch_campaign_summary",
        "description": "Per-campaign KPIs with computed ctr/cpc/cpa/roas.",
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "customer_id":   {"type": "string", "maxLength": 20, "pattern": "^[0-9-]*$"},
                "customer":      {"type": "string", "maxLength": 120},
                "customer_name": {"type": "string", "maxLength": 120},
                "date_preset":   {"type": "string", "enum": ["TODAY","YESTERDAY","LAST_7_DAYS","LAST_30_DAYS","THIS_MONTH","LAST_MONTH"]},
                "time_range": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "since": {"type": "string", "maxLength": 10, "pattern": "^\\d{4}-\\d{2}-\\d{2}$"},
                        "until": {"type": "string", "maxLength": 10, "pattern": "^\\d{4}-\\d{2}-\\d{2}$"}
                    }
                },
                "login_customer_id": {"type": "string", "maxLength": 20, "pattern": "^[0-9-]*$"}
            }
        }
    },
    {
        "name": "list_recommendations",
        "description": "Google Ads Recommendations for a customer.",
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "customer_id":   {"type": "string", "maxLength": 20, "pattern": "^[0-9-]*$"},
                "customer":      {"type": "string", "maxLength": 120},
                "customer_name": {"type": "string", "maxLength": 120},
                "types": {
                    "type": "array",
                    "description": "Optional filter by recommendation.type",
                    "maxItems": 50,
                    "items": {"type": "string", "maxLength": 64}
                },
                "login_customer_id": {"type": "string", "maxLength": 20, "pattern": "^[0-9-]*$"}
            }
        }
    },
    {
        "name": "fetch_search_terms",
        "description": "Search terms with basic filters.",
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "customer_id":   {"type": "string", "maxLength": 20, "pattern": "^[0-9-]*$"},
                "customer":      {"type": "string", "maxLength": 120},
                "customer_name": {"type": "string", "maxLength": 120},
                "date_preset":   {"type": "string", "enum": ["TODAY","YESTERDAY","LAST_7_DAYS","LAST_30_DAYS","THIS_MONTH","LAST_MONTH"]},
                "time_range": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "since": {"type": "string", "maxLength": 10, "pattern": "^\\d{4}-\\d{2}-\\d{2}$"},
                        "until": {"type": "string", "maxLength": 10, "pattern": "^\\d{4}-\\d{2}-\\d{2}$"}
                    }
                },
                "min_clicks": {"type": "integer", "minimum": 0, "default": 0},
                "min_cost_micros": {"type": "integer", "minimum": 0, "default": 0},
                "campaign_ids": {
                    "type": "array",
                    "maxItems": 200,
                    "items": {"type": "string", "maxLength": 30, "pattern": "^[0-9-]*$"}
                },
                "ad_group_ids": {
                    "type": "array",
                    "maxItems": 200,
                    "items": {"type": "string", "maxLength": 30, "pattern": "^[0-9-]*$"}
                },
                "login_customer_id": {"type": "string", "maxLength": 20, "pattern": "^[0-9-]*$"}
            }
        }
    },
    {
        "name": "fetch_change_history",
        "description": "Change events within a date range.",
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "customer_id":   {"type": "string", "maxLength": 20, "pattern": "^[0-9-]*$"},
                "customer":      {"type": "string", "maxLength": 120},
                "customer_name": {"type": "string", "maxLength": 120},
                "time_range": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "since": {"type": "string", "maxLength": 19, "pattern": "^\\d{4}-\\d{2}-\\d{2}$"},
                        "until": {"type": "string", "maxLength": 19, "pattern": "^\\d{4}-\\d{2}-\\d{2}$"}
                    }
                },
                "resource_types": {
                    "type": "array",
                    "description": "Optional: filter by change_event.resource_type",
                    "maxItems": 50,
                    "items": {"type": "string", "maxLength": 64}
                },
                "login_customer_id": {"type": "string", "maxLength": 20, "pattern": "^[0-9-]*$"}
            },
            "required": ["time_range"]
        }
    },
    {
        "name": "fetch_budget_pacing",
        "description": "Month-to-date spend and projected end-of-month vs target.",
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "customer_id":   {"type": "string", "maxLength": 20, "pattern": "^[0-9-]*$"},
                "customer":      {"type": "string", "maxLength": 120},
                "customer_name": {"type": "string", "maxLength": 120},
                "month": {"type": "string", "description": "YYYY-MM", "maxLength": 7, "pattern": "^\\d{4}-\\d{2}$"},
                "target_spend": {"type": "number", "description": "Target for the month in account currency"},
                "login_customer_id": {"type": "string", "maxLength": 20, "pattern": "^[0-9-]*$"}
            },
            "required": ["month","target_spend"]
        }
    },
        {
        "name": "list_resources",
        "description": "List accessible Google Ads customer accounts for the authenticated user.",
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "login_customer_id": {
                    "type": "string",
                    "description": "Optional manager (MCC) header override",
                    "maxLength": 20,
                    "pattern": "^[0-9-]*$"
                }
            }
        }
    },
    {
        "name": "resolve_customer",
        "description": "Resolve a client name or alias (or raw ID) to a normalized customer_id.",
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "customer": {"type":"string", "description":"Client name, alias, or numeric ID", "maxLength": 120},
                "login_customer_id": {"type":"string", "description":"MCC for hierarchy lookup (optional)", "maxLength": 20, "pattern": "^[0-9-]*$"}
            },
            "required": ["customer"]
        }
    }
]

# ---------- Public/debug tools registration ----------
TOOLS.append({
    "name": "ping",
    "description": "Health check (public).",
    "inputSchema": {"type": "object", "additionalProperties": False, "properties": {}}
})

TOOLS.append({
    "name": "debug_login_header",
    "description": "Show which login_customer_id (MCC) the server will use.",
    "inputSchema": {"type": "object", "additionalProperties": False, "properties": {}}
})

TOOLS.append({
    "name": "echo_short",
    "description": "Echo a short string. Use only for debugging tool calls.",
    "inputSchema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {"msg": {"type": "string", "maxLength": 80}},
        "required": ["msg"]
    }
})

TOOLS.append({
    "name": "noop_ok",
    "description": "Returns a tiny fixed JSON object.",
    "inputSchema": {"type": "object", "additionalProperties": False, "properties": {}}
})

# Public tools visible without auth (TEMP: expose everything while testing)
PUBLIC_TOOLS: Set[str] = {
    "ping", "noop_ok", "debug_login_header", "echo_short",
    "list_resources", "resolve_customer",
    "fetch_account_tree", "fetch_metrics", "fetch_campaign_summary",
    "list_recommendations", "fetch_search_terms", "fetch_change_history",
    "fetch_budget_pacing",
}


# ---------- Tool implementations ----------
def tool_ping(_args: Dict[str, Any]) -> Dict[str, Any]:
    return {"ok": True, "time": datetime.datetime.utcnow().isoformat() + "Z"}

def tool_debug_login_header(_args: Dict[str, Any]) -> Dict[str, Any]:
    return {"env_LOGIN_CUSTOMER_ID": LOGIN_CUSTOMER_ID}

def tool_echo_short(args: Dict[str, Any]) -> Dict[str, Any]:
    m = (args.get("msg") or "").strip()
    if not m:
        raise ValueError("msg required")
    return {"msg": m}

def tool_noop_ok(_args: Dict[str, Any]) -> Dict[str, Any]:
    return {"ok": True}

def tool_fetch_account_tree(args: Dict[str, Any]) -> Dict[str, Any]:
    root = args["root_customer_id"].replace("-", "")
    depth = int(args.get("depth", 2))
    client = _new_ads_client(login_cid=root)
    svc = client.get_service("GoogleAdsService")

    # GAQL: list customer_client hierarchy
    q = f"""
    SELECT
      customer_client.client_customer,
      customer_client.level,
      customer_client.descriptive_name,
      customer_client.manager,
      customer_client.id
    FROM customer_client
    WHERE customer_client.level <= {depth}
    """
    rows = _ads_call(lambda: svc.search(request={
        "customer_id": str(root),
        "query": q
    }))

    out = []
    for r in rows:
        cc = r.customer_client
        out.append({
            "id": str(cc.id),
            "client_customer": str(cc.client_customer),
            "name": cc.descriptive_name,
            "level": cc.level,
            "manager": cc.manager,
        })
    return {"root": root, "clients": out}

def tool_fetch_metrics(args: Dict[str, Any]) -> Dict[str, Any]:
    login = (args.get("login_customer_id") or LOGIN_CUSTOMER_ID or "").replace("-", "") or None
    customer_id = _ensure_customer_id_arg(args, root_mcc=login).replace("-", "")
    entity = args.get("entity","campaign")
    if entity not in ENTITY_FROM: raise ValueError("Invalid entity")
    fields = args.get("fields") or ["metrics.cost_micros","metrics.clicks","metrics.impressions","metrics.conversions","metrics.conversions_value"]
    ids = [i.replace("-","") for i in (args.get("ids") or [])]
    segs_in = args.get("segments") or []
    segs = [SEGMENT_MAP.get(s,s) for s in segs_in]
    tnorm = _normalize_time(args)
    where_time = _gaql_where_for_time(tnorm)

    select_cols = []
    if entity == "account":
        select_cols += ["customer.id","customer.descriptive_name"]
    elif entity == "campaign":
        select_cols += ["campaign.id","campaign.name","campaign.status"]
    elif entity == "ad_group":
        select_cols += ["ad_group.id","ad_group.name","ad_group.status","campaign.id","campaign.name"]
    else:
        select_cols += ["ad_group_ad.ad.id","ad_group_ad.ad.name","ad_group.id","ad_group.name","campaign.id","campaign.name"]

    select_cols += fields + segs

    frm = ENTITY_FROM[entity]
    where_ids = ""
    if ids:
        col = {"account":"customer.id","campaign":"campaign.id","ad_group":"ad_group.id","ad":"ad_group_ad.ad.id"}[entity]
        where_ids = f" AND {col} IN ({','.join(ids)}) "

        # Build GAQL
    q = f"""SELECT {", ".join(select_cols)} FROM {frm} WHERE {where_time} {where_ids}"""

    # Client + service
    client = _new_ads_client(login_cid=login)
    svc = client.get_service("GoogleAdsService")

    # Request (do not send page_size; server has a fixed size)
    req = {"customer_id": str(customer_id), "query": q}
    page_token = args.get("page_token")
    if page_token:
        req["page_token"] = page_token

    # Execute
    resp = _ads_call(lambda: svc.search(request=req))

    # Prepare output
    out_rows: List[Dict[str, Any]] = []
    max_rows = int(os.getenv("MCP_MAX_ROWS", "2000"))
    next_token = getattr(resp, "next_page_token", "") or ""
    count = 0

    # Iterate rows (cap at max_rows to keep payloads modest)
    for r in resp:
        obj: Dict[str, Any] = {}
        try:
            if entity == "account":
                obj["customer_id"] = str(r.customer.id)
                obj["customer_name"] = r.customer.descriptive_name
            elif entity == "campaign":
                obj["campaign_id"] = str(r.campaign.id)
                obj["campaign_name"] = r.campaign.name
                obj["campaign_status"] = r.campaign.status.name
            elif entity == "ad_group":
                obj["ad_group_id"] = str(r.ad_group.id)
                obj["ad_group_name"] = r.ad_group.name
                obj["ad_group_status"] = r.ad_group.status.name
                obj["campaign_id"] = str(r.campaign.id)
                obj["campaign_name"] = r.campaign.name
            else:
                obj["ad_id"] = str(r.ad_group_ad.ad.id)
                obj["ad_group_id"] = str(r.ad_group.id)
                obj["ad_group_name"] = r.ad_group.name
                obj["campaign_id"] = str(r.campaign.id)
                obj["campaign_name"] = r.campaign.name

            m = r.metrics
            obj.update({
                "cost": _money(getattr(m, "cost_micros", None)),
                "impressions": getattr(m, "impressions", 0),
                "clicks": getattr(m, "clicks", 0),
                "conversions": getattr(m, "conversions", 0.0),
                "conversions_value": getattr(m, "conversions_value", 0.0),
            })

            # Optional segments
            if segs:
                if "segments.date" in segs:
                    obj["date"] = str(r.segments.date)
                if "segments.device" in segs:
                    obj["device"] = r.segments.device.name
                if "segments.ad_network_type" in segs:
                    obj["network"] = r.segments.ad_network_type.name
                if "segments.hour" in segs and hasattr(r.segments, "hour"):
                    obj["hour"] = getattr(r.segments, "hour", None)
                if "segments.day_of_week" in segs and hasattr(r.segments, "day_of_week"):
                    obj["day_of_week"] = getattr(r.segments, "day_of_week", None)
                if "segments.quarter" in segs and hasattr(r.segments, "quarter"):
                    obj["quarter"] = getattr(r.segments, "quarter", None)

        except Exception:
            # Best-effort row; skip if structure unexpected
            pass

        out_rows.append(obj)
        count += 1
        if count >= max_rows:
            break

    return {
        "query": q,
        "count": len(out_rows),
        "next_page_token": next_token,
        "rows": out_rows
    }

def tool_fetch_campaign_summary(args: Dict[str, Any]) -> Dict[str, Any]:
    login = (args.get("login_customer_id") or LOGIN_CUSTOMER_ID or "").replace("-", "") or None
    customer_id = _ensure_customer_id_arg(args, root_mcc=login).replace("-", "")
    tnorm = _normalize_time(args)
    where_time = _gaql_where_for_time(tnorm)
    q = f"""
    SELECT
      campaign.id, campaign.name, campaign.status,
      metrics.cost_micros, metrics.impressions, metrics.clicks, metrics.conversions, metrics.conversions_value
    FROM campaign
    WHERE {where_time}
    """
    client = _new_ads_client(login_cid=login)
    svc = client.get_service("GoogleAdsService")
    rows = _ads_call(lambda: svc.search(request={
        "customer_id": str(customer_id),
        "query": q
    }))

    out = []
    for r in rows:
        cost = _money(r.metrics.cost_micros)
        imps = int(r.metrics.impressions or 0)
        clicks = int(r.metrics.clicks or 0)
        conv = float(r.metrics.conversions or 0.0)
        conv_value = float(r.metrics.conversions_value or 0.0)
        ctr = (clicks / imps * 100) if imps else 0.0
        cpc = (cost / clicks) if clicks else 0.0
        cpa = (cost / conv) if conv else 0.0
        roas = (conv_value / cost) if cost > 0 else 0.0
        out.append({
            "campaign_id": str(r.campaign.id),
            "campaign_name": r.campaign.name,
            "status": r.campaign.status.name,
            "cost": round(cost, 2),
            "impressions": imps,
            "clicks": clicks,
            "conversions": round(conv, 2),
            "conv_value": round(conv_value, 2),
            "ctr_pct": round(ctr, 2),
            "cpc": round(cpc, 2),
            "cpa": round(cpa, 2),
            "roas": round(roas, 2),
        })
    return {"summary": out, "query": q}

def tool_list_recommendations(args: Dict[str, Any]) -> Dict[str, Any]:
    login = (args.get("login_customer_id") or LOGIN_CUSTOMER_ID or "").replace("-", "") or None
    customer_id = _ensure_customer_id_arg(args, root_mcc=login).replace("-", "")
    types = args.get("types") or []
    type_filter = ""
    if types:
        safe = ",".join([f"'{t}'" for t in types])
        type_filter = f" AND recommendation.type IN ({safe}) "
    q = f"""
    SELECT
      recommendation.type,
      recommendation.resource_name,
      recommendation.dismissed,
      recommendation.campaign,
      recommendation.ad_group
    FROM recommendation
    WHERE 1=1 {type_filter}
    """
    client = _new_ads_client(login_cid=login)
    svc = client.get_service("GoogleAdsService")
    rows = _ads_call(lambda: svc.search(request={
        "customer_id": str(customer_id),
        "query": q
    }))

    out = []
    for r in rows:
        out.append({
            "type": r.recommendation.type.name,
            "resource_name": r.recommendation.resource_name,
            "dismissed": r.recommendation.dismissed,
            "campaign": str(r.recommendation.campaign) if r.recommendation.campaign else None,
            "ad_group": str(r.recommendation.ad_group) if r.recommendation.ad_group else None,
        })
    return {"recommendations": out, "query": q}

def tool_fetch_search_terms(args: Dict[str, Any]) -> Dict[str, Any]:
    login = (args.get("login_customer_id") or LOGIN_CUSTOMER_ID or "").replace("-", "") or None
    customer_id = _ensure_customer_id_arg(args, root_mcc=login).replace("-", "")
    tnorm = _normalize_time(args)
    where_time = _gaql_where_for_time(tnorm)
    min_clicks = int(args.get("min_clicks", 0))
    min_cost = int(args.get("min_cost_micros", 0))
    cids = [c.replace("-","") for c in (args.get("campaign_ids") or [])]
    agids = [g.replace("-","") for g in (args.get("ad_group_ids") or [])]

    filters = [where_time]
    if min_clicks > 0: filters.append(f" AND metrics.clicks >= {min_clicks} ")
    if min_cost > 0:   filters.append(f" AND metrics.cost_micros >= {min_cost} ")
    if cids:           filters.append(f" AND campaign.id IN ({','.join(cids)}) ")
    if agids:          filters.append(f" AND ad_group.id IN ({','.join(agids)}) ")

    q = f"""
    SELECT
      search_term_view.search_term,
      campaign.id, campaign.name,
      ad_group.id, ad_group.name,
      metrics.impressions, metrics.clicks, metrics.cost_micros, metrics.conversions, metrics.conversions_value
    FROM search_term_view
    WHERE {''.join(filters)}
    """
    client = _new_ads_client(login_cid=login)
    svc = client.get_service("GoogleAdsService")
    rows = _ads_call(lambda: svc.search(request={
        "customer_id": str(customer_id),
        "query": q
    }))

    out = []
    for r in rows:
        out.append({
            "search_term": r.search_term_view.search_term,
            "campaign_id": str(r.campaign.id),
            "campaign_name": r.campaign.name,
            "ad_group_id": str(r.ad_group.id),
            "ad_group_name": r.ad_group.name,
            "impressions": int(r.metrics.impressions or 0),
            "clicks": int(r.metrics.clicks or 0),
            "cost": _money(r.metrics.cost_micros),
            "conversions": float(r.metrics.conversions or 0.0),
            "conv_value": float(r.metrics.conversions_value or 0.0),
        })
    return {"rows": out, "query": q}

def tool_fetch_change_history(args: Dict[str, Any]) -> Dict[str, Any]:
    login = (args.get("login_customer_id") or LOGIN_CUSTOMER_ID or "").replace("-", "") or None
    customer_id = _ensure_customer_id_arg(args, root_mcc=login).replace("-", "")
    tr = args["time_range"]; since = tr["since"]; until = tr["until"]
    types = args.get("resource_types") or []
    type_filter = ""
    if types:
        safe = ",".join([f"'{t}'" for t in types])
        type_filter = f" AND change_event.resource_type IN ({safe}) "
    q = f"""
    SELECT
      change_event.change_date_time,
      change_event.change_resource_name,
      change_event.resource_type,
      change_event.user_email,
      change_event.client_type,
      change_event.old_resource,
      change_event.new_resource
    FROM change_event
    WHERE change_event.change_date_time BETWEEN '{since} 00:00:00' AND '{until} 23:59:59' {type_filter}
    ORDER BY change_event.change_date_time DESC
    """
    client = _new_ads_client(login_cid=login)
    svc = client.get_service("GoogleAdsService")
    rows = _ads_call(lambda: svc.search(request={
        "customer_id": str(customer_id),
        "query": q
    }))

    out = []
    for r in rows:
        out.append({
            "time": r.change_event.change_date_time,
            "resource_type": r.change_event.resource_type.name,
            "user": r.change_event.user_email,
            "client_type": r.change_event.client_type.name,
            "change_resource_name": r.change_event.change_resource_name,
        })
    return {"changes": out, "query": q}

def tool_fetch_budget_pacing(args: Dict[str, Any]) -> Dict[str, Any]:
    login = (args.get("login_customer_id") or LOGIN_CUSTOMER_ID or "").replace("-", "") or None
    customer_id = _ensure_customer_id_arg(args, root_mcc=login).replace("-", "")
    month = args["month"]  # YYYY-MM
    target = float(args["target_spend"])
    year, mon = map(int, month.split("-"))
    start = datetime.date(year, mon, 1)
    today = datetime.date.today()
    if today.year == year and today.month == mon:
        end = today
        days_elapsed = (end - start).days + 1
        next_month = (start.replace(day=28) + datetime.timedelta(days=4)).replace(day=1)
        days_in_month = (next_month - start).days
    else:
        next_month = (start.replace(day=28) + datetime.timedelta(days=4)).replace(day=1)
        end = next_month - datetime.timedelta(days=1)
        days_in_month = (next_month - start).days
        days_elapsed = days_in_month

    q = f"""
    SELECT
      segments.date,
      metrics.cost_micros
    FROM customer
    WHERE segments.date BETWEEN '{start:%Y-%m-%d}' AND '{end:%Y-%m-%d}'
    """
    client = _new_ads_client(login_cid=login)
    svc = client.get_service("GoogleAdsService")
    rows = _ads_call(lambda: svc.search(request={
        "customer_id": str(customer_id),
        "query": q
    }))

    mtd_cost = 0
    for r in rows:
        mtd_cost += _money(r.metrics.cost_micros)
    avg_per_day = (mtd_cost / days_elapsed) if days_elapsed else 0.0
    projected_eom = round(avg_per_day * days_in_month, 2)
    pace_status = "on_track"
    if projected_eom > target * 1.05: pace_status = "over"
    elif projected_eom < target * 0.95: pace_status = "under"

    return {
        "month": month, "target": round(target,2),
        "mtd_spend": round(mtd_cost,2),
        "projected_eom": projected_eom,
        "days_elapsed": days_elapsed, "days_in_month": days_in_month,
        "pace_status": pace_status,
        "query": q
    }

def tool_list_resources(args: Dict[str, Any]) -> Dict[str, Any]:
    login = (args.get("login_customer_id") or LOGIN_CUSTOMER_ID or "").replace("-", "") or None
    client = _new_ads_client(login_cid=login)
    svc = client.get_service("CustomerService")
    response = _ads_call(lambda: svc.list_accessible_customers())
    customers: List[Dict[str, str]] = []
    for resource_name in response.resource_names:
        customer_id = resource_name.split("/")[-1]
        customers.append({"resource_name": resource_name, "customer_id": customer_id})
    return {"count": len(customers), "customers": customers}

def tool_resolve_customer(args: Dict[str, Any]) -> Dict[str, Any]:
    login = (args.get("login_customer_id") or LOGIN_CUSTOMER_ID or "").replace("-", "") or None
    target = args["customer"]
    resolved = _resolve_customer_id(target, login)
    return {"input": target, "resolved_customer_id": resolved}

# ---------- Health & discovery ----------
@app.get("/", include_in_schema=False)
@app.head("/", include_in_schema=False)
async def root_get(request: Request):
    if request.method == "HEAD":
        return PlainTextResponse("")
    return JSONResponse({
        "ok": True,
        "message": "MCP server. POST / for JSON-RPC; see /.well-known/mcp.json and /mcp/*"
    })

# Quiet favicon noise in logs (optional but nice)
@app.get("/favicon.ico", include_in_schema=False)
def favicon_ico(): return Response(status_code=204)
@app.get("/favicon.png", include_in_schema=False)
def favicon_png(): return Response(status_code=204)
@app.get("/favicon.svg", include_in_schema=False)
def favicon_svg(): return Response(status_code=204)

def _is_authed(request: Request) -> bool:
    """Shared-secret auth check for discovery and other handlers."""
    if not MCP_SHARED_KEY:
        return True  # No shared key configured => effectively open
    auth_hdr = request.headers.get("Authorization", "")
    bearer_ok = auth_hdr.lower().startswith("bearer ") and auth_hdr.split(" ", 1)[1].strip() == MCP_SHARED_KEY
    xhdr_ok = request.headers.get("X-MCP-Key", "") == MCP_SHARED_KEY
    return bearer_ok or xhdr_ok

@app.get("/.well-known/mcp.json")
def mcp_discovery(request: Request):
    # Advertise auth mode
    if MCP_SHARED_KEY:
        auth = {
            "type": "shared-secret",
            "tokenHeader": "Authorization",  # prefer Bearer
            "scheme": "Bearer",
            "altHeaders": ["X-MCP-Key"],     # optional secondary header
        }
    else:
        auth = {"type": "none"}

    # Filter tools based on auth (hide non-public tools when not authed)
    authed = _is_authed(request)
    visible_tools = [t for t in TOOLS if authed or t.get("name") in PUBLIC_TOOLS]

    return JSONResponse({
        "mcpVersion": _latest_supported_protocol(),
        "supportedVersions": SUPPORTED_MCP_VERSIONS,
        "name": APP_NAME,
        "version": APP_VER,
        "auth": auth,
        "capabilities": {"tools": {"listChanged": True}},
        "endpoints": {"rpc": "/"},
        "tools": visible_tools,
    })

@app.get("/mcp/tools")
def mcp_tools(request: Request):
    """Optional endpoint; mirror discovery's visibility rules."""
    authed = _is_authed(request)
    visible_tools = [t for t in TOOLS if authed or t.get("name") in PUBLIC_TOOLS]
    return JSONResponse({"tools": visible_tools})

def _build_jsonrpc_error(_id: Any, code: int, message: str, data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    body: Dict[str, Any] = {"jsonrpc": "2.0", "id": _id, "error": {"code": code, "message": message}}
    if data is not None:
        body["error"]["data"] = data
    return body


def _handle_single_rpc(
    obj: Any,
    request: Request,
    headers: Dict[str, str],
    status: Dict[str, int],
    public_tools: Set[str],
) -> Optional[Dict[str, Any]]:
    """Handle one JSON-RPC object and return a JSON-RPC response object, or None for notifications."""
    if not isinstance(obj, dict):
        return _build_jsonrpc_error(None, -32600, "Invalid Request")

    rid = getattr(request.state, "request_id", "-")
    payload = obj
    is_notification = ("id" not in payload and payload.get("jsonrpc") == "2.0" and "method" in payload)
    _id = payload.get("id")
    method = (payload.get("method") or "").lower()

    require_key = bool(MCP_SHARED_KEY)

    # ---- Auth gate for non-public tools (JSON-RPC error only; don't flip outer HTTP status) ----
    if require_key and method == "tools/call":
        params = (payload.get("params") or {})
        tool_name = (params.get("name") or "").lower()
        if tool_name not in public_tools:
            auth_hdr = request.headers.get("Authorization", "")
            has_bearer = auth_hdr.lower().startswith("bearer ")
            bearer_ok = has_bearer and auth_hdr.split(" ", 1)[1].strip() == MCP_SHARED_KEY
            has_xhdr = "X-MCP-Key" in request.headers
            xhdr_ok = request.headers.get("X-MCP-Key", "") == MCP_SHARED_KEY

            if not (bearer_ok or xhdr_ok):
                log.warning(
                    "401 on tools/call tool=%s has_bearer=%s has_xmcp=%s rid=%s",
                    tool_name, has_bearer, has_xhdr, rid
                )
                headers["WWW-Authenticate"] = 'Bearer realm="mcp-google-ads"'
                if is_notification:
                    return None
                return _build_jsonrpc_error(_id, -32001, "Unauthorized")

    def success(result: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if is_notification:
            return None
        return {"jsonrpc": "2.0", "id": _id, "result": result}

    def error(code: int, message: str, data: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        if is_notification:
            return None
        return _build_jsonrpc_error(_id, code, message, data)

    # ---------------- initialize ----------------
    if method == "initialize":
        raw_proto = (
            (payload.get("params") or {}).get("protocolVersion")
            or request.headers.get("MCP-Protocol-Version")
            or request.headers.get("Mcp-Protocol-Version")
            or None
        )

        # validate the incoming string if present
        try:
            requested = _validate_protocol_version_string(raw_proto) if raw_proto else None
        except ValueError:
            latest = _latest_supported_protocol()
            headers["MCP-Protocol-Version"] = latest
            request.state.mcp_protocol_version = latest
            return error(-32602, "Invalid protocolVersion format", {"supportedVersions": SUPPORTED_MCP_VERSIONS})

        # negotiate down to a supported version
        negotiated = _negotiate_protocol_version(requested)
        if negotiated is None:
            latest = _latest_supported_protocol()
            headers["MCP-Protocol-Version"] = latest
            request.state.mcp_protocol_version = latest
            return error(-32602, "Unsupported protocolVersion", {"supportedVersions": SUPPORTED_MCP_VERSIONS})

        # pin response/header to negotiated version
        headers["MCP-Protocol-Version"] = negotiated
        request.state.mcp_protocol_version = negotiated

        authed = _is_authed(request)
        visible_tools = [t for t in TOOLS if authed or t.get("name") in PUBLIC_TOOLS]

        log.info(
            "protocol negotiated requested=%s -> %s rid=%s",
            raw_proto, negotiated, getattr(request.state, "request_id", "-"),
        )

        return success({
            "protocolVersion": negotiated,
            "capabilities": {"tools": {"listChanged": True}},
            "serverInfo": {"name": APP_NAME, "version": APP_VER},
            "tools": visible_tools,
        })

    # ---------------- initialized ack ----------------
    if method in ("initialized", "notifications/initialized"):
        return {"jsonrpc": "2.0", "id": _id or "notif", "result": {"ok": True}}

    # ---------------- tools/list ----------------
    if method in ("tools/list", "tools.list", "list_tools", "tools.index"):
        authed = _is_authed(request)
        visible_tools = [t for t in TOOLS if authed or t.get("name") in PUBLIC_TOOLS]
        return success({"tools": visible_tools})

    # ---------------- tools/call ----------------
    if method == "tools/call":
        params = payload.get("params") or {}
        name = params.get("name")
        args = params.get("arguments") or {}

        TOOL_IMPLS = {
            "fetch_account_tree":     (tool_fetch_account_tree,     "Account tree"),
            "fetch_metrics":          (tool_fetch_metrics,          "Metrics"),
            "fetch_campaign_summary": (tool_fetch_campaign_summary, "Campaign summary"),
            "list_recommendations":   (tool_list_recommendations,   "Recommendations"),
            "fetch_search_terms":     (tool_fetch_search_terms,     "Search terms"),
            "fetch_change_history":   (tool_fetch_change_history,   "Change history"),
            "fetch_budget_pacing":    (tool_fetch_budget_pacing,    "Budget pacing"),
            "list_resources":         (tool_list_resources,         "Resources"),
            "resolve_customer":       (tool_resolve_customer,       "Resolved customer"),
            # debug
            "ping":                   (tool_ping,                   "pong"),
            "debug_login_header":     (tool_debug_login_header,     "Debug login header"),
            "echo_short":             (tool_echo_short,             "echo"),
            "noop_ok":                (tool_noop_ok,                "ok"),
        }

        try:
            log.info("tools/call start name=%s rid=%s", name, rid)
            handler = TOOL_IMPLS.get(name)
            if not handler:
                return error(-32601, f"Unknown tool: {name}")

            func, title = handler
            data = func(args)

            # Always return a single JSON content item for strict MCP clients.
            res = mcp_ok_json(title, data)

            log.info("tools/call ok name=%s rid=%s", name, rid)
            return success(res)

        except ValueError as ve:
            log.warning("tools/call invalid_params name=%s rid=%s error=%s", name, rid, ve)
            return error(-32602, f"Invalid params: {ve}")

        except Exception as e:
            log.exception("tools/call failed name=%s rid=%s", name, rid)
            try:
                data = json.loads(str(e))
            except Exception:
                data = {"detail": str(e)}
            log.info("tools/call error_data name=%s rid=%s data=%s", name, rid, data)
            return {"jsonrpc": "2.0", "id": _id, "error": mcp_err("Google Ads API error", data)}

    # ---------------- fallback ----------------
    return error(-32601, f"Method not found: {method}")


@app.post("/")
async def rpc(request: Request):
    """JSON-RPC endpoint that supports single objects and batches."""
    proto_header = request.headers.get("MCP-Protocol-Version", MCP_PROTO_DEFAULT)
    headers: Dict[str, str] = {"MCP-Protocol-Version": proto_header}
    status = {"code": 200}
    public_tools: Set[str] = PUBLIC_TOOLS

    def _sync_protocol_header() -> None:
        negotiated = getattr(request.state, "mcp_protocol_version", None)
        if negotiated:
            headers["MCP-Protocol-Version"] = negotiated
        elif not headers.get("MCP-Protocol-Version"):
            headers["MCP-Protocol-Version"] = _latest_supported_protocol()

    try:
        payload = await request.json()

        # Batch
        if isinstance(payload, list):
            responses: List[Dict[str, Any]] = []
            for entry in payload:
                resp = _handle_single_rpc(entry, request, headers, status, public_tools)
                _sync_protocol_header()
                if resp is not None:
                    responses.append(resp)

            rid = getattr(request.state, "request_id", "-")
            log.info("resp headers: %s rid=%s", headers, rid)

            if responses:
                return JSONResponse(responses, status_code=status["code"], headers=headers)
            return JSONResponse([], status_code=200, headers=headers)

        # Single
        resp = _handle_single_rpc(payload, request, headers, status, public_tools)
        _sync_protocol_header()

        rid = getattr(request.state, "request_id", "-")
        log.info("resp headers: %s rid=%s", headers, rid)

        if resp is not None:
            return JSONResponse(resp, status_code=status["code"], headers=headers)
        return Response(status_code=200, headers=headers)

    except Exception as e:
        log.exception("RPC dispatch error")
        _sync_protocol_header()
        rid = getattr(request.state, "request_id", "-")
        log.info("resp headers: %s rid=%s", headers, rid)
        return JSONResponse(
            {"jsonrpc": "2.0", "id": None, "error": {"code": -32098, "message": f"RPC dispatch error: {e}"}},
            headers=headers
        )

# ---------- Local dev ----------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=int(os.getenv("PORT","8080")))
