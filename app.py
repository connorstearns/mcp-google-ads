import os, json, time, random, logging, datetime
from typing import Dict, Any, Optional, List, Tuple

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware

# Google Ads
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

APP_NAME = "mcp-google-ads"
APP_VER  = "0.1.0"
MCP_PROTO_DEFAULT = "2024-11-05"

# ---------- Logging & shared-secret ----------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(APP_NAME)
MCP_SHARED_KEY = os.getenv("MCP_SHARED_KEY", "").strip()
log.info("MCP_SHARED_KEY enabled? %s", "YES" if MCP_SHARED_KEY else "NO")

# ---------- Env & Ads client factory ----------
DEV_TOKEN         = os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN", "")
CLIENT_ID         = os.getenv("GOOGLE_ADS_CLIENT_ID", "")
CLIENT_SECRET     = os.getenv("GOOGLE_ADS_CLIENT_SECRET", "")
REFRESH_TOKEN     = os.getenv("GOOGLE_ADS_REFRESH_TOKEN", "")
LOGIN_CUSTOMER_ID = os.getenv("GOOGLE_ADS_LOGIN_CUSTOMER_ID", "")  # manager id, no dashes

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
        # login_customer_id optional; can also be provided per-call header
    }
    if login_cid or LOGIN_CUSTOMER_ID:
        cfg["login_customer_id"] = (login_cid or LOGIN_CUSTOMER_ID).replace("-", "")
    return GoogleAdsClient.load_from_dict(cfg, version="v18")  # adjust if you pin versions

# ---------- FastAPI base ----------
app = FastAPI()

# CORS (dev-open now; lock down later)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET","POST","OPTIONS"],
    allow_headers=["*"],
    expose_headers=["MCP-Protocol-Version","Mcp-Session-Id"],
)

class MCPProtocolHeader(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        proto = request.headers.get("Mcp-Protocol-Version") or MCP_PROTO_DEFAULT
        response = await call_next(request)
        response.headers["MCP-Protocol-Version"] = proto
        return response

app.add_middleware(MCPProtocolHeader)

# ---------- MCP tooling ----------
def mcp_ok_json(title: str, data: Any) -> Dict[str, Any]:
    # Dual payload: JSON (machine) + text (human)
    return {"content": [
        {"type": "json", "json": data},
        {"type": "text", "text": f"{title}\n" + json.dumps(data, indent=2, ensure_ascii=False)}
    ]}

def mcp_err(message: str, data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    out = {"message": message}
    if data is not None:
        out["data"] = data
    return {"code": -32000, "message": message, "data": data or {}}

TOOLS = [
    {
        "name": "fetch_account_tree",
        "description": "List accessible customer hierarchy (manager → clients).",
        "inputSchema": {
            "type":"object",
            "properties":{
                "root_customer_id":{"type":"string","description":"Manager (MCC) customer id, digits only"},
                "depth":{"type":"integer","minimum":1,"maximum":10,"default":2}
            },
            "required":["root_customer_id"]
        }
    },
    {
        "name": "fetch_metrics",
        "description": "Generic metrics for account/campaign/ad_group/ad with optional segments.",
        "inputSchema": {
            "type":"object",
            "properties":{
                "customer_id":{"type":"string"},
                "entity":{"type":"string","enum":["account","campaign","ad_group","ad"],"default":"campaign"},
                "ids":{"type":"array","items":{"type":"string"}},
                "fields":{"type":"array","items":{"type":"string"},
                    "default":["metrics.cost_micros","metrics.clicks","metrics.impressions","metrics.conversions","metrics.conversions_value"]},
                "segments":{"type":"array","items":{"type":"string"},
                    "description":"e.g. date, device, network"},
                "date_preset":{"type":"string","description":"TODAY|YESTERDAY|LAST_7_DAYS|LAST_30_DAYS|THIS_MONTH|LAST_MONTH"},
                "time_range":{"type":"object","properties":{"since":{"type":"string"},"until":{"type":"string"}}},
                "page_size":{"type":"integer","minimum":1,"maximum":10000,"default":1000},
                "page_token":{"type":["string","null"]},
                "login_customer_id":{"type":"string","description":"Manager id for header (optional)"}
            },
            "required":["customer_id"]
        }
    },
    {
        "name": "fetch_campaign_summary",
        "description": "Per-campaign KPIs with computed ctr/cpc/cpa/roas.",
        "inputSchema": {
            "type":"object",
            "properties":{
                "customer_id":{"type":"string"},
                "date_preset":{"type":"string"},
                "time_range":{"type":"object","properties":{"since":{"type":"string"},"until":{"type":"string"}}},
                "login_customer_id":{"type":"string"}
            },
            "required":["customer_id"]
        }
    },
    {
        "name": "list_recommendations",
        "description": "Google Ads Recommendations for a customer.",
        "inputSchema": {
            "type":"object",
            "properties":{
                "customer_id":{"type":"string"},
                "types":{"type":"array","items":{"type":"string"}},
                "login_customer_id":{"type":"string"}
            },
            "required":["customer_id"]
        }
    },
    {
        "name": "fetch_search_terms",
        "description": "Search terms with basic filters.",
        "inputSchema": {
            "type":"object",
            "properties":{
                "customer_id":{"type":"string"},
                "date_preset":{"type":"string"},
                "time_range":{"type":"object","properties":{"since":{"type":"string"},"until":{"type":"string"}}},
                "min_clicks":{"type":"integer","default":0},
                "min_cost_micros":{"type":"integer","default":0},
                "campaign_ids":{"type":"array","items":{"type":"string"}},
                "ad_group_ids":{"type":"array","items":{"type":"string"}},
                "login_customer_id":{"type":"string"}
            },
            "required":["customer_id"]
        }
    },
    {
        "name": "fetch_change_history",
        "description": "Change events within a date range.",
        "inputSchema": {
            "type":"object",
            "properties":{
                "customer_id":{"type":"string"},
                "time_range":{"type":"object","properties":{"since":{"type":"string"},"until":{"type":"string"}}},
                "resource_types":{"type":"array","items":{"type":"string"}},
                "login_customer_id":{"type":"string"}
            },
            "required":["customer_id","time_range"]
        }
    },
    {
        "name": "fetch_budget_pacing",
        "description": "Month-to-date spend and projected end-of-month vs target.",
        "inputSchema": {
            "type":"object",
            "properties":{
                "customer_id":{"type":"string"},
                "month":{"type":"string","description":"YYYY-MM"},
                "target_spend":{"type":"number","description":"Target for the month in account currency"},
                "login_customer_id":{"type":"string"}
            },
            "required":["customer_id","month","target_spend"]
        }
    }
]

# ---------- Helpers ----------
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

def _authz_check(request: Request) -> Optional[JSONResponse]:
    if MCP_SHARED_KEY:
        key = request.headers.get("X-MCP-Key") or ""
        if key != MCP_SHARED_KEY:
            return JSONResponse({"jsonrpc":"2.0","id":None,"error":{"code":-32001,"message":"Unauthorized"}}, status_code=200)
    return None

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
                "errors": [{"message": err.message, "code": err.error_code.__class__.__name__} for err in (getattr(e, "failure", None).errors or [])] if getattr(e, "failure", None) else []
            }
            raise RuntimeError(json.dumps(details))

def _gaql_where_for_time(tnorm: Dict[str,str]) -> str:
    if "preset" in tnorm:
        # Google Ads has date presets in the API, but GAQL generally expects explicit dates.
        # We'll translate common presets to [start, end] UTC calendar dates.
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

# ---------- Tool implementations ----------
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
    rows = _ads_call(lambda: svc.search(customer_id=root, query=q, page_size=1000))
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
    customer_id = args["customer_id"].replace("-", "")
    entity = args.get("entity","campaign")
    if entity not in ENTITY_FROM: raise ValueError("Invalid entity")
    fields = args.get("fields") or ["metrics.cost_micros","metrics.clicks","metrics.impressions","metrics.conversions","metrics.conversions_value"]
    ids = [i.replace("-","") for i in (args.get("ids") or [])]
    segs_in = args.get("segments") or []
    segs = [SEGMENT_MAP.get(s,s) for s in segs_in]
    tnorm = _normalize_time(args)
    where_time = _gaql_where_for_time(tnorm)
    login = (args.get("login_customer_id") or LOGIN_CUSTOMER_ID or "").replace("-","") or None

    select_cols = []
    if entity == "account":
        select_cols += ["customer.id","customer.descriptive_name"]
    elif entity == "campaign":
        select_cols += ["campaign.id","campaign.name","campaign.status"]
    elif entity == "ad_group":
        select_cols += ["ad_group.id","ad_group.name","ad_group.status","campaign.id","campaign.name"]
    else:
        select_cols += ["ad_group_ad.ad.id","ad_group_ad.ad.name","ad_group.id","ad_group.name","campaign.id","campaign.name"]

    select_cols += fields
    select_cols += segs

    frm = ENTITY_FROM[entity]
    where_ids = ""
    if ids:
        col = {"account":"customer.id","campaign":"campaign.id","ad_group":"ad_group.id","ad":"ad_group_ad.ad.id"}[entity]
        ids_csv = ",".join(ids)
        where_ids = f" AND {col} IN ({ids_csv}) "

    q = f"""
    SELECT {", ".join(select_cols)}
    FROM {frm}
    WHERE {where_time} {where_ids}
    """
    client = _new_ads_client(login_cid=login)
    svc = client.get_service("GoogleAdsService")
    page_size = int(args.get("page_size", 1000))
    page_token = args.get("page_token")

    resp = _ads_call(lambda: svc.search(customer_id=customer_id, query=q, page_size=page_size, page_token=page_token))
    out_rows = []
    next_token = getattr(resp, "next_page_token", None)
    for r in resp:
        row = r._pb  # protobuf; we’ll pull the known fields safely below
        # Extract metrics & ids defensively
        obj = {}
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
            # segments requested
            if segs:
                if "segments.date" in segs:
                    obj["date"] = str(r.segments.date)
                if "segments.device" in segs:
                    obj["device"] = r.segments.device.name
                if "segments.ad_network_type" in segs:
                    obj["network"] = r.segments.ad_network_type.name
        except Exception:
            pass
        out_rows.append(obj)

    return {"query": q, "count": len(out_rows), "next_page_token": next_token or "", "rows": out_rows}

def tool_fetch_campaign_summary(args: Dict[str, Any]) -> Dict[str, Any]:
    customer_id = args["customer_id"].replace("-", "")
    login = (args.get("login_customer_id") or LOGIN_CUSTOMER_ID or "").replace("-","") or None
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
    rows = _ads_call(lambda: svc.search(customer_id=customer_id, query=q, page_size=5000))
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
    customer_id = args["customer_id"].replace("-", "")
    login = (args.get("login_customer_id") or LOGIN_CUSTOMER_ID or "").replace("-","") or None
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
    rows = _ads_call(lambda: svc.search(customer_id=customer_id, query=q, page_size=1000))
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
    customer_id = args["customer_id"].replace("-", "")
    login = (args.get("login_customer_id") or LOGIN_CUSTOMER_ID or "").replace("-","") or None
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
    rows = _ads_call(lambda: svc.search(customer_id=customer_id, query=q, page_size=5000))
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
    customer_id = args["customer_id"].replace("-", "")
    login = (args.get("login_customer_id") or LOGIN_CUSTOMER_ID or "").replace("-","") or None
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
    rows = _ads_call(lambda: svc.search(customer_id=customer_id, query=q, page_size=1000))
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
    customer_id = args["customer_id"].replace("-", "")
    login = (args.get("login_customer_id") or LOGIN_CUSTOMER_ID or "").replace("-","") or None
    month = args["month"]  # YYYY-MM
    target = float(args["target_spend"])
    year, mon = map(int, month.split("-"))
    start = datetime.date(year, mon, 1)
    # end date = today if same month; else last day of month
    today = datetime.date.today()
    if today.year == year and today.month == mon:
        end = today
        days_elapsed = (end - start).days + 1
        # days in month
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
    rows = _ads_call(lambda: svc.search(customer_id=customer_id, query=q, page_size=1000))
    mtd_cost = 0
    for r in rows:
        mtd_cost += _money(r.metrics.cost_micros)
    # naive linear projection
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

# ---------- Health & discovery ----------
@app.get("/", include_in_schema=False)
@app.head("/", include_in_schema=False)
async def root_get(request: Request):
    if request.method == "HEAD":
        return PlainTextResponse("")
    return PlainTextResponse("ok")

@app.get("/.well-known/mcp.json")
def mcp_discovery():
    return JSONResponse({
        "mcpVersion": MCP_PROTO_DEFAULT,
        "name": APP_NAME,
        "version": APP_VER,
        "auth": {"type": "none"},
        "capabilities": {"tools": {"listChanged": True}},
        "endpoints": {"rpc": "/"},
        "tools": TOOLS
    })

@app.get("/mcp/tools")
def mcp_tools():
    return JSONResponse({"tools": TOOLS})

# ---------- JSON-RPC ----------
@app.post("/")
async def rpc(request: Request):
    maybe = _authz_check(request)
    if maybe: return maybe

    try:
        payload = await request.json()
        method  = (payload.get("method") or "").lower()
        _id     = payload.get("id")

        if method == "initialize":
            client_proto = (payload.get("params") or {}).get("protocolVersion") or MCP_PROTO_DEFAULT
            result = {"protocolVersion": client_proto,
                      "capabilities": {"tools": {"listChanged": True}},
                      "serverInfo": {"name": APP_NAME, "version": APP_VER},
                      "tools": TOOLS}
            return JSONResponse({"jsonrpc":"2.0","id":_id,"result":result},
                                headers={"MCP-Protocol-Version": client_proto})

        if method in ("initialized","notifications/initialized"):
            return JSONResponse({"jsonrpc":"2.0","id":_id,"result":{"ok": True}})

        if method in ("tools/list","tools.list","list_tools","tools.index"):
            return JSONResponse({"jsonrpc":"2.0","id":_id,"result":{"tools": TOOLS}})

        if method == "tools/call":
            params = payload.get("params") or {}
            name   = params.get("name")
            args   = params.get("arguments") or {}

            try:
                if name == "fetch_account_tree":
                    data = tool_fetch_account_tree(args)
                    return JSONResponse({"jsonrpc":"2.0","id":_id,"result": mcp_ok_json("Account tree", data)})
                if name == "fetch_metrics":
                    data = tool_fetch_metrics(args)
                    return JSONResponse({"jsonrpc":"2.0","id":_id,"result": mcp_ok_json("Metrics", data)})
                if name == "fetch_campaign_summary":
                    data = tool_fetch_campaign_summary(args)
                    return JSONResponse({"jsonrpc":"2.0","id":_id,"result": mcp_ok_json("Campaign summary", data)})
                if name == "list_recommendations":
                    data = tool_list_recommendations(args)
                    return JSONResponse({"jsonrpc":"2.0","id":_id,"result": mcp_ok_json("Recommendations", data)})
                if name == "fetch_search_terms":
                    data = tool_fetch_search_terms(args)
                    return JSONResponse({"jsonrpc":"2.0","id":_id,"result": mcp_ok_json("Search terms", data)})
                if name == "fetch_change_history":
                    data = tool_fetch_change_history(args)
                    return JSONResponse({"jsonrpc":"2.0","id":_id,"result": mcp_ok_json("Change history", data)})
                if name == "fetch_budget_pacing":
                    data = tool_fetch_budget_pacing(args)
                    return JSONResponse({"jsonrpc":"2.0","id":_id,"result": mcp_ok_json("Budget pacing", data)})

                return JSONResponse({"jsonrpc":"2.0","id":_id,
                    "error":{"code":-32601,"message":f"Unknown tool: {name}"}})
            except ValueError as ve:
                return JSONResponse({"jsonrpc":"2.0","id":_id,
                    "error":{"code":-32602,"message":f"Invalid params: {ve}"}})
            except Exception as e:
                log.exception("Tool call failed")
                # Bubble a uniform error envelope
                try:
                    data = json.loads(str(e))
                except Exception:
                    data = {"detail": str(e)}
                return JSONResponse({"jsonrpc":"2.0","id":_id,
                    "error": mcp_err("Google Ads API error", data)})

        return JSONResponse({"jsonrpc":"2.0","id":_id,
                             "error":{"code":-32601,"message":f"Method not found: {method}"}})
    except Exception as e:
        log.exception("RPC dispatch error")
        return JSONResponse({"jsonrpc":"2.0","id":None,
                             "error":{"code":-32098,"message":f"RPC dispatch error: {e}"}})

# ---------- Local dev ----------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=int(os.getenv("PORT","8080")))
