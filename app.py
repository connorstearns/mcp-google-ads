# app.py
from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse, Response

# Google Ads
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException


# -------------------- App & MCP basics --------------------
APP_NAME = "mcp-google-ads"
APP_VER = "0.2.0"
MCP_PROTO_DEFAULT = "2024-11-05"

app = FastAPI()


# -------------------- Env & Ads client --------------------
DEV_TOKEN = os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN", "")
CLIENT_ID = os.getenv("GOOGLE_ADS_CLIENT_ID", "")
CLIENT_SECRET = os.getenv("GOOGLE_ADS_CLIENT_SECRET", "")
REFRESH_TOKEN = os.getenv("GOOGLE_ADS_REFRESH_TOKEN", "")
LOGIN_CUSTOMER_ID = (os.getenv("GOOGLE_ADS_LOGIN_CUSTOMER_ID", "") or "").replace("-", "").strip()


def _require_env() -> None:
    missing = [k for k, v in [
        ("GOOGLE_ADS_DEVELOPER_TOKEN", DEV_TOKEN),
        ("GOOGLE_ADS_CLIENT_ID", CLIENT_ID),
        ("GOOGLE_ADS_CLIENT_SECRET", CLIENT_SECRET),
        ("GOOGLE_ADS_REFRESH_TOKEN", REFRESH_TOKEN),
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
    final_login = (login_cid or LOGIN_CUSTOMER_ID or "").replace("-", "").strip()
    if final_login:
        cfg["login_customer_id"] = final_login
    return GoogleAdsClient.load_from_dict(cfg)


def _money(micros: int | None) -> float:
    return round((micros or 0) / 1_000_000, 6)


# -------------------- Minimal tools --------------------
def tool_ping(_args: Dict[str, Any]) -> Dict[str, Any]:
    return {"ok": True}


def tool_list_resources(_args: Dict[str, Any]) -> Dict[str, Any]:
    try:
        client = _new_ads_client()
        svc = client.get_service("CustomerService")
        resp = svc.list_accessible_customers()
        customers: List[Dict[str, str]] = []
        for rn in resp.resource_names:
            customers.append({"resource_name": rn, "customer_id": rn.split("/")[-1]})
        return {"count": len(customers), "customers": customers}
    except GoogleAdsException as e:
        status = e.error.code().name if hasattr(e, "error") else "UNKNOWN"
        rid = getattr(e, "request_id", None)
        details = {"status": status, "request_id": rid}
        try:
            if getattr(e, "failure", None) and e.failure.errors:
                details["errors"] = [{"message": er.message} for er in e.failure.errors]
        except Exception:
            pass
        return {"error": details}
    except Exception as e:
        return {"error": {"detail": str(e)}}


def tool_fetch_campaign_summary(args: Dict[str, Any]) -> Dict[str, Any]:
    """
    Inputs (all optional, but customer_id strongly recommended):
      customer_id: "1234567890"
      login_customer_id: MCC header override
      date_preset: TODAY|YESTERDAY|LAST_7_DAYS|LAST_30_DAYS|THIS_MONTH|LAST_MONTH
      time_range: {"since":"YYYY-MM-DD","until":"YYYY-MM-DD"}  # overrides date_preset
    """
    login = (args.get("login_customer_id") or LOGIN_CUSTOMER_ID or "").replace("-", "") or None
    customer_id = (args.get("customer_id") or "").replace("-", "") or ""
    if not customer_id:
        return {"error": {"detail": "customer_id required"}}

    date_preset = (args.get("date_preset") or "").upper().strip()
    tr = args.get("time_range") or {}
    if tr.get("since") and tr.get("until"):
        where_time = f" segments.date BETWEEN '{tr['since']}' AND '{tr['until']}' "
    elif date_preset in {"TODAY", "YESTERDAY", "LAST_7_DAYS", "LAST_30_DAYS", "THIS_MONTH", "LAST_MONTH"}:
        where_time = f" segments.date DURING {date_preset} "
    else:
        where_time = " segments.date DURING LAST_30_DAYS "

    q = f"""
    SELECT
      campaign.id, campaign.name, campaign.status,
      metrics.impressions, metrics.clicks, metrics.cost_micros,
      metrics.conversions, metrics.conversions_value
    FROM campaign
    WHERE {where_time}
    """

    try:
        client = _new_ads_client(login_cid=login)
        svc = client.get_service("GoogleAdsService")
        resp = svc.search(request={"customer_id": customer_id, "query": q})

        out: List[Dict[str, Any]] = []
        for r in resp:
            cost = _money(getattr(r.metrics, "cost_micros", 0))
            imps = int(getattr(r.metrics, "impressions", 0) or 0)
            clicks = int(getattr(r.metrics, "clicks", 0) or 0)
            conv = float(getattr(r.metrics, "conversions", 0.0) or 0.0)
            conv_val = float(getattr(r.metrics, "conversions_value", 0.0) or 0.0)
            ctr = (clicks / imps * 100) if imps else 0.0
            cpc = (cost / clicks) if clicks else 0.0
            cpa = (cost / conv) if conv else 0.0
            roas = (conv_val / cost) if cost > 0 else 0.0
            out.append({
                "campaign_id": str(r.campaign.id),
                "campaign_name": r.campaign.name,
                "status": r.campaign.status.name,
                "impressions": imps,
                "clicks": clicks,
                "cost": round(cost, 2),
                "conversions": round(conv, 2),
                "conv_value": round(conv_val, 2),
                "ctr_pct": round(ctr, 2),
                "cpc": round(cpc, 2),
                "cpa": round(cpa, 2),
                "roas": round(roas, 2),
            })
        return {"query": q, "rows": out}
    except GoogleAdsException as e:
        status = e.error.code().name if hasattr(e, "error") else "UNKNOWN"
        rid = getattr(e, "request_id", None)
        details = {"status": status, "request_id": rid}
        try:
            if getattr(e, "failure", None) and e.failure.errors:
                details["errors"] = [{"message": er.message} for er in e.failure.errors]
        except Exception:
            pass
        return {"error": details}
    except Exception as e:
        return {"error": {"detail": str(e)}}


ENTITY_FROM = {
    "account": "customer",
    "campaign": "campaign",
    "ad_group": "ad_group",
    "ad": "ad_group_ad",
}


def tool_fetch_metrics(args: Dict[str, Any]) -> Dict[str, Any]:
    """
    Inputs:
      customer_id (required)
      entity: account|campaign|ad_group|ad (default campaign)
      ids: ["123","456"] optional
      fields: GAQL fields list (default common set)
      date_preset OR time_range like above
      login_customer_id optional
    """
    login = (args.get("login_customer_id") or LOGIN_CUSTOMER_ID or "").replace("-", "") or None
    customer_id = (args.get("customer_id") or "").replace("-", "") or ""
    if not customer_id:
        return {"error": {"detail": "customer_id required"}}

    entity = (args.get("entity") or "campaign").lower()
    if entity not in ENTITY_FROM:
        return {"error": {"detail": f"invalid entity '{entity}'"}}

    fields = args.get("fields") or [
        "metrics.cost_micros",
        "metrics.clicks",
        "metrics.impressions",
        "metrics.conversions",
        "metrics.conversions_value",
    ]
    ids = [str(x).replace("-", "") for x in (args.get("ids") or [])]

    date_preset = (args.get("date_preset") or "").upper().strip()
    tr = args.get("time_range") or {}
    if tr.get("since") and tr.get("until"):
        where_time = f" segments.date BETWEEN '{tr['since']}' AND '{tr['until']}' "
    elif date_preset in {"TODAY", "YESTERDAY", "LAST_7_DAYS", "LAST_30_DAYS", "THIS_MONTH", "LAST_MONTH"}:
        where_time = f" segments.date DURING {date_preset} "
    else:
        where_time = " segments.date DURING LAST_30_DAYS "

    id_col = {
        "account": "customer.id",
        "campaign": "campaign.id",
        "ad_group": "ad_group.id",
        "ad": "ad_group_ad.ad.id",
    }[entity]
    id_clause = f" AND {id_col} IN ({','.join(ids)}) " if ids else ""

    base_cols = {
        "account": ["customer.id", "customer.descriptive_name"],
        "campaign": ["campaign.id", "campaign.name", "campaign.status"],
        "ad_group": ["ad_group.id", "ad_group.name", "ad_group.status", "campaign.id", "campaign.name"],
        "ad": ["ad_group_ad.ad.id", "ad_group.id", "ad_group.name", "campaign.id", "campaign.name"],
    }[entity]

    select_cols = base_cols + fields
    frm = ENTITY_FROM[entity]
    q = f"SELECT {', '.join(select_cols)} FROM {frm} WHERE {where_time}{id_clause}"

    try:
        client = _new_ads_client(login_cid=login)
        svc = client.get_service("GoogleAdsService")
        resp = svc.search(request={"customer_id": customer_id, "query": q})

        out: List[Dict[str, Any]] = []
        for r in resp:
            row: Dict[str, Any] = {}
            if entity == "account":
                row["customer_id"] = str(r.customer.id)
                row["customer_name"] = r.customer.descriptive_name
            elif entity == "campaign":
                row["campaign_id"] = str(r.campaign.id)
                row["campaign_name"] = r.campaign.name
                row["campaign_status"] = r.campaign.status.name
            elif entity == "ad_group":
                row["ad_group_id"] = str(r.ad_group.id)
                row["ad_group_name"] = r.ad_group.name
                row["ad_group_status"] = r.ad_group.status.name
                row["campaign_id"] = str(r.campaign.id)
                row["campaign_name"] = r.campaign.name
            else:
                row["ad_id"] = str(r.ad_group_ad.ad.id)
                row["ad_group_id"] = str(r.ad_group.id)
                row["ad_group_name"] = r.ad_group.name
                row["campaign_id"] = str(r.campaign.id)
                row["campaign_name"] = r.campaign.name

            m = r.metrics
            row.update({
                "cost": _money(getattr(m, "cost_micros", 0)),
                "impressions": int(getattr(m, "impressions", 0) or 0),
                "clicks": int(getattr(m, "clicks", 0) or 0),
                "conversions": float(getattr(m, "conversions", 0.0) or 0.0),
                "conversions_value": float(getattr(m, "conversions_value", 0.0) or 0.0),
            })
            out.append(row)

        return {"query": q, "rows": out}
    except GoogleAdsException as e:
        status = e.error.code().name if hasattr(e, "error") else "UNKNOWN"
        rid = getattr(e, "request_id", None)
        details = {"status": status, "request_id": rid}
        try:
            if getattr(e, "failure", None) and e.failure.errors:
                details["errors"] = [{"message": er.message} for er in e.failure.errors]
        except Exception:
            pass
        return {"error": details}
    except Exception as e:
        return {"error": {"detail": str(e)}}


# ---------- TOOLS (schemas updated with min_spend) ----------
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
        "description": "Generic metrics for account/campaign/ad_group/ad with optional segments. Supports min_spend to filter by spend in the date range.",
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
                "min_spend": {
                    "type": "number",
                    "description": "Minimum spend (account currency) required in the selected time range. Rows below this are filtered out.",
                    "minimum": 1,
                    "default": 1.0
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
        "description": "Per-campaign KPIs with computed ctr/cpc/cpa/roas. Supports min_spend to filter by spend in the date range.",
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
                "min_spend": {
                    "type": "number",
                    "description": "Minimum spend (account currency) required in the selected time range. Campaigns below this are filtered out.",
                    "minimum": 1,
                    "default": 1.0
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

# Public/debug tools (unchanged)
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

# Public tools visible without auth (everything else is gated)
PUBLIC_TOOLS: Set[str] = {"ping", "noop_ok"}



# -------------------- Discovery (minimal) --------------------
@app.get("/", include_in_schema=False)
@app.head("/", include_in_schema=False)
def root(request: Request):
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
        "tools": TOOLS,
    })


# -------------------- JSON-RPC (initialize, tools/list, tools/call) --------------------
def _pack_text(data: Any) -> Dict[str, Any]:
    # Text-only response (works with strict MCP clients)
    try:
        text = data if isinstance(data, str) else json.dumps(data, ensure_ascii=False)
    except Exception:
        text = str(data)
    return {"content": [{"type": "text", "text": text}]}


def _call_tool(name: str, args: Dict[str, Any]) -> Dict[str, Any]:
    if name == "ping":
        return _pack_text(tool_ping(args))
    if name == "list_resources":
        return _pack_text(tool_list_resources(args))
    if name == "fetch_campaign_summary":
        return _pack_text(tool_fetch_campaign_summary(args))
    if name == "fetch_metrics":
        return _pack_text(tool_fetch_metrics(args))
    return {"error": {"code": -32601, "message": f"Unknown tool: {name}"}}


@app.post("/")
async def rpc(request: Request):
    try:
        payload = await request.json()
    except Exception:
        return JSONResponse({"jsonrpc": "2.0", "id": None,
                             "error": {"code": -32700, "message": "Parse error"}})

    def handle(obj: Dict[str, Any]) -> Dict[str, Any] | None:
        if not isinstance(obj, dict):
            return {"jsonrpc": "2.0", "id": None, "error": {"code": -32600, "message": "Invalid Request"}}

        _id = obj.get("id")
        method = (obj.get("method") or "").lower()

        if method == "initialize":
            client_proto = (obj.get("params") or {}).get("protocolVersion") or MCP_PROTO_DEFAULT
            result = {
                "protocolVersion": client_proto,
                "capabilities": {"tools": {"listChanged": True}},
                "serverInfo": {"name": APP_NAME, "version": APP_VER},
                "tools": TOOLS,
            }
            return {"jsonrpc": "2.0", "id": _id, "result": result}

        if method in ("initialized", "notifications/initialized"):
            return {"jsonrpc": "2.0", "id": _id, "result": {"ok": True}}

        if method in ("tools/list", "tools.list", "list_tools", "tools.index"):
            return {"jsonrpc": "2.0", "id": _id, "result": {"tools": TOOLS}}

        if method == "tools/call":
            params = obj.get("params") or {}
            name = params.get("name")
            args = params.get("arguments") or {}
            res = _call_tool(name, args)
            if "error" in res and "content" not in res:
                return {"jsonrpc": "2.0", "id": _id, "error": res["error"]}
            return {"jsonrpc": "2.0", "id": _id, "result": res}

        # unknown
        return {"jsonrpc": "2.0", "id": _id, "error": {"code": -32601, "message": f"Method not found: {method}"}}

    # batch support
    if isinstance(payload, list):
        out: List[Dict[str, Any]] = []
        for entry in payload:
            resp = handle(entry)
            if resp is not None:
                out.append(resp)
        return JSONResponse(out if out else [], status_code=200)

    # single
    resp = handle(payload)
    return JSONResponse(resp if resp is not None else {}, status_code=200)


# -------------------- Local dev --------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
