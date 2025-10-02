# app.py
from __future__ import annotations

import json
import os
import datetime
from typing import Any, Dict, Optional, List, Tuple

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse

# --- Google Ads SDK ---
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

APP_NAME = "mcp-google-ads"
APP_VER = "0.1.0"
MCP_PROTOCOL = "2024-11-05"

# ---------------- Env & Google Ads client helpers ----------------
DEV_TOKEN         = os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN", "")
CLIENT_ID         = os.getenv("GOOGLE_ADS_CLIENT_ID", "")
CLIENT_SECRET     = os.getenv("GOOGLE_ADS_CLIENT_SECRET", "")
REFRESH_TOKEN     = os.getenv("GOOGLE_ADS_REFRESH_TOKEN", "")
LOGIN_CUSTOMER_ID = (os.getenv("GOOGLE_ADS_LOGIN_CUSTOMER_ID", "") or "").replace("-", "").strip()

def _require_env():
    missing = [k for k, v in [
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
    final_login = (login_cid or LOGIN_CUSTOMER_ID or "").replace("-", "").strip()
    if final_login:
        cfg["login_customer_id"] = final_login
    return GoogleAdsClient.load_from_dict(cfg)

# ---------------- Minimal FastAPI app ----------------
app = FastAPI()

@app.get("/", include_in_schema=False)
async def root():
    return PlainTextResponse("ok")

@app.get("/.well-known/mcp.json")
def discovery():
    # Simple, no-auth discovery
    return JSONResponse({
        "mcpVersion": MCP_PROTOCOL,
        "name": APP_NAME,
        "version": APP_VER,
        "auth": {"type": "none"},
        "capabilities": {"tools": {"listChanged": True}},
        "endpoints": {"rpc": "/"},
        "tools": TOOLS,  # defined below
    })

# ---------------- MCP helpers (text-only content) ----------------
def mcp_ok_text(data: Any) -> Dict[str, Any]:
    # Always return a single text item (stringify dicts)
    if not isinstance(data, str):
        try:
            data = json.dumps(data, ensure_ascii=False)
        except Exception:
            data = str(data)
    return {"content": [{"type": "text", "text": data}]}

def mcp_err(message: str, data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return {"code": -32000, "message": message, "data": data or {}}

# ---------------- Tools ----------------
TOOLS: List[Dict[str, Any]] = [
    {
        "name": "ping",
        "description": "Health check.",
        "inputSchema": {"type": "object", "additionalProperties": False, "properties": {}},
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
                    "description": "Optional MCC header override",
                    "maxLength": 20,
                    "pattern": "^[0-9-]*$"
                }
            }
        }
    },
]

# ---- Implementations ----
def tool_ping(_args: Dict[str, Any]) -> Dict[str, Any]:
    return {"ok": True, "time": datetime.datetime.utcnow().isoformat() + "Z"}

def tool_list_resources(args: Dict[str, Any]) -> Dict[str, Any]:
    login = (args.get("login_customer_id") or LOGIN_CUSTOMER_ID or "").replace("-", "") or None
    try:
        client = _new_ads_client(login_cid=login)
        svc = client.get_service("CustomerService")
        resp = svc.list_accessible_customers()
        customers: List[Dict[str, str]] = []
        for resource_name in resp.resource_names:
            customer_id = resource_name.split("/")[-1]
            customers.append({"resource_name": resource_name, "customer_id": customer_id})
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

TOOL_IMPLS: Dict[str, Any] = {
    "ping":           (tool_ping, "pong"),
    "list_resources": (tool_list_resources, "Resources"),
}

# ---------------- Minimal JSON-RPC ----------------
@app.post("/")
async def rpc(request: Request):
    try:
        payload = await request.json()
    except Exception:
        return JSONResponse({"jsonrpc": "2.0", "id": None,
                             "error": {"code": -32700, "message": "Parse error"}})

    # Batch
    if isinstance(payload, list):
        results = []
        for obj in payload:
            res = await _handle_one(obj)
            if res is not None:
                results.append(res)
        return JSONResponse(results if results else [], status_code=200)

    # Single
    res = await _handle_one(payload)
    return JSONResponse(res if res is not None else {}, status_code=200)

async def _handle_one(obj: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(obj, dict):
        return {"jsonrpc": "2.0", "id": None, "error": {"code": -32600, "message": "Invalid Request"}}

    _id = obj.get("id")
    method = (obj.get("method") or "").lower()
    params = obj.get("params") or {}

    def ok(result: Dict[str, Any]) -> Dict[str, Any]:
        return {"jsonrpc": "2.0", "id": _id, "result": result}

    def err(code: int, message: str, data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        out = {"jsonrpc": "2.0", "id": _id, "error": {"code": code, "message": message}}
        if data is not None:
            out["error"]["data"] = data
        return out

    # initialize
    if method == "initialize":
        result = {
            "protocolVersion": MCP_PROTOCOL,
            "capabilities": {"tools": {"listChanged": True}},
            "serverInfo": {"name": APP_NAME, "version": APP_VER},
            "tools": TOOLS,
        }
        return ok(result)

    # initialized ack
    if method in ("initialized", "notifications/initialized"):
        return ok({"ok": True})

    # tools/list
    if method in ("tools/list", "tools.list", "list_tools", "tools.index"):
        return ok({"tools": TOOLS})

    # tools/call
    if method == "tools/call":
        name = params.get("name")
        args = params.get("arguments") or {}
        impl = TOOL_IMPLS.get(name)
        if not impl:
            return err(-32601, f"Unknown tool: {name}")
        func, title = impl
        try:
            data = func(args)
            return ok(mcp_ok_text({"title": title, "data": data}))
        except Exception as e:
            return {"jsonrpc": "2.0", "id": _id, "error": mcp_err("Tool error", {"detail": str(e)})}

    # unknown method
    return err(-32601, f"Method not found: {method}")

# ---------------- Local dev ----------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
