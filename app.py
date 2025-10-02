import os
import json
import logging
from typing import Any, Dict, Optional, List, Set

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse, Response
from fastapi.middleware.cors import CORSMiddleware

# Google Ads (only needed for list_resources)
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

APP_NAME = "mcp-google-ads"
APP_VER  = "0.1.0"
MCP_PROTOCOL = "2024-11-05"

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(APP_NAME)

# Optional shared secret (to protect non-public tools). Leave empty to disable.
MCP_SHARED_KEY = os.getenv("MCP_SHARED_KEY", "").strip()
log.info("MCP_SHARED_KEY enabled? %s", "YES" if MCP_SHARED_KEY else "NO")

# ---- Minimal tool registry ----
TOOLS: List[Dict[str, Any]] = [
    {
        "name": "ping",
        "description": "Health check.",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False}
    },
    {
        "name": "noop_ok",
        "description": "Returns a tiny JSON object.",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False}
    },
    {
        "name": "list_resources",
        "description": "List accessible Google Ads customer accounts.",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False}
    },
]

# Public tools that do NOT require MCP_SHARED_KEY when set
PUBLIC_TOOLS: Set[str] = {"ping", "noop_ok"}

# ---- FastAPI (simple, permissive CORS like your Meta MCP) ----
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=["MCP-Protocol-Version"],
)

# ---- Helpers to format MCP replies (1 content item only) ----
def mcp_ok_json(data: Any) -> Dict[str, Any]:
    return {"content": [{"type": "json", "json": data}]}

def mcp_err(message: str, data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return {"code": -32000, "message": message, "data": data or {}}

def _jsonrpc_ok(_id: Any, result: Dict[str, Any]) -> Dict[str, Any]:
    return {"jsonrpc": "2.0", "id": _id, "result": result}

def _jsonrpc_err(_id: Any, code: int, message: str, data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    body: Dict[str, Any] = {"jsonrpc": "2.0", "id": _id, "error": {"code": code, "message": message}}
    if data is not None:
        body["error"]["data"] = data
    return body

# ---- Optional auth check (Meta-style, simple) ----
def _is_authorized(request: Request) -> bool:
    if not MCP_SHARED_KEY:
        return True
    # Accept either Authorization: Bearer <key> OR X-MCP-Key: <key>
    auth_hdr = request.headers.get("Authorization", "")
    bearer_ok = auth_hdr.lower().startswith("bearer ") and auth_hdr.split(" ", 1)[1].strip() == MCP_SHARED_KEY
    xhdr_ok = request.headers.get("X-MCP-Key", "") == MCP_SHARED_KEY
    return bearer_ok or xhdr_ok

# ---- Google Ads client factory (simple) ----
def _ads_client() -> GoogleAdsClient:
    # Minimal env: developer token, OAuth client, refresh token
    cfg = {
        "developer_token": os.environ.get("GOOGLE_ADS_DEVELOPER_TOKEN", ""),
        "client_id": os.environ.get("GOOGLE_ADS_CLIENT_ID", ""),
        "client_secret": os.environ.get("GOOGLE_ADS_CLIENT_SECRET", ""),
        "refresh_token": os.environ.get("GOOGLE_ADS_REFRESH_TOKEN", ""),
        "use_proto_plus": True,
    }
    login_cid = (os.environ.get("GOOGLE_ADS_LOGIN_CUSTOMER_ID", "") or "").replace("-", "").strip()
    if login_cid:
        cfg["login_customer_id"] = login_cid

    missing = [k for k, v in cfg.items() if k != "use_proto_plus" and not v]
    if missing:
        raise RuntimeError(f"Missing required Google Ads env: {', '.join(missing)}")

    return GoogleAdsClient.load_from_dict(cfg)

# ---- Tool implementations (very small) ----
def tool_ping(_args: Dict[str, Any]) -> Dict[str, Any]:
    return {"ok": True}

def tool_noop_ok(_args: Dict[str, Any]) -> Dict[str, Any]:
    return {"ok": True}

def tool_list_resources(_args: Dict[str, Any]) -> Dict[str, Any]:
    client = _ads_client()
    svc = client.get_service("CustomerService")
    try:
        resp = svc.list_accessible_customers()
        customers = [{"resource_name": rn, "customer_id": rn.split("/")[-1]} for rn in resp.resource_names]
        return {"count": len(customers), "customers": customers}
    except GoogleAdsException as e:
        # Return a compact JSON-RPC error; do not raise HTTP errors
        detail = {
            "status": e.error.code().name if hasattr(e, "error") else "UNKNOWN",
            "request_id": getattr(e, "request_id", None),
            "errors": [{"message": ee.message} for ee in getattr(getattr(e, "failure", None), "errors", [])] if getattr(e, "failure", None) else [],
        }
        raise RuntimeError(json.dumps(detail))

# ---- Health & discovery (Meta-simple) ----
@app.get("/", include_in_schema=False)
@app.head("/", include_in_schema=False)
def root_get(request: Request):
    if request.method == "HEAD":
        return PlainTextResponse("")
    return JSONResponse({"ok": True, "message": "MCP server. POST / for JSON-RPC; see /.well-known/mcp.json"})

@app.get("/.well-known/mcp.json")
def mcp_discovery(request: Request):
    # Don’t hide tools here; keep it super simple like the Meta app
    auth = {"type": "none"} if not MCP_SHARED_KEY else {
        "type": "shared-secret",
        "tokenHeader": "Authorization",
        "scheme": "Bearer",
        "altHeaders": ["X-MCP-Key"],
    }
    return JSONResponse({
        "mcpVersion": MCP_PROTOCOL,
        "name": APP_NAME,
        "version": APP_VER,
        "auth": auth,
        "capabilities": {"tools": {"listChanged": True}},
        "endpoints": {"rpc": "/"},
        "tools": TOOLS,
    })

@app.get("/mcp/tools")
def mcp_tools():
    # Simple mirror endpoint (optional)
    return JSONResponse({"tools": TOOLS})

# ---- JSON-RPC core (simple, Meta-like) ----
@app.post("/")
async def rpc(request: Request):
    try:
        payload = await request.json()
    except Exception:
        return JSONResponse(_jsonrpc_err(None, -32700, "Parse error"), headers={"MCP-Protocol-Version": MCP_PROTOCOL})

    # Batch: respond to each; single: just handle one
    if isinstance(payload, list):
        out: List[Dict[str, Any]] = []
        for obj in payload:
            resp = await _handle_one(obj, request)
            if resp is not None:
                out.append(resp)
        return JSONResponse(out or [], headers={"MCP-Protocol-Version": MCP_PROTOCOL})

    resp = await _handle_one(payload, request)
    return JSONResponse(resp or {}, headers={"MCP-Protocol-Version": MCP_PROTOCOL})

async def _handle_one(obj: Any, request: Request) -> Optional[Dict[str, Any]]:
    if not isinstance(obj, dict):
        return _jsonrpc_err(None, -32600, "Invalid Request")

    _id = obj.get("id")
    method = (obj.get("method") or "").lower()
    params = obj.get("params") or {}
    name = params.get("name")
    args = params.get("arguments") or {}

    # initialize
    if method == "initialize":
        result = {
            "protocolVersion": MCP_PROTOCOL,
            "capabilities": {"tools": {"listChanged": True}},
            "serverInfo": {"name": APP_NAME, "version": APP_VER},
            "tools": TOOLS,
        }
        return _jsonrpc_ok(_id, result)

    # initialized (ack)
    if method in ("initialized", "notifications/initialized"):
        return _jsonrpc_ok(_id, {"ok": True})

    # tools/list
    if method in ("tools/list", "tools.list", "list_tools", "tools.index"):
        return _jsonrpc_ok(_id, {"tools": TOOLS})

    # tools/call
    if method == "tools/call":
        tool = (name or "").lower()

        # Gate non-public tools if MCP_SHARED_KEY is set
        if MCP_SHARED_KEY and tool not in PUBLIC_TOOLS and not _is_authorized(request):
            log.warning("UNAUTH tools/call tool=%s ua=%s", tool, request.headers.get("user-agent",""))
            # IMPORTANT: still HTTP 200 — JSON-RPC error object
            return _jsonrpc_err(_id, -32001, "Unauthorized")

        HANDLERS = {
            "ping": tool_ping,
            "noop_ok": tool_noop_ok,
            "list_resources": tool_list_resources,
        }

        fn = HANDLERS.get(tool)
        if not fn:
            return _jsonrpc_err(_id, -32601, f"Unknown tool: {name}")

        try:
            data = fn(args)
            return _jsonrpc_ok(_id, mcp_ok_json(data))
        except ValueError as ve:
            return _jsonrpc_err(_id, -32602, f"Invalid params: {ve}")
        except Exception as e:
            # Try to unpack structured errors emitted as JSON strings
            try:
                err_data = json.loads(str(e))
            except Exception:
                err_data = {"detail": str(e)}
            return {"jsonrpc": "2.0", "id": _id, "error": mcp_err("Tool failed", err_data)}

    # Fallback
    return _jsonrpc_err(_id, -32601, f"Method not found: {method}")

# ---- Local dev ----
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
