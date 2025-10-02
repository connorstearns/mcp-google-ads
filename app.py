# app.py
from __future__ import annotations

import json
import os
from typing import Any, Dict, Optional

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse, Response

APP_NAME = "mcp-google-ads"
APP_VER  = "0.0.1"
DEFAULT_PROTO = "2024-11-05"

# -----------------------------------------------------------------------------
# Minimal FastAPI app
# -----------------------------------------------------------------------------
app = FastAPI()

# -----------------------------------------------------------------------------
# Helpers — always return *text* content for tools
# -----------------------------------------------------------------------------
def mcp_ok_text(text: str) -> Dict[str, Any]:
    if not isinstance(text, str):
        text = json.dumps(text, ensure_ascii=False)
    return {"content": [{"type": "text", "text": text}]}

def jsonrpc_error(_id: Any, code: int, message: str, data: Optional[Dict[str, Any]] = None):
    err = {"jsonrpc": "2.0", "id": _id, "error": {"code": code, "message": message}}
    if data is not None:
        err["error"]["data"] = data
    return err

# -----------------------------------------------------------------------------
# Trivial tools (no Google Ads calls yet — just prove the shape works)
# -----------------------------------------------------------------------------
def tool_ping(_args: Dict[str, Any]) -> Dict[str, Any]:
    return {"ok": True, "tool": "ping"}

def tool_noop(_args: Dict[str, Any]) -> Dict[str, Any]:
    return {"ok": True}

def tool_echo(args: Dict[str, Any]) -> Dict[str, Any]:
    msg = (args or {}).get("msg", "")
    return {"echo": msg}

TOOLS = [
    {
        "name": "ping",
        "description": "Health check.",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False}
    },
    {
        "name": "noop_ok",
        "description": "Return a tiny OK object.",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False}
    },
    {
        "name": "echo_short",
        "description": "Echo a short string for debug.",
        "inputSchema": {
            "type": "object",
            "properties": {"msg": {"type": "string", "maxLength": 120}},
            "required": ["msg"],
            "additionalProperties": False
        }
    },
]

TOOL_IMPLS = {
    "ping": (tool_ping, "pong"),
    "noop_ok": (tool_noop, "ok"),
    "echo_short": (tool_echo, "echo"),
}

# -----------------------------------------------------------------------------
# Discovery (like your Meta MCP)
# -----------------------------------------------------------------------------
@app.get("/", include_in_schema=False)
@app.head("/", include_in_schema=False)
async def root_get(request: Request):
    if request.method == "HEAD":
        return PlainTextResponse("")
    return JSONResponse({"ok": True, "message": "MCP server. POST / for JSON-RPC; see /.well-known/mcp.json"})

@app.get("/.well-known/mcp.json")
def mcp_discovery():
    return JSONResponse({
        "mcpVersion": DEFAULT_PROTO,       # advertise one version
        "name": APP_NAME,
        "version": APP_VER,
        "auth": {"type": "none"},
        "capabilities": {"tools": {"listChanged": True}},
        "endpoints": {"rpc": "/"},
        "tools": TOOLS,
    })

@app.get("/mcp/tools")
def mcp_tools():
    return JSONResponse({"tools": TOOLS})

# Quiet favicon noise (optional)
@app.get("/favicon.svg", include_in_schema=False)
def favicon_svg(): return Response(status_code=204)

# -----------------------------------------------------------------------------
# JSON-RPC handler (minimal)
# -----------------------------------------------------------------------------
@app.post("/")
async def rpc(request: Request):
    try:
        payload = await request.json()
    except Exception:
        # Invalid JSON → JSON-RPC parse error
        return JSONResponse(jsonrpc_error(None, -32700, "Parse error"))

    # Batch
    if isinstance(payload, list):
        replies = [handle_one(obj) for obj in payload]
        # Notifications return None; per JSON-RPC, ignore them
        replies = [r for r in replies if r is not None]
        # If all were notifications, reply 204
        if not replies:
            return Response(status_code=204)
        return JSONResponse(replies)

    # Single
    reply = handle_one(payload)
    if reply is None:
        # notification → 204
        return Response(status_code=204)
    return JSONResponse(reply)

def handle_one(obj: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(obj, dict):
        return jsonrpc_error(None, -32600, "Invalid Request")

    _id = obj.get("id", None)
    method = obj.get("method", "")
    is_notification = "id" not in obj and obj.get("jsonrpc") == "2.0"

    def ok(result: Dict[str, Any]):
        if is_notification:
            return None
        return {"jsonrpc": "2.0", "id": _id, "result": result}

    # ---------- initialize ----------
    if method == "initialize":
        # Be very permissive: echo their protocol back if present, else use ours.
        client_proto = (obj.get("params") or {}).get("protocolVersion") or DEFAULT_PROTO
        return ok({
            "protocolVersion": client_proto,
            "capabilities": {"tools": {"listChanged": True}},
            "serverInfo": {"name": APP_NAME, "version": APP_VER},
            "tools": TOOLS,
        })

    # ---------- initialized ack ----------
    if method in ("initialized", "notifications/initialized"):
        return ok({"ok": True})

    # ---------- tools/list ----------
    if method in ("tools/list", "tools.list", "list_tools", "tools.index"):
        return ok({"tools": TOOLS})

    # ---------- tools/call ----------
    if method == "tools/call":
        params = obj.get("params") or {}
        name = params.get("name")
        args = params.get("arguments") or {}

        impl = TOOL_IMPLS.get(name)
        if not impl:
            return jsonrpc_error(_id, -32601, f"Unknown tool: {name}")

        func, _title = impl
        try:
            data = func(args)
        except Exception as e:
            # Return a JSON-RPC error — BUT still with HTTP 200 (client expects JSON-RPC envelope)
            return jsonrpc_error(_id, -32000, f"Tool '{name}' failed", {"detail": str(e)})

        # IMPORTANT: Always return text content (stringify the dict)
        return ok(mcp_ok_text(data))

    # ---------- Fallback ----------
    return jsonrpc_error(_id, -32601, f"Method not found: {method}")

# -----------------------------------------------------------------------------
# Local dev
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
