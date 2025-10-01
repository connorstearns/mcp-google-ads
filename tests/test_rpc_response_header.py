import asyncio, json
from starlette.requests import Request
from app import rpc, _latest_supported_protocol

def test_rpc_response_header_matches_negotiated_protocol():
    payload = {
        "jsonrpc": "2.0",
        "id": "init",
        "method": "initialize",
        "params": {"protocolVersion": "2025-06-18"},
    }
    body = json.dumps(payload).encode("utf-8")

    scope = {
        "type": "http",
        "method": "POST",
        "path": "/",
        "headers": [
            (b"content-type", b"application/json"),
            (b"mcp-protocol-version", b"2025-06-18"),
        ],
    }

    async def receive():
        return {"type": "http.request", "body": body, "more_body": False}

    request = Request(scope, receive)
    response = asyncio.run(rpc(request))

    negotiated = _latest_supported_protocol()
    assert response.status_code == 200
    assert response.headers["MCP-Protocol-Version"] == negotiated
    data = json.loads(response.body.decode("utf-8"))
    assert data["result"]["protocolVersion"] == negotiated
