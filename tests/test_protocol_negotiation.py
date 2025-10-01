import json
from types import SimpleNamespace
from app import SUPPORTED_MCP_VERSIONS, _handle_single_rpc, _latest_supported_protocol, PUBLIC_TOOLS

class DummyRequest:
    def __init__(self, headers=None):
        self.headers = headers or {}
        self.state = SimpleNamespace()

def _initialize(payload):
    request = DummyRequest()
    headers = {}
    status = {"code": 200}
    response = _handle_single_rpc(payload, request, headers, status, PUBLIC_TOOLS)
    return response, headers, request

def test_initialize_downgrades_to_supported_version():
    response, headers, request = _initialize({
        "jsonrpc": "2.0",
        "id": "init",
        "method": "initialize",
        "params": {"protocolVersion": "2025-06-18"},
    })
    negotiated = _latest_supported_protocol()
    assert response["result"]["protocolVersion"] == negotiated
    assert headers["MCP-Protocol-Version"] == negotiated
    assert request.state.mcp_protocol_version == negotiated

def test_initialize_rejects_older_than_supported():
    response, headers, request = _initialize({
        "jsonrpc": "2.0",
        "id": "init-old",
        "method": "initialize",
        "params": {"protocolVersion": "2023-01-01"},
    })
    assert response["error"]["code"] == -32602
    assert response["error"]["message"] == "Unsupported protocolVersion"
    assert response["error"]["data"]["supportedVersions"] == SUPPORTED_MCP_VERSIONS
    assert headers["MCP-Protocol-Version"] == _latest_supported_protocol()
    assert request.state.mcp_protocol_version == _latest_supported_protocol()

def test_initialize_rejects_bad_format():
    response, headers, _ = _initialize({
        "jsonrpc": "2.0",
        "id": "init-bad",
        "method": "initialize",
        "params": {"protocolVersion": "not-a-date"},
    })
    assert response["error"]["code"] == -32602
    assert response["error"]["message"] == "Invalid protocolVersion format"
    assert "supportedVersions" in response["error"]["data"]
