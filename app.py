# app.py
from __future__ import annotations

import datetime
import json
import os
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI, Request
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse

# Google Ads
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException


# -------------------- App & MCP basics --------------------
APP_NAME = "mcp-google-ads"
APP_VER = "0.4.3"
MCP_PROTO_DEFAULT = "2024-11-05"
REGISTRY_PATH = Path(__file__).with_name("google_ads_field_registry.json")

DEFAULT_FETCH_LIMIT = 100
MAX_FETCH_LIMIT = 1000
MAX_FIELDS_PER_FETCH = 25
DEFAULT_VALIDATION_ENTITIES = ["campaign"]
DEFAULT_VALIDATION_PRIORITY = "P0"
DEFAULT_VALIDATION_MAX_FIELDS = 25
MAX_VALIDATION_FIELDS = 50
MAX_VALIDATION_ENTITIES = 4
DEFAULT_LOGIN_CUSTOMER_ID = "9000159936"

STATIC_AVAILABLE_ACCOUNTS = [
    {"account_name": "Lazy Dog Restaurants", "customer_id": "7241931996"},
    {"account_name": "Kaiser SCPMG", "customer_id": "5024696517"},
    {"account_name": "US Storage Centers", "customer_id": "1052724631"},
    {"account_name": "JHT - Remarketing", "customer_id": "6624823090"},
    {"account_name": "First Health", "customer_id": "9685777523"},
    {"account_name": "Hoag - Client Owned Account", "customer_id": "1187938077"},
    {"account_name": "ZO SKIN HEALTH - PORE REFINER", "customer_id": "4240189729"},
    {"account_name": "Rhythm Interactive", "customer_id": "6392899580"},
    {"account_name": "CorVel", "customer_id": "4388437684"},
    {"account_name": "Symbeo", "customer_id": "8614123503"},
    {"account_name": "Zeta Global", "customer_id": "7377022347"},
    {"account_name": "Sunsweet", "customer_id": "8053901639"},
    {"account_name": "CEP", "customer_id": "9698364823"},
    {"account_name": "RiechesBaird", "customer_id": "1585289180"},
    {"account_name": "Wind Water Realty", "customer_id": "4000497459"},
    {"account_name": "Harlem Children's Zone", "customer_id": "7987978735"},
    {"account_name": "The Legal Aid Society", "customer_id": "7170010976"},
    {"account_name": "Meta Family Center", "customer_id": "8919687415"},
    {"account_name": "_DONOTUSE | Champions Ascension", "customer_id": "9749059511"},
    {"account_name": "Nevada Eye Care | NVISION", "customer_id": "1038043584"},
    {"account_name": "Rome", "customer_id": "7127909654"},
    {"account_name": "Littleton Regional Healthcare", "customer_id": "7958732307"},
    {"account_name": "Ireland Earth", "customer_id": "2625880167"},
    {"account_name": "CEP America - Engagement", "customer_id": "4357830149"},
]

app = FastAPI()
app.add_middleware(GZipMiddleware, minimum_size=1000)


def normalize_customer_id(value: Any, field_name: str = "customer_id") -> str:
    """Normalize Google Ads IDs by removing dashes and requiring digits."""
    normalized = str(value or "").replace("-", "").strip()
    if not normalized:
        raise ValueError(f"{field_name} required")
    if not normalized.isdigit():
        raise ValueError(f"{field_name} must be numeric after removing dashes")
    return normalized


# -------------------- Env & Ads client --------------------
DEV_TOKEN = os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN", "")
CLIENT_ID = os.getenv("GOOGLE_ADS_CLIENT_ID", "")
CLIENT_SECRET = os.getenv("GOOGLE_ADS_CLIENT_SECRET", "")
REFRESH_TOKEN = os.getenv("GOOGLE_ADS_REFRESH_TOKEN", "")
try:
    LOGIN_CUSTOMER_ID = normalize_customer_id(os.getenv("GOOGLE_ADS_LOGIN_CUSTOMER_ID", DEFAULT_LOGIN_CUSTOMER_ID), "login_customer_id")
except ValueError:
    LOGIN_CUSTOMER_ID = ""


def _require_env() -> None:
    missing = [k for k, v in [
        ("GOOGLE_ADS_DEVELOPER_TOKEN", DEV_TOKEN),
        ("GOOGLE_ADS_CLIENT_ID", CLIENT_ID),
        ("GOOGLE_ADS_CLIENT_SECRET", CLIENT_SECRET),
        ("GOOGLE_ADS_REFRESH_TOKEN", REFRESH_TOKEN),
    ] if not v]
    if missing:
        raise RuntimeError(f"Missing required env: {', '.join(missing)}")


def _resolve_login_customer_id(args: Dict[str, Any]) -> str:
    raw = args.get("login_customer_id") or LOGIN_CUSTOMER_ID
    return normalize_customer_id(raw, "login_customer_id")


def _resolve_child_customer_id(args: Dict[str, Any]) -> Tuple[str, List[str]]:
    if not args.get("customer_id"):
        raise ValueError("customer_id required: pass the child Google Ads customer_id to query, not the MCC login_customer_id")
    customer_id = normalize_customer_id(args.get("customer_id"), "customer_id")
    login_customer_id = _resolve_login_customer_id(args)
    warnings: List[str] = []
    if customer_id == login_customer_id:
        warnings.append("customer_id equals login_customer_id; this queries the MCC directly and may return no campaign metrics unless intended")
    return customer_id, warnings


def _new_ads_client(login_cid: Optional[str] = None) -> GoogleAdsClient:
    _require_env()
    cfg = {
        "developer_token": DEV_TOKEN,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "refresh_token": REFRESH_TOKEN,
        "use_proto_plus": True,
    }
    final_login = normalize_customer_id(login_cid or LOGIN_CUSTOMER_ID, "login_customer_id")
    cfg["login_customer_id"] = final_login
    return GoogleAdsClient.load_from_dict(cfg)


def _money(micros: int | None) -> float:
    return round((micros or 0) / 1_000_000, 6)


def _where_time(args: Dict[str, Any]) -> str:
    date_preset = (args.get("date_preset") or "").upper().strip()
    tr = args.get("time_range") or {}
    if tr.get("since") and tr.get("until"):
        return f" segments.date BETWEEN '{tr['since']}' AND '{tr['until']}' "
    if date_preset in {"TODAY", "YESTERDAY", "LAST_7_DAYS", "LAST_30_DAYS", "THIS_MONTH", "LAST_MONTH"}:
        return f" segments.date DURING {date_preset} "
    return " segments.date DURING LAST_30_DAYS "


def _err_from_gax(e: GoogleAdsException) -> Dict[str, Any]:
    status = e.error.code().name if hasattr(e, "error") else "UNKNOWN"
    rid = getattr(e, "request_id", None)
    details: Dict[str, Any] = {"status": status, "request_id": rid}
    try:
        if getattr(e, "failure", None) and e.failure.errors:
            details["errors"] = [{"message": er.message} for er in e.failure.errors]
    except Exception:
        pass
    return details


def _google_ads_error_messages(e: GoogleAdsException) -> List[str]:
    try:
        if getattr(e, "failure", None) and e.failure.errors:
            return [er.message for er in e.failure.errors if getattr(er, "message", None)]
    except Exception:
        pass
    return [str(e)]


def _clamped_int(value: Any, default: int, minimum: int, maximum: int) -> int:
    try:
        return max(minimum, min(int(value), maximum))
    except Exception:
        return default


# -------------------- Field registry --------------------
@lru_cache(maxsize=1)
def _load_field_registry() -> Dict[str, Any]:
    with REGISTRY_PATH.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _registry_presets() -> Dict[str, Any]:
    return _load_field_registry().get("presets", {})


def _registry_fields() -> Dict[str, Any]:
    return _load_field_registry().get("fields", {})


def _dedupe(items: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for item in items:
        if item not in seen:
            out.append(item)
            seen.add(item)
    return out


def _resolve_registry_fields(entity: str, requested_fields: Optional[List[str]]) -> Tuple[str, List[str], List[Dict[str, Any]]]:
    presets = _registry_presets()
    fields = _registry_fields()
    if entity not in presets:
        return "", [], [{"error": f"invalid entity '{entity}'. Use one of: {', '.join(sorted(presets))}"}]
    preset = presets[entity]
    from_resource = preset["from"]
    requested = requested_fields or preset.get("default_metrics", [])
    public_names = _dedupe(list(preset.get("base_fields", [])) + list(requested))
    errors: List[Dict[str, Any]] = []
    selected: List[str] = []
    selected_meta: List[Dict[str, Any]] = []
    for public_name in public_names:
        meta = fields.get(public_name)
        if not meta:
            errors.append({"field": public_name, "error": "unknown field", "hint": "Call list_google_ads_fields to see supported public field names."})
            continue
        if from_resource not in meta.get("resources", []):
            errors.append({"field": public_name, "error": f"field is not compatible with entity '{entity}' (FROM {from_resource})", "supported_resources": meta.get("resources", [])})
            continue
        selected.append(meta["google_ads_field"])
        selected_meta.append({"name": public_name, **meta})
    return from_resource, _dedupe(selected), errors if errors else selected_meta


def _get_nested_attr(obj: Any, dotted_path: str) -> Any:
    cur = obj
    for part in dotted_path.split("."):
        if cur is None:
            return None
        cur = getattr(cur, part, None)
    return cur


def _coerce_registry_value(value: Any, transform: str) -> Any:
    if value is None:
        if transform == "int":
            return 0
        if transform in {"float", "micros_to_currency", "percent_ratio"}:
            return 0.0
        return ""
    if hasattr(value, "name"):
        value = value.name
    if transform == "micros_to_currency":
        return _money(int(value or 0))
    if transform == "int":
        return int(value or 0)
    if transform == "float":
        return float(value or 0.0)
    if transform == "percent_ratio":
        return round(float(value or 0.0) * 100, 4)
    return value


def _serialize_registry_row(row: Any, selected_fields: List[Dict[str, Any]]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for field in selected_fields:
        out[field["name"]] = _coerce_registry_value(_get_nested_attr(row, field["google_ads_field"]), field.get("transform", "identity"))
    return out


def _registry_field_is_compatible(public_name: str, from_resource: str) -> bool:
    meta = _registry_fields().get(public_name) or {}
    return from_resource in meta.get("resources", [])


def _field_registry_suggestion(public_name: str, meta: Dict[str, Any], from_resource: str) -> Dict[str, Any]:
    actions = [f"remove '{from_resource}' from resources for '{public_name}' if this failure reproduces"]
    if meta.get("verified"):
        actions.append("set verified=false until this field/resource combination is validated")
    if not meta.get("verification_note"):
        actions.append("add verification_note documenting the live GAQL result")
    return {"field": public_name, "google_ads_field": meta.get("google_ads_field"), "resource": from_resource, "actions": actions}


def _run_validation_query(svc: Any, customer_id: str, query: str) -> None:
    rows = svc.search(request={"customer_id": customer_id, "query": query})
    for _ in rows:
        break


def _base_response_metadata(login_customer_id: str, customer_id: Optional[str] = None, warnings: Optional[List[str]] = None) -> Dict[str, Any]:
    return {"login_customer_id": login_customer_id, "customer_id": customer_id, "warnings": warnings or []}


# -------------------- Minimal tools --------------------
def tool_ping(_args: Dict[str, Any]) -> Dict[str, Any]:
    return {"ok": True}


def tool_debug_login_header(_args: Dict[str, Any]) -> Dict[str, Any]:
    return {"env_LOGIN_CUSTOMER_ID": LOGIN_CUSTOMER_ID}


def tool_echo_short(args: Dict[str, Any]) -> Dict[str, Any]:
    m = (args.get("msg") or "").strip()
    if not m:
        return {"error": {"detail": "msg required"}}
    return {"msg": m}


def tool_noop_ok(_args: Dict[str, Any]) -> Dict[str, Any]:
    return {"ok": True}


def tool_auth_diagnostics(args: Dict[str, Any]) -> Dict[str, Any]:
    login = ""
    try:
        login = _resolve_login_customer_id(args)
    except ValueError as e:
        return {"error": {"detail": str(e)}}
    out: Dict[str, Any] = {
        "env_present": {
            "GOOGLE_ADS_CLIENT_ID": bool(CLIENT_ID),
            "GOOGLE_ADS_CLIENT_SECRET": bool(CLIENT_SECRET),
            "GOOGLE_ADS_REFRESH_TOKEN": bool(REFRESH_TOKEN),
            "GOOGLE_ADS_DEVELOPER_TOKEN": bool(DEV_TOKEN),
        },
        "login_customer_id": login,
        "accessible_customer_ids": [],
    }
    try:
        client = _new_ads_client(login_cid=login)
        svc = client.get_service("CustomerService")
        resp = svc.list_accessible_customers()
        out["accessible_customer_ids"] = [rn.split("/")[-1] for rn in resp.resource_names]
    except Exception as e:
        out["api_error"] = str(e)
    return out


def tool_list_resources(args: Dict[str, Any]) -> Dict[str, Any]:
    try:
        login = _resolve_login_customer_id(args)
        client = _new_ads_client(login_cid=login)
        svc = client.get_service("CustomerService")
        resp = svc.list_accessible_customers()
        customers = [{"resource_name": rn, "customer_id": rn.split("/")[-1]} for rn in resp.resource_names]
        return {"login_customer_id": login, "count": len(customers), "customers": customers}
    except GoogleAdsException as e:
        return {"error": _err_from_gax(e)}
    except Exception as e:
        return {"error": {"detail": str(e)}}


def tool_list_available_accounts(args: Dict[str, Any]) -> Dict[str, Any]:
    try:
        login = _resolve_login_customer_id(args)
    except ValueError as e:
        return {"error": {"detail": str(e)}}
    include_dynamic = bool(args.get("include_dynamic", True))
    dynamic: List[Dict[str, Any]] = []
    if include_dynamic:
        q = """
        SELECT customer_client.client_customer, customer_client.descriptive_name, customer_client.id, customer_client.manager
        FROM customer_client
        WHERE customer_client.manager = false
        """
        try:
            client = _new_ads_client(login_cid=login)
            svc = client.get_service("GoogleAdsService")
            rows = svc.search(request={"customer_id": login, "query": q})
            for r in rows:
                cid = str(getattr(r.customer_client, "id", "") or "")
                dynamic.append({"account_name": r.customer_client.descriptive_name, "customer_id": cid, "resource_name": r.customer_client.client_customer})
        except Exception as e:
            return {"login_customer_id": login, "accounts": STATIC_AVAILABLE_ACCOUNTS, "source": "static_fallback", "dynamic_error": str(e)}
    merged = {a["customer_id"]: dict(a) for a in STATIC_AVAILABLE_ACCOUNTS}
    for account in dynamic:
        if account.get("customer_id"):
            merged[account["customer_id"]] = account
    accounts = sorted(merged.values(), key=lambda x: (x.get("account_name") or "").lower())
    return {"login_customer_id": login, "source": "dynamic_plus_static" if dynamic else "static", "count": len(accounts), "accounts": accounts}


def tool_list_google_ads_fields(args: Dict[str, Any]) -> Dict[str, Any]:
    try:
        registry = _load_field_registry()
        fields = registry.get("fields", {})
        presets = registry.get("presets", {})
        entity = (args.get("entity") or "").lower().strip()
        priority = (args.get("priority") or "").upper().strip()
        kind = (args.get("kind") or "").lower().strip()
        if entity and entity not in presets:
            return {"error": {"detail": f"invalid entity '{entity}'. Use one of: {', '.join(sorted(presets))}"}}
        if priority and priority not in {"P0", "P1", "P2"}:
            return {"error": {"detail": "priority must be one of P0, P1, P2"}}
        if kind and kind not in {"metric", "dimension"}:
            return {"error": {"detail": "kind must be metric or dimension"}}
        from_resource = presets[entity]["from"] if entity else None
        out = []
        for name, meta in sorted(fields.items()):
            if from_resource and from_resource not in meta.get("resources", []):
                continue
            if priority and meta.get("priority") != priority:
                continue
            if kind and meta.get("kind") != kind:
                continue
            out.append({"name": name, "label": meta.get("label"), "kind": meta.get("kind"), "format": meta.get("format"), "google_ads_field": meta.get("google_ads_field"), "verified": bool(meta.get("verified", False)), "priority": meta.get("priority"), "verification_note": meta.get("verification_note")})
        return {"version": registry.get("version"), "entity": entity or None, "count": len(out), "fields": out}
    except Exception as e:
        return {"error": {"detail": str(e)}}


def tool_validate_google_ads_registry(args: Dict[str, Any]) -> Dict[str, Any]:
    """
    Safe examples:
      validate_google_ads_registry customer_id=7241931996 login_customer_id=9000159936 entities=["campaign"] priority="P0" max_fields=10 dry_run=true
    """
    try:
        login = _resolve_login_customer_id(args)
        customer_id, warnings = _resolve_child_customer_id(args)
    except ValueError as e:
        return {"error": {"detail": str(e)}}
    registry = _load_field_registry()
    presets = registry.get("presets", {})
    fields = registry.get("fields", {})
    entities = [str(e).lower().strip() for e in (args.get("entities") or DEFAULT_VALIDATION_ENTITIES) if str(e).strip()] or list(DEFAULT_VALIDATION_ENTITIES)
    if len(entities) > MAX_VALIDATION_ENTITIES:
        return {"error": {"detail": f"validate_google_ads_registry supports at most {MAX_VALIDATION_ENTITIES} entities per call"}}
    invalid_entities = [e for e in entities if e not in presets]
    if invalid_entities:
        return {"error": {"detail": f"invalid entities: {', '.join(invalid_entities)}", "valid_entities": sorted(presets)}}
    priority = (args.get("priority") or DEFAULT_VALIDATION_PRIORITY).upper().strip()
    if priority not in {"P0", "P1", "P2"}:
        return {"error": {"detail": "priority must be one of P0, P1, P2"}}
    max_fields = _clamped_int(args.get("max_fields", DEFAULT_VALIDATION_MAX_FIELDS), DEFAULT_VALIDATION_MAX_FIELDS, 1, MAX_VALIDATION_FIELDS)
    include_unverified = bool(args.get("include_unverified", False))
    dry_run = bool(args.get("dry_run", False))
    compact = bool(args.get("compact", False))
    summary: Dict[str, Any] = {}
    planned_queries: List[Dict[str, Any]] = []
    for entity in entities:
        preset = presets[entity]
        from_resource = preset["from"]
        base_names = [name for name in preset.get("base_fields", []) if name in fields and from_resource in fields[name].get("resources", [])]
        base_gaql = [fields[name]["google_ads_field"] for name in base_names]
        candidates = [(name, meta) for name, meta in sorted(fields.items()) if from_resource in meta.get("resources", []) and meta.get("priority") == priority and (include_unverified or bool(meta.get("verified", False)))][:max_fields]
        summary[entity] = {"resource": from_resource, "tested": 0, "passed": 0, "failed": 0, "base_fields": base_names}
        for name, meta in candidates:
            select_cols = _dedupe(base_gaql + [meta["google_ads_field"]])
            query = f"""
            SELECT {', '.join(select_cols)}
            FROM {from_resource}
            WHERE segments.date DURING LAST_7_DAYS
            LIMIT 1
            """
            planned_queries.append({"entity": entity, "resource": from_resource, "field": name, "google_ads_field": meta.get("google_ads_field"), "query": query})
    metadata = {**_base_response_metadata(login, customer_id, warnings), "entity_count": len(entities), "planned_query_count": len(planned_queries), "executed_query_count": 0, "max_fields": max_fields, "dry_run": dry_run}
    if len(planned_queries) > MAX_VALIDATION_FIELDS * len(entities):
        return {"error": {"detail": "planned validation query count exceeds configured safety cap"}, "metadata": metadata}
    if dry_run:
        return {"registry_version": registry.get("version"), "validated_date_range": "LAST_7_DAYS", "priority": priority, "include_unverified": include_unverified, "summary": summary, "planned_queries": [] if compact else planned_queries, "metadata": metadata}
    try:
        client = _new_ads_client(login_cid=login)
        svc = client.get_service("GoogleAdsService")
    except Exception as e:
        return {"error": {"detail": str(e)}, "metadata": metadata}
    passed: List[Dict[str, Any]] = []
    failed: List[Dict[str, Any]] = []
    suggestions: List[Dict[str, Any]] = []
    for item in planned_queries:
        entity = item["entity"]
        meta = fields[item["field"]]
        result_base = {"entity": entity, "resource": item["resource"], "field": item["field"], "google_ads_field": item["google_ads_field"], "priority": meta.get("priority"), "verified": bool(meta.get("verified", False))}
        try:
            _run_validation_query(svc, customer_id, item["query"])
            passed.append(result_base if compact else {**result_base, "query": item["query"]})
            summary[entity]["passed"] += 1
        except GoogleAdsException as e:
            messages = _google_ads_error_messages(e)
            failed.append({**result_base, "query": item["query"], "error": _err_from_gax(e), "messages": messages} if not compact else {**result_base, "messages": messages})
            suggestions.append(_field_registry_suggestion(item["field"], meta, item["resource"]))
            summary[entity]["failed"] += 1
        except Exception as e:
            failed.append({**result_base, "query": item["query"], "error": {"detail": str(e)}, "messages": [str(e)]} if not compact else {**result_base, "messages": [str(e)]})
            suggestions.append(_field_registry_suggestion(item["field"], meta, item["resource"]))
            summary[entity]["failed"] += 1
        finally:
            summary[entity]["tested"] += 1
            metadata["executed_query_count"] += 1
    return {"registry_version": registry.get("version"), "validated_date_range": "LAST_7_DAYS", "priority": priority, "include_unverified": include_unverified, "summary": summary, "passed_fields": passed, "failed_fields": failed, "suggested_registry_updates": suggestions, "metadata": metadata}


# -------------------- Query tools --------------------
def tool_fetch_campaign_summary(args: Dict[str, Any]) -> Dict[str, Any]:
    try:
        login = _resolve_login_customer_id(args)
        customer_id, warnings = _resolve_child_customer_id(args)
    except ValueError as e:
        return {"error": {"detail": str(e)}}
    where_time = _where_time(args)
    min_spend = max(1.0, float(args.get("min_spend", 1.0)))
    q = f"""
    SELECT campaign.id, campaign.name, campaign.status, metrics.impressions, metrics.clicks, metrics.cost_micros, metrics.conversions, metrics.conversions_value
    FROM campaign
    WHERE {where_time} AND metrics.cost_micros >= {int(min_spend * 1_000_000)}
    ORDER BY metrics.cost_micros DESC
    """
    try:
        client = _new_ads_client(login_cid=login)
        rows = client.get_service("GoogleAdsService").search(request={"customer_id": customer_id, "query": q})
        out: List[Dict[str, Any]] = []
        for r in rows:
            cost = _money(getattr(r.metrics, "cost_micros", 0))
            imps = int(getattr(r.metrics, "impressions", 0) or 0)
            clicks = int(getattr(r.metrics, "clicks", 0) or 0)
            conv = float(getattr(r.metrics, "conversions", 0.0) or 0.0)
            conv_val = float(getattr(r.metrics, "conversions_value", 0.0) or 0.0)
            out.append({"campaign_id": str(r.campaign.id), "campaign_name": r.campaign.name, "status": r.campaign.status.name, "impressions": imps, "clicks": clicks, "cost": round(cost, 2), "conversions": round(conv, 2), "conv_value": round(conv_val, 2), "ctr_pct": round((clicks / imps * 100) if imps else 0.0, 2), "cpc": round((cost / clicks) if clicks else 0.0, 2), "cpa": round((cost / conv) if conv else 0.0, 2), "roas": round((conv_val / cost) if cost > 0 else 0.0, 2)})
        return {"query": q, "rows": out, "metadata": _base_response_metadata(login, customer_id, warnings)}
    except GoogleAdsException as e:
        return {"error": _err_from_gax(e)}
    except Exception as e:
        return {"error": {"detail": str(e)}}


def tool_fetch_metrics(args: Dict[str, Any]) -> Dict[str, Any]:
    """
    Safe examples:
      fetch_metrics customer_id=7241931996 login_customer_id=9000159936 entity=campaign compact=true limit=25
      fetch_metrics customer_id=7987978735 login_customer_id=9000159936 entity=campaign fields=[cost, impressions, clicks]
    """
    try:
        login = _resolve_login_customer_id(args)
        customer_id, warnings = _resolve_child_customer_id(args)
    except ValueError as e:
        return {"error": {"detail": str(e)}}
    entity = (args.get("entity") or "campaign").lower().strip()
    fields_arg = args.get("fields")
    if isinstance(fields_arg, list) and len(fields_arg) > MAX_FIELDS_PER_FETCH:
        return {"error": {"detail": f"fetch_metrics accepts at most {MAX_FIELDS_PER_FETCH} requested fields per call"}}
    requested_fields = [str(f).strip() for f in fields_arg if str(f).strip()] if isinstance(fields_arg, list) else None
    from_resource, select_cols, resolved = _resolve_registry_fields(entity, requested_fields)
    if resolved and "error" in resolved[0]:
        return {"error": {"detail": "invalid fetch_metrics fields", "issues": resolved}}
    selected_fields = resolved
    columns = [field["name"] for field in selected_fields]
    ids = [normalize_customer_id(x, "ids item") for x in (args.get("ids") or []) if str(x).strip()]
    id_col = {"account": "customer.id", "campaign": "campaign.id", "ad_group": "ad_group.id", "ad": "ad_group_ad.ad.id", "asset_group": "asset_group.id"}.get(entity)
    id_clause = f" AND {id_col} IN ({','.join(ids)}) " if ids and id_col else ""
    spend_clause = ""
    if args.get("min_spend") is not None and _registry_field_is_compatible("cost", from_resource):
        try:
            spend_clause = f" AND metrics.cost_micros >= {int(max(1.0, float(args.get('min_spend'))) * 1_000_000)} "
        except Exception:
            return {"error": {"detail": "min_spend must be a number"}}
    order_by = (args.get("order_by") or _registry_presets().get(entity, {}).get("order_by") or "").strip()
    order_clause = ""
    if order_by:
        order_meta = _registry_fields().get(order_by)
        if not order_meta:
            return {"error": {"detail": f"invalid order_by '{order_by}'. Use a public registry field name."}}
        if from_resource not in order_meta.get("resources", []):
            return {"error": {"detail": f"order_by field '{order_by}' is not compatible with entity '{entity}'"}}
        order_clause = f" ORDER BY {order_meta['google_ads_field']} DESC "
    limit = _clamped_int(args.get("limit", DEFAULT_FETCH_LIMIT), DEFAULT_FETCH_LIMIT, 1, MAX_FETCH_LIMIT)
    compact = bool(args.get("compact", False))
    dry_run = bool(args.get("dry_run", False))
    q = f"""
    SELECT {', '.join(select_cols)}
    FROM {from_resource}
    WHERE {_where_time(args)}{id_clause}{spend_clause}
    {order_clause}
    LIMIT {limit}
    """
    metadata = {**_base_response_metadata(login, customer_id, warnings), "row_count": 0, "field_count": len(columns), "limit": limit, "compact": compact, "dry_run": dry_run}
    if dry_run:
        return {"query": q, "entity": entity, "columns": columns, "selected_fields": selected_fields, "metadata": metadata}
    try:
        client = _new_ads_client(login_cid=login)
        resp = client.get_service("GoogleAdsService").search(request={"customer_id": customer_id, "query": q})
        dict_rows = [_serialize_registry_row(r, selected_fields) for r in resp]
        metadata["row_count"] = len(dict_rows)
        if compact:
            return {"query": q, "entity": entity, "columns": columns, "rows": [[row.get(col) for col in columns] for row in dict_rows], "metadata": metadata}
        return {"query": q, "entity": entity, "rows": dict_rows, "metadata": metadata}
    except GoogleAdsException as e:
        return {"error": _err_from_gax(e), "metadata": metadata}
    except Exception as e:
        return {"error": {"detail": str(e)}, "metadata": metadata}


def tool_fetch_search_terms(args: Dict[str, Any]) -> Dict[str, Any]:
    try:
        login = _resolve_login_customer_id(args)
        customer_id, warnings = _resolve_child_customer_id(args)
    except ValueError as e:
        return {"error": {"detail": str(e)}}
    min_spend = max(1.0, float(args.get("min_spend", 1.0)))
    min_clicks = int(args.get("min_clicks", 0))
    cids = [normalize_customer_id(c, "campaign_ids item") for c in (args.get("campaign_ids") or [])]
    agids = [normalize_customer_id(g, "ad_group_ids item") for g in (args.get("ad_group_ids") or [])]
    filters = [_where_time(args), f" AND metrics.cost_micros >= {int(min_spend * 1_000_000)} "]
    if min_clicks > 0:
        filters.append(f" AND metrics.clicks >= {min_clicks} ")
    if cids:
        filters.append(f" AND campaign.id IN ({','.join(cids)}) ")
    if agids:
        filters.append(f" AND ad_group.id IN ({','.join(agids)}) ")
    limit = max(1, min(int(args.get("limit", 100)), 1000))
    q = f"""
    SELECT search_term_view.search_term, campaign.id, campaign.name, ad_group.id, ad_group.name, metrics.impressions, metrics.clicks, metrics.cost_micros, metrics.conversions, metrics.conversions_value
    FROM search_term_view
    WHERE {''.join(filters)}
    ORDER BY metrics.cost_micros DESC
    LIMIT {limit}
    """
    try:
        client = _new_ads_client(login_cid=login)
        rows = client.get_service("GoogleAdsService").search(request={"customer_id": customer_id, "query": q})
        out = [{"search_term": r.search_term_view.search_term, "campaign_id": str(r.campaign.id), "campaign_name": r.campaign.name, "ad_group_id": str(r.ad_group.id), "ad_group_name": r.ad_group.name, "impressions": int(r.metrics.impressions or 0), "clicks": int(r.metrics.clicks or 0), "cost": _money(r.metrics.cost_micros), "conversions": float(r.metrics.conversions or 0.0), "conv_value": float(r.metrics.conversions_value or 0.0)} for r in rows]
        return {"query": q, "rows": out, "metadata": _base_response_metadata(login, customer_id, warnings)}
    except GoogleAdsException as e:
        return {"error": _err_from_gax(e)}
    except Exception as e:
        return {"error": {"detail": str(e)}}


def tool_fetch_change_history(args: Dict[str, Any]) -> Dict[str, Any]:
    try:
        login = _resolve_login_customer_id(args)
        customer_id, warnings = _resolve_child_customer_id(args)
    except ValueError as e:
        return {"error": {"detail": str(e)}}
    tr = args.get("time_range") or {}
    since = tr.get("since")
    until = tr.get("until")
    if not (since and until):
        return {"error": {"detail": "time_range.since and time_range.until are required"}}
    limit = max(1, min(int(args.get("limit", 200)), 1000))
    types = args.get("resource_types") or []
    type_filter = f" AND change_event.resource_type IN ({','.join([repr(t) for t in types])}) " if types else ""
    q = f"""
    SELECT change_event.change_date_time, change_event.resource_type, change_event.client_type, change_event.user_email, change_event.change_resource_name
    FROM change_event
    WHERE change_event.change_date_time BETWEEN '{since} 00:00:00' AND '{until} 23:59:59' {type_filter}
    ORDER BY change_event.change_date_time DESC
    LIMIT {limit}
    """
    try:
        client = _new_ads_client(login_cid=login)
        rows = client.get_service("GoogleAdsService").search(request={"customer_id": customer_id, "query": q})
        out = [{"time": r.change_event.change_date_time, "resource_type": r.change_event.resource_type.name, "client_type": r.change_event.client_type.name, "user": r.change_event.user_email, "change_resource_name": r.change_event.change_resource_name} for r in rows]
        return {"query": q, "changes": out, "metadata": _base_response_metadata(login, customer_id, warnings)}
    except GoogleAdsException as e:
        return {"error": _err_from_gax(e)}
    except Exception as e:
        return {"error": {"detail": str(e)}}


def tool_fetch_budget_pacing(args: Dict[str, Any]) -> Dict[str, Any]:
    try:
        login = _resolve_login_customer_id(args)
        customer_id, warnings = _resolve_child_customer_id(args)
    except ValueError as e:
        return {"error": {"detail": str(e)}}
    month = args.get("month")
    target = args.get("target_spend")
    if not (month and target is not None):
        return {"error": {"detail": "month and target_spend are required"}}
    target = float(target)
    year, mon = map(int, month.split("-"))
    start = datetime.date(year, mon, 1)
    today = datetime.date.today()
    if today.year == year and today.month == mon:
        end = today
        days_elapsed = (end - start).days + 1
    else:
        end = (start.replace(day=28) + datetime.timedelta(days=4)).replace(day=1) - datetime.timedelta(days=1)
        days_elapsed = (end - start).days + 1
    days_in_month = ((start.replace(day=28) + datetime.timedelta(days=4)).replace(day=1) - start).days
    q = f"SELECT segments.date, metrics.cost_micros FROM customer WHERE segments.date BETWEEN '{start:%Y-%m-%d}' AND '{end:%Y-%m-%d}'"
    try:
        client = _new_ads_client(login_cid=login)
        rows = client.get_service("GoogleAdsService").search(request={"customer_id": customer_id, "query": q})
        mtd_cost = sum(_money(r.metrics.cost_micros) for r in rows)
        avg_per_day = (mtd_cost / days_elapsed) if days_elapsed else 0.0
        projected_eom = round(avg_per_day * days_in_month, 2)
        pace_status = "over" if projected_eom > target * 1.05 else "under" if projected_eom < target * 0.95 else "on_track"
        return {"month": month, "target": round(target, 2), "mtd_spend": round(mtd_cost, 2), "projected_eom": projected_eom, "days_elapsed": days_elapsed, "days_in_month": days_in_month, "pace_status": pace_status, "query": q, "metadata": _base_response_metadata(login, customer_id, warnings)}
    except GoogleAdsException as e:
        return {"error": _err_from_gax(e)}
    except Exception as e:
        return {"error": {"detail": str(e)}}


def tool_fetch_geo_performance(args: Dict[str, Any]) -> Dict[str, Any]:
    try:
        login = _resolve_login_customer_id(args)
        customer_id, warnings = _resolve_child_customer_id(args)
    except ValueError as e:
        return {"error": {"detail": str(e)}}
    level = (args.get("level") or "city").lower().strip()
    level_map = {"city": ("geo_target_city", "city"), "region": ("geo_target_region", "region"), "country": ("geo_target_country", "country")}
    if level not in level_map:
        return {"error": {"detail": f"invalid level '{level}' (use city|region|country)"}}
    geo_attr, geo_key = level_map[level]
    view = (args.get("view") or "geographic").lower().strip()
    if view not in {"geographic", "user_location"}:
        return {"error": {"detail": f"invalid view '{view}' (use geographic|user_location)"}}
    from_view = "geographic_view" if view == "geographic" else "user_location_view"
    cids = [normalize_customer_id(c, "campaign_ids item") for c in (args.get("campaign_ids") or [])]
    cid_clause = f" AND campaign.id IN ({','.join(cids)}) " if cids else ""
    spend_clause = ""
    if args.get("min_spend") is not None:
        spend_clause = f" AND metrics.cost_micros >= {int(max(0.0, float(args.get('min_spend', 0.0))) * 1_000_000)} "
    q = f"""
    SELECT campaign.id, campaign.name, segments.{geo_attr}, metrics.impressions, metrics.clicks, metrics.cost_micros, metrics.conversions, metrics.conversions_value
    FROM {from_view}
    WHERE {_where_time(args)}{cid_clause}{spend_clause}
    ORDER BY metrics.cost_micros DESC
    """
    try:
        client = _new_ads_client(login_cid=login)
        rows = client.get_service("GoogleAdsService").search(request={"customer_id": customer_id, "query": q})
        out: List[Dict[str, Any]] = []
        totals_by_campaign: Dict[str, Dict[str, float]] = {}
        for r in rows:
            cost = _money(getattr(r.metrics, "cost_micros", 0))
            imps = int(getattr(r.metrics, "impressions", 0) or 0)
            clicks = int(getattr(r.metrics, "clicks", 0) or 0)
            conv = float(getattr(r.metrics, "conversions", 0.0) or 0.0)
            conv_val = float(getattr(r.metrics, "conversions_value", 0.0) or 0.0)
            geo_label = getattr(r.segments, geo_attr, None)
            row = {"campaign_id": str(r.campaign.id), "campaign_name": r.campaign.name, geo_key: str(geo_label) if geo_label is not None else "", "impressions": imps, "clicks": clicks, "cost": round(cost, 2), "conversions": round(conv, 2), "conv_value": round(conv_val, 2)}
            out.append(row)
            key = str(r.campaign.id)
            totals_by_campaign.setdefault(key, {"cost": 0.0, "clicks": 0.0, "impressions": 0.0, "conversions": 0.0, "conv_value": 0.0})
            totals_by_campaign[key]["cost"] += cost
            totals_by_campaign[key]["clicks"] += clicks
            totals_by_campaign[key]["impressions"] += imps
            totals_by_campaign[key]["conversions"] += conv
            totals_by_campaign[key]["conv_value"] += conv_val
        totals = {cid: {"cost": round(v["cost"], 2), "clicks": int(v["clicks"]), "impressions": int(v["impressions"]), "conversions": round(v["conversions"], 2), "conv_value": round(v["conv_value"], 2)} for cid, v in totals_by_campaign.items()}
        return {"query": q, "view": from_view, "level": level, "rows": out, "totals_by_campaign": totals, "metadata": _base_response_metadata(login, customer_id, warnings)}
    except GoogleAdsException as e:
        return {"error": _err_from_gax(e)}
    except Exception as e:
        return {"error": {"detail": str(e)}}


# ---------- TOOLS (schemas) ----------
DATE_PRESET_SCHEMA = {"type": "string", "enum": ["TODAY", "YESTERDAY", "LAST_7_DAYS", "LAST_30_DAYS", "THIS_MONTH", "LAST_MONTH"]}
TIME_RANGE_SCHEMA = {"type": "object", "additionalProperties": False, "properties": {"since": {"type": "string", "maxLength": 10, "pattern": "^\\d{4}-\\d{2}-\\d{2}$"}, "until": {"type": "string", "maxLength": 10, "pattern": "^\\d{4}-\\d{2}-\\d{2}$"}}}
CUSTOMER_ID_SCHEMA = {"type": "string", "maxLength": 20, "pattern": "^[0-9-]*$"}
ENTITY_ENUM = ["account", "campaign", "ad_group", "ad", "search_term", "geo", "user_location", "landing_page", "conversion_action", "asset_group", "video"]

TOOLS = [
    {"name": "fetch_campaign_summary", "description": "Per-campaign KPIs for a child customer_id under the login_customer_id MCC.", "inputSchema": {"type": "object", "additionalProperties": False, "properties": {"customer_id": CUSTOMER_ID_SCHEMA, "date_preset": DATE_PRESET_SCHEMA, "time_range": TIME_RANGE_SCHEMA, "min_spend": {"type": "number", "minimum": 1, "default": 1.0}, "login_customer_id": CUSTOMER_ID_SCHEMA}, "required": ["customer_id"]}},
    {"name": "fetch_metrics", "description": "Generic Google Ads metrics for a child customer_id using public registry field names.", "inputSchema": {"type": "object", "additionalProperties": False, "properties": {"customer_id": CUSTOMER_ID_SCHEMA, "entity": {"type": "string", "enum": ENTITY_ENUM, "default": "campaign"}, "ids": {"type": "array", "maxItems": 200, "items": {"type": "string", "maxLength": 30, "pattern": "^[0-9-]*$"}}, "fields": {"type": "array", "maxItems": MAX_FIELDS_PER_FETCH, "items": {"type": "string", "maxLength": 96}}, "date_preset": DATE_PRESET_SCHEMA, "time_range": TIME_RANGE_SCHEMA, "min_spend": {"type": "number", "minimum": 1}, "limit": {"type": "integer", "minimum": 1, "maximum": MAX_FETCH_LIMIT, "default": DEFAULT_FETCH_LIMIT}, "order_by": {"type": "string", "maxLength": 96}, "dry_run": {"type": "boolean", "default": False}, "compact": {"type": "boolean", "default": False}, "login_customer_id": CUSTOMER_ID_SCHEMA}, "required": ["customer_id"]}},
    {"name": "list_google_ads_fields", "description": "List registry fields available to fetch_metrics.", "inputSchema": {"type": "object", "additionalProperties": False, "properties": {"entity": {"type": "string", "enum": ENTITY_ENUM}, "priority": {"type": "string", "enum": ["P0", "P1", "P2"]}, "kind": {"type": "string", "enum": ["metric", "dimension"]}}}},
    {"name": "validate_google_ads_registry", "description": "Run capped live LIMIT 1 GAQL checks for registry field/resource compatibility on a child customer_id.", "inputSchema": {"type": "object", "additionalProperties": False, "properties": {"customer_id": CUSTOMER_ID_SCHEMA, "login_customer_id": CUSTOMER_ID_SCHEMA, "entities": {"type": "array", "maxItems": MAX_VALIDATION_ENTITIES, "items": {"type": "string", "enum": ENTITY_ENUM}, "default": DEFAULT_VALIDATION_ENTITIES}, "priority": {"type": "string", "enum": ["P0", "P1", "P2"], "default": DEFAULT_VALIDATION_PRIORITY}, "include_unverified": {"type": "boolean", "default": False}, "max_fields": {"type": "integer", "minimum": 1, "maximum": MAX_VALIDATION_FIELDS, "default": DEFAULT_VALIDATION_MAX_FIELDS}, "dry_run": {"type": "boolean", "default": False}, "compact": {"type": "boolean", "default": False}}, "required": ["customer_id"]}},
    {"name": "fetch_search_terms", "description": "Top search terms by spend for a child customer_id.", "inputSchema": {"type": "object", "additionalProperties": False, "properties": {"customer_id": CUSTOMER_ID_SCHEMA, "date_preset": DATE_PRESET_SCHEMA, "time_range": TIME_RANGE_SCHEMA, "min_spend": {"type": "number", "minimum": 1, "default": 1.0}, "min_clicks": {"type": "integer", "minimum": 0, "default": 0}, "campaign_ids": {"type": "array", "maxItems": 200, "items": {"type": "string", "maxLength": 30, "pattern": "^[0-9-]*$"}}, "ad_group_ids": {"type": "array", "maxItems": 200, "items": {"type": "string", "maxLength": 30, "pattern": "^[0-9-]*$"}}, "limit": {"type": "integer", "minimum": 1, "maximum": 1000, "default": 100}, "login_customer_id": CUSTOMER_ID_SCHEMA}, "required": ["customer_id"]}},
    {"name": "fetch_change_history", "description": "Change events within a date range for a child customer_id.", "inputSchema": {"type": "object", "additionalProperties": False, "properties": {"customer_id": CUSTOMER_ID_SCHEMA, "time_range": TIME_RANGE_SCHEMA, "resource_types": {"type": "array", "maxItems": 50, "items": {"type": "string", "maxLength": 64}}, "limit": {"type": "integer", "minimum": 1, "maximum": 1000, "default": 200}, "login_customer_id": CUSTOMER_ID_SCHEMA}, "required": ["customer_id", "time_range"]}},
    {"name": "fetch_budget_pacing", "description": "Month-to-date spend and projected EOM vs target for a child customer_id.", "inputSchema": {"type": "object", "additionalProperties": False, "properties": {"customer_id": CUSTOMER_ID_SCHEMA, "month": {"type": "string", "maxLength": 7, "pattern": "^\\d{4}-\\d{2}$"}, "target_spend": {"type": "number"}, "login_customer_id": CUSTOMER_ID_SCHEMA}, "required": ["customer_id", "month", "target_spend"]}},
    {"name": "list_resources", "description": "List accessible Google Ads customer IDs for the authenticated user/MCC.", "inputSchema": {"type": "object", "additionalProperties": False, "properties": {"login_customer_id": CUSTOMER_ID_SCHEMA}}},
    {"name": "list_available_accounts", "description": "List known child accounts under the MCC, with dynamic customer_client lookup plus static fallback.", "inputSchema": {"type": "object", "additionalProperties": False, "properties": {"login_customer_id": CUSTOMER_ID_SCHEMA, "include_dynamic": {"type": "boolean", "default": True}}}},
    {"name": "list_accessible_accounts", "description": "Alias for list_available_accounts.", "inputSchema": {"type": "object", "additionalProperties": False, "properties": {"login_customer_id": CUSTOMER_ID_SCHEMA, "include_dynamic": {"type": "boolean", "default": True}}}},
    {"name": "auth_diagnostics", "description": "Show non-secret auth/account diagnostics and accessible customer IDs if available.", "inputSchema": {"type": "object", "additionalProperties": False, "properties": {"login_customer_id": CUSTOMER_ID_SCHEMA}}},
    {"name": "fetch_geo_performance", "description": "Geo performance for selected campaigns using geographic_view or user_location_view.", "inputSchema": {"type": "object", "additionalProperties": False, "properties": {"customer_id": CUSTOMER_ID_SCHEMA, "date_preset": DATE_PRESET_SCHEMA, "time_range": TIME_RANGE_SCHEMA, "campaign_ids": {"type": "array", "maxItems": 200, "items": {"type": "string", "maxLength": 30, "pattern": "^[0-9-]*$"}}, "level": {"type": "string", "enum": ["city", "region", "country"], "default": "city"}, "view": {"type": "string", "enum": ["geographic", "user_location"], "default": "geographic"}, "min_spend": {"type": "number", "minimum": 0}, "login_customer_id": CUSTOMER_ID_SCHEMA}, "required": ["customer_id"]}},
    {"name": "ping", "description": "Health check (public).", "inputSchema": {"type": "object", "additionalProperties": False, "properties": {}}},
    {"name": "debug_login_header", "description": "Show which login_customer_id (MCC) the server will use.", "inputSchema": {"type": "object", "additionalProperties": False, "properties": {}}},
    {"name": "echo_short", "description": "Echo a short string. Use only for debugging tool calls.", "inputSchema": {"type": "object", "additionalProperties": False, "properties": {"msg": {"type": "string", "maxLength": 80}}, "required": ["msg"]}},
    {"name": "noop_ok", "description": "Returns a tiny fixed JSON object.", "inputSchema": {"type": "object", "additionalProperties": False, "properties": {}}},
]


# -------------------- Discovery (minimal) --------------------
@app.get("/", include_in_schema=False)
@app.head("/", include_in_schema=False)
def root(request: Request):
    if request.method == "HEAD":
        return PlainTextResponse("")
    return PlainTextResponse("ok")


@app.get("/.well-known/mcp.json")
def mcp_discovery():
    return JSONResponse({"mcpVersion": MCP_PROTO_DEFAULT, "name": APP_NAME, "version": APP_VER, "auth": {"type": "none"}, "capabilities": {"tools": {"listChanged": True}}, "endpoints": {"rpc": "/"}, "tools": TOOLS})


# -------------------- JSON-RPC (initialize, tools/list, tools/call) --------------------
def _pack_text(data: Any) -> Dict[str, Any]:
    try:
        text = data if isinstance(data, str) else json.dumps(data, ensure_ascii=False)
    except Exception:
        text = str(data)
    return {"content": [{"type": "text", "text": text}]}


def _call_tool(name: str, args: Dict[str, Any]) -> Dict[str, Any]:
    if name == "ping":
        return _pack_text(tool_ping(args))
    if name == "debug_login_header":
        return _pack_text(tool_debug_login_header(args))
    if name == "echo_short":
        return _pack_text(tool_echo_short(args))
    if name == "noop_ok":
        return _pack_text(tool_noop_ok(args))
    if name == "list_resources":
        return _pack_text(tool_list_resources(args))
    if name in {"list_available_accounts", "list_accessible_accounts"}:
        return _pack_text(tool_list_available_accounts(args))
    if name == "auth_diagnostics":
        return _pack_text(tool_auth_diagnostics(args))
    if name == "list_google_ads_fields":
        return _pack_text(tool_list_google_ads_fields(args))
    if name == "validate_google_ads_registry":
        return _pack_text(tool_validate_google_ads_registry(args))
    if name == "fetch_campaign_summary":
        return _pack_text(tool_fetch_campaign_summary(args))
    if name == "fetch_metrics":
        return _pack_text(tool_fetch_metrics(args))
    if name == "fetch_search_terms":
        return _pack_text(tool_fetch_search_terms(args))
    if name == "fetch_change_history":
        return _pack_text(tool_fetch_change_history(args))
    if name == "fetch_budget_pacing":
        return _pack_text(tool_fetch_budget_pacing(args))
    if name == "fetch_geo_performance":
        return _pack_text(tool_fetch_geo_performance(args))
    return {"error": {"code": -32601, "message": f"Unknown tool: {name}"}}


@app.post("/")
async def rpc(request: Request):
    try:
        payload = await request.json()
    except Exception:
        return JSONResponse({"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "Parse error"}})

    def handle(obj: Dict[str, Any]) -> Dict[str, Any] | None:
        if not isinstance(obj, dict):
            return {"jsonrpc": "2.0", "id": None, "error": {"code": -32600, "message": "Invalid Request"}}
        _id = obj.get("id")
        method = (obj.get("method") or "").lower()
        if method == "initialize":
            client_proto = (obj.get("params") or {}).get("protocolVersion") or MCP_PROTO_DEFAULT
            return {"jsonrpc": "2.0", "id": _id, "result": {"protocolVersion": client_proto, "capabilities": {"tools": {"listChanged": True}}, "serverInfo": {"name": APP_NAME, "version": APP_VER}, "tools": TOOLS}}
        if method in ("initialized", "notifications/initialized"):
            return {"jsonrpc": "2.0", "id": _id, "result": {"ok": True}}
        if method in ("tools/list", "tools.list", "list_tools", "tools.index"):
            return {"jsonrpc": "2.0", "id": _id, "result": {"tools": TOOLS}}
        if method == "tools/call":
            params = obj.get("params") or {}
            res = _call_tool(params.get("name"), params.get("arguments") or {})
            if "error" in res and "content" not in res:
                return {"jsonrpc": "2.0", "id": _id, "error": res["error"]}
            return {"jsonrpc": "2.0", "id": _id, "result": res}
        return {"jsonrpc": "2.0", "id": _id, "error": {"code": -32601, "message": f"Method not found: {method}"}}

    if isinstance(payload, list):
        out = [resp for entry in payload if (resp := handle(entry)) is not None]
        return JSONResponse(out if out else [], status_code=200)
    resp = handle(payload)
    return JSONResponse(resp if resp is not None else {}, status_code=200)


# -------------------- Local dev --------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
