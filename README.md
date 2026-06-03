# mcp-google-ads

This MCP server queries child Google Ads accounts through an MCC login customer ID.

## MCC vs child account IDs

Use `login_customer_id` only for the MCC / manager account context in Google Ads API headers. The default comes from:

```text
GOOGLE_ADS_LOGIN_CUSTOMER_ID=9000159936
```

Use `customer_id` for the child Google Ads account being queried. Dashed and undashed IDs are accepted and normalized by removing dashes.

Examples:

```json
{
  "login_customer_id": "9000159936",
  "customer_id": "7241931996"
}
```

Lazy Dog Restaurants:

```json
{
  "customer_id": "7241931996",
  "login_customer_id": "9000159936"
}
```

Harlem Children's Zone:

```json
{
  "customer_id": "7987978735",
  "login_customer_id": "9000159936"
}
```

## Useful tools

- `list_available_accounts` / `list_accessible_accounts`: returns known child accounts from the built-in registry and, when possible, dynamic `customer_client` results from Google Ads.
- `auth_diagnostics`: returns non-secret auth status, the effective `login_customer_id`, and accessible customer IDs when the API call succeeds.
- Query tools such as `fetch_metrics`, `fetch_campaign_summary`, `fetch_search_terms`, `fetch_change_history`, `fetch_budget_pacing`, `fetch_geo_performance`, and `validate_google_ads_registry` require a child `customer_id`.

## Example calls

Fetch Lazy Dog campaign metrics:

```json
{
  "name": "fetch_metrics",
  "arguments": {
    "customer_id": "7241931996",
    "login_customer_id": "9000159936",
    "entity": "campaign",
    "fields": ["cost", "impressions", "clicks", "conversions"],
    "compact": true,
    "limit": 25
  }
}
```

Fetch HCZ campaign metrics:

```json
{
  "name": "fetch_metrics",
  "arguments": {
    "customer_id": "7987978735",
    "login_customer_id": "9000159936",
    "entity": "campaign",
    "fields": ["cost", "impressions", "clicks", "conversions"],
    "limit": 25
  }
}
```

Dry-run registry validation safely:

```json
{
  "name": "validate_google_ads_registry",
  "arguments": {
    "customer_id": "7241931996",
    "login_customer_id": "9000159936",
    "entities": ["campaign"],
    "priority": "P0",
    "max_fields": 10,
    "dry_run": true
  }
}
```

## Cloud Run config

No additional config is needed if Secret Manager already exposes:

```text
GOOGLE_ADS_LOGIN_CUSTOMER_ID = Secret: LOGIN_CUSTOMER_ID:latest
```

The secret value should remain the MCC ID `9000159936`. Child account IDs should be passed per tool call as `customer_id`.
