from __future__ import annotations

import sys
import types


def _install_google_ads_stubs() -> None:
    google = sys.modules.setdefault("google", types.ModuleType("google"))
    ads = sys.modules.setdefault("google.ads", types.ModuleType("google.ads"))
    googleads = sys.modules.setdefault("google.ads.googleads", types.ModuleType("google.ads.googleads"))
    client_mod = types.ModuleType("google.ads.googleads.client")
    errors_mod = types.ModuleType("google.ads.googleads.errors")

    class GoogleAdsClient:
        @classmethod
        def load_from_dict(cls, cfg):
            obj = cls()
            obj.cfg = cfg
            return obj

    class GoogleAdsException(Exception):
        pass

    client_mod.GoogleAdsClient = GoogleAdsClient
    errors_mod.GoogleAdsException = GoogleAdsException
    sys.modules["google.ads.googleads.client"] = client_mod
    sys.modules["google.ads.googleads.errors"] = errors_mod
    google.ads = ads
    ads.googleads = googleads


_install_google_ads_stubs()

import app  # noqa: E402


def test_normalize_customer_id_dashed():
    assert app.normalize_customer_id("724-193-1996") == "7241931996"


def test_normalize_customer_id_undashed():
    assert app.normalize_customer_id("7241931996") == "7241931996"


def test_normalize_customer_id_invalid():
    try:
        app.normalize_customer_id("724-abc-1996")
    except ValueError as exc:
        assert "numeric" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def test_login_customer_id_and_child_customer_id_are_separate():
    args = {"login_customer_id": "900-015-9936", "customer_id": "724-193-1996"}
    login = app._resolve_login_customer_id(args)
    child, warnings = app._resolve_child_customer_id(args)
    assert login == "9000159936"
    assert child == "7241931996"
    assert login != child
    assert warnings == []
