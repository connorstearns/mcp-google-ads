# Ensure project root is importable
import pathlib, sys, types
ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Google Ads import stubs (so app.py imports succeed in tests)
google_module = types.ModuleType("google")
ads_module = types.ModuleType("google.ads")
googleads_module = types.ModuleType("google.ads.googleads")
client_module = types.ModuleType("google.ads.googleads.client")
errors_module = types.ModuleType("google.ads.googleads.errors")

class _StubAdsClient:
    @staticmethod
    def load_from_dict(_config):
        class _Dummy:
            def get_service(self, _name):
                raise RuntimeError("Stubbed client does not implement get_service")
        return _Dummy()

google_module.ads = ads_module
ads_module.googleads = googleads_module
googleads_module.client = client_module
googleads_module.errors = errors_module
client_module.GoogleAdsClient = _StubAdsClient
errors_module.GoogleAdsException = Exception

sys.modules.setdefault("google", google_module)
sys.modules.setdefault("google.ads", ads_module)
sys.modules.setdefault("google.ads.googleads", googleads_module)
sys.modules.setdefault("google.ads.googleads.client", client_module)
sys.modules.setdefault("google.ads.googleads.errors", errors_module)
