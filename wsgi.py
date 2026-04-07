from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

module_path = Path(__file__).resolve().parent / "veyron-pos.py"
spec = spec_from_file_location("veyron_pos_module", module_path)
module = module_from_spec(spec)
spec.loader.exec_module(module)
app = module.app
