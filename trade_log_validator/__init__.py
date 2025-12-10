from .functional_main import main

class _ValidatorModule:
    def __call__(self, algo_name, trade_log_path, options_file_path, lot_size, segment="UNIVERSAL"):
        return main(algo_name, trade_log_path, options_file_path, lot_size, segment)

# Replace this module with a callable instance
import sys
sys.modules[__name__] = _ValidatorModule()
