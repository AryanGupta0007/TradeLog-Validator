from .functional_main import main

class _ValidatorModule:
    def __call__(self, algo_name, trade_log, options_file, lot_size, segment="UNIVERSAL"):
        return main(algo_name, trade_log, options_file, lot_size, segment)

# Replace this module with a callable instance
import sys
sys.modules[__name__] = _ValidatorModule()
