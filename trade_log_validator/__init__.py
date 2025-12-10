from .functional_main import main

class _ValidatorModule:
    def __call__(self, algo_name="algo_name", trade_log_path=None, lot_size=None, segment="UNIVERSAL", options_file_path=None):
        if trade_log_path == None:
            return "Trade Log path not provided"
        if options_file_path == None:
            print("[WARNING] options file path not provided will lead the program to skip checks")
        if (segment.upper() == "OPTIONS") or (segment.upper() == "OPTION") :
            segment = "OPTIONS"
            if lot_size == None:
                print("[WARNING] Contract Lot size not defined using default value 75.")
            
        return main(algo_name, trade_log_path, options_file_path, lot_size, segment)

# Replace this module with a callable instance
import sys
sys.modules[__name__] = _ValidatorModule()
