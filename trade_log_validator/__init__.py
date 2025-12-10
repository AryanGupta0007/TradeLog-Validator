from .functional_main import main

class _ValidatorModule:
    def __call__(self, algo_name="algo_name", trade_log_path=None, lot_size_file_path=None, segment="UNIVERSAL", options_file_path=None):
        if trade_log_path == None:
            return "[ERROR] Trade Log path not provided"
        if options_file_path == None:
            return ("[ERROR] options file path not provided will lead the program to skip checks")
        if (segment.upper() == "OPTIONS") or (segment.upper() == "OPTION") :
            segment = "OPTIONS"
            if lot_size_file_path == None:
                return("[ERROR] LOT SIZE FILE NOT PROVIDED")
            
        return main(algo_name=algo_name, trade_log_path=trade_log_path, options_file_path=options_file_path, lot_size_file_path=lot_size_file_path, segment=segment)

# Replace this module with a callable instance
import sys
sys.modules[__name__] = _ValidatorModule()
