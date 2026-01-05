from .functional_main import main

class _ValidatorModule:
    def __call__(self, algo_name="algo_name", trade_log_path=None, lot_size_file_path=None, segment="UNIVERSAL", ORB_URL=None, ACCESS_TOKEN=None, output_path='output/'):
        if trade_log_path == None:
            return "[ERROR] Trade Log path not provided"
        if (ORB_URL == None) or (ACCESS_TOKEN == None):
            return ("[ERROR] ORB URL AND ACCESS TOKEN REQUIRED.")
        if (segment.upper() == "OPTIONS") or (segment.upper() == "OPTION") :
            segment = "OPTIONS"
            if lot_size_file_path == None:
                return("[ERROR] LOT SIZE FILE NOT PROVIDED")
            
        return main(ORB_URL=ORB_URL, ACCESS_TOKEN=ACCESS_TOKEN, algo_name=algo_name, trade_log_path=trade_log_path, lot_size_file_path=lot_size_file_path, segment=segment, output_path=output_path)

# Replace this module with a callable instance
import sys
sys.modules[__name__] = _ValidatorModule()
