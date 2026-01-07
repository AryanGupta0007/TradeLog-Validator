from .functional_main import main

class _ValidatorModule:
    def __call__(self, algo_name="algo_name", trade_log_path=None, lot_size_file_path=None, segment="UNIVERSAL", ORB_URL=None, ORB_USERNAME=None, ORB_PASSWORD=None, output_path='output/'):
        if trade_log_path == None:
            return "[ERROR] Trade Log path not provided"
        if (ORB_URL == None) or (ORB_USERNAME == None) or (ORB_PASSWORD == None):
            return ("[ERROR] ORB URL OR CREDENTIALS NOT PROVIDED.")
        if (segment.upper() == "OPTIONS") or (segment.upper() == "OPTION") :
            segment = "OPTIONS"
            if lot_size_file_path == None:
                return("[ERROR] LOT SIZE FILE NOT PROVIDED")
            
        return main(algo_name=algo_name, trade_log_path=trade_log_path, lot_size_file_path=lot_size_file_path, segment=segment, output_path=output_path, ORB_URL=ORB_URL, ORB_USERNAME=ORB_USERNAME, ORB_PASSWORD=ORB_PASSWORD)

# Replace this module with a callable instance
import sys
sys.modules[__name__] = _ValidatorModule()
