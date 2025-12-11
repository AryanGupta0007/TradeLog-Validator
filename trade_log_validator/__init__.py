from .functional_main import main

class _ValidatorModule:
    def __call__(self, algo_name="algo_name", trade_log_path=None, lot_size_file_path=None, segment="UNIVERSAL", mongo_client=None, output_path=None):
        if trade_log_path == None:
            return "[ERROR] Trade Log path not provided"
        if mongo_client == None:
            return ("[ERROR] MONGO CLIENT OBJECT REQUIRED.")
        if (segment.upper() == "OPTIONS") or (segment.upper() == "OPTION") :
            segment = "OPTIONS"
            if lot_size_file_path == None:
                return("[ERROR] LOT SIZE FILE NOT PROVIDED")
            
        return main(mongo_client=mongo_client, algo_name=algo_name, trade_log_path=trade_log_path, lot_size_file_path=lot_size_file_path, segment=segment, output_path=output_path)

# Replace this module with a callable instance
import sys
sys.modules[__name__] = _ValidatorModule()
