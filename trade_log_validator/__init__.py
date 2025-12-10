from .functional_main import main

def __call__(
    algo_name,
    trade_log,
    options_file,
    lot_size,
    segment="UNIVERSAL"
):
    return main(algo_name, trade_log, options_file, lot_size, segment)
