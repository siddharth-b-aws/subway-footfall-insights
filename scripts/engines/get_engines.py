# scripts/engines/get_engine.py

def get_engine(engine_name):
    engine_name = engine_name.lower()

    if engine_name == "pandas":
        from .pandas_engine import PandasEngine
        return PandasEngine()

    elif engine_name == "modin":
        from .modin_engine import ModinEngine
        return ModinEngine()

    elif engine_name == "duckdb":
        from .duckdb_engine import DuckDBEngine
        return DuckDBEngine()

    elif engine_name == "pyspark":
        from .pyspark_engine import PySparkEngine
        return PySparkEngine()

    elif engine_name == "chunked_pandas":
        from .chunked_pandas_engine import ChunkedPandasEngine
        return ChunkedPandasEngine()
    
    elif engine_name == "dask":
        from .dask_engine import DaskEngine
        return DaskEngine()
    # Future support
    elif engine_name == "cudf":
        from .cudf_engine import CuDFEngine
        return CuDFEngine()

    elif engine_name == "cuda_pyspark":
        from .cuda_pyspark_engine import CudaPySparkEngine
        return CudaPySparkEngine()

    raise ValueError(f"Unknown engine: {engine_name}")
