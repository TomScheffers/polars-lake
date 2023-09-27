import polars as pl
import numpy as np

lf1 = pl.DataFrame(
    {
        "nrs": [1, 2, 3, None, 5],
        "names": ["foo", "ham", "spam", "egg", None],
        "random": np.random.rand(5),
        "groups": ["A", "A", "A", "A", "A"],
    }
)
lf2 = lf1.with_columns(pl.lit("B").alias("groups"))

lf = pl.concat([lf1.lazy(), lf2.lazy()]).filter(pl.col("groups") == pl.lit("A"))
print(lf.explain(optimized=True))

lf = pl.concat([lf1.filter(pl.col("groups") == pl.lit("A")).lazy(), lf2.filter(pl.col("groups") == pl.lit("A")).lazy()]).filter(pl.col("groups") == pl.lit("A"))
print(lf.explain(optimized=True, ))