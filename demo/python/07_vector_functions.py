#!/usr/bin/env python3
"""Evaluate L2 distance, cosine similarity and inner product with Spark SQL."""

import math

from common import demo_session


def main(argv=None):
    with demo_session(__doc__, argv, snapshot=False) as (spark, args, _, _queries):
        # The functions consume equally sized ARRAY<FLOAT> values. F suffixes
        # make SQL literals floats rather than doubles or decimal values.
        # These in-memory rows need no snapshot or object-storage credentials.
        result = spark.sql("""
            SELECT name,
                   vector_l2_distance(left_vector, right_vector) AS l2_distance,
                   vector_cosine_similarity(left_vector, right_vector) AS cosine_similarity,
                   vector_inner_product(left_vector, right_vector) AS inner_product
            FROM VALUES
                ('same_direction', array(3.0F, 4.0F), array(6.0F, 8.0F)),
                ('orthogonal', array(1.0F, 0.0F), array(0.0F, 1.0F)),
                ('opposite', array(1.0F, 0.0F), array(-1.0F, 0.0F)),
                ('zero_vector', array(0.0F, 0.0F), array(1.0F, 0.0F)),
                ('null_vector', CAST(NULL AS ARRAY<FLOAT>), array(1.0F, 0.0F))
            AS pairs(name, left_vector, right_vector)
        """)
        if args.explain:
            result.explain()
        result.show(truncate=False)

        # Distance ranks ascending; cosine similarity and inner product rank
        # descending. Cosine is undefined (NULL) when either vector is zero.
        expected = {
            "same_direction": (5.0, 1.0, 50.0),
            "orthogonal": (math.sqrt(2.0), 0.0, 0.0),
            "opposite": (2.0, -1.0, -1.0),
            "zero_vector": (1.0, None, 0.0),
            "null_vector": (None, None, None),
        }
        for row in result.collect():
            for actual, wanted in zip(row[1:], expected[row.name]):
                if wanted is None:
                    assert actual is None, f"Expected NULL for {row.name}, got {actual}"
                else:
                    assert actual is not None and math.isclose(
                        actual, wanted, rel_tol=1e-6, abs_tol=1e-6
                    ), f"Unexpected vector score for {row.name}: {actual} != {wanted}"
        print("All vector-function examples passed")


if __name__ == "__main__":
    main()
