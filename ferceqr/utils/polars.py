"""
Polars-baased utils
"""
from __future__ import annotations

from functools import reduce
import polars as pl
import operator



def filters_to_predicate(filters: dict[str, object]) -> pl.Expr:
    """
    Build a Polars boolean expression from a simple filter mapping.

    Supports three styles of constraints:

    1. Equality
       A scalar RHS produces `col == value`.

    2. Membership
       A list/tuple/set RHS produces `col.is_in(values)`.

    3. Operator tuple
       A 2-tuple `(op, rhs)` produces the corresponding comparison:
       ``"gt" (>)``, ``"ge" (>=)``, ``"lt" (<)``, ``"le" (<=)``, ``"ne" (!=)``,
       or ``"between"`` with `rhs = (low, high[, inclusive])`.

    Parameters
    ----------
    filters : dict[str, object]
        Mapping from column name to constraint.

    Returns
    -------
    pl.Expr
        Combined conjunction (`&`) of all predicates. If `filters` is empty,
        the identity predicate `pl.lit(True)` is returned.

    Examples
    --------
    >>> expr = _filters_to_predicate({
    ...     "point_of_delivery_balancing_authority": {"PJM", "MISO"},
    ...     "trade_date": ("between", (20240101, 20240331, True)),
    ... })
    >>> lf.filter(expr)  # doctest: +SKIP
    """
    exprs: list[pl.Expr] = []
    for col, val in filters.items():
        c = pl.col(col)
        if (
            isinstance(val, (list, tuple, set))
            and not (len(val) == 2 and isinstance(val[0], str))
        ):
            exprs.append(c.is_in(list(val)))
        elif isinstance(val, tuple) and len(val) == 2 and isinstance(val[0], str):
            op, rhs = val
            if op in {"gt", ">"}: exprs.append(c > rhs)
            elif op in {"ge", ">="}: exprs.append(c >= rhs)
            elif op in {"lt", "<"}: exprs.append(c < rhs)
            elif op in {"le", "<="}: exprs.append(c <= rhs)
            elif op in {"ne", "!="}: exprs.append(c != rhs)
            elif op == "between":
                lo, hi, *rest = rhs if isinstance(rhs, (list, tuple)) else (rhs,)
                inclusive = True
                if rest:
                    inclusive = bool(rest[0])
                if inclusive:
                    exprs.append((c >= lo) & (c <= hi))
                else:
                    exprs.append((c > lo) & (c < hi))
            else:
                raise ValueError(f"Unsupported operator: {op!r} for column {col!r}")
        else:
            exprs.append(c == val)
    return reduce(operator.and_, exprs, pl.lit(True))
