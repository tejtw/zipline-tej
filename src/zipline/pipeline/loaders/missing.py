

from zipline.utils.numpy_utils import (
    bool_dtype,
    float64_dtype,
    object_dtype,
    datetime64ns_dtype,
    nan,
    NaTD,
    NaTns
)

MISSING_VALUES_BY_DTYPE = {
    bool_dtype: False,
    float64_dtype: nan,
    datetime64ns_dtype: NaTD,
    # datetime64ns_dtype:NaTns,
    object_dtype: ""
}