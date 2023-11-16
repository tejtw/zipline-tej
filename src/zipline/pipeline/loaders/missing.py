

from zipline.utils.numpy_utils import (
    bool_dtype,
    float64_dtype,
    object_dtype,
    datetime64ns_dtype,
    nan,
    NaTD
)

MISSING_VALUES_BY_DTYPE = {
    bool_dtype: False,
    float64_dtype: nan,
    datetime64ns_dtype: NaTD,
    object_dtype: ""
}