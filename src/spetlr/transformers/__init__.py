import warnings

from spetlr.etl.transformers.clean_column_names_transformer import (  # noqa: F401
    CleanColumnNamesTransformer,
)
from spetlr.etl.transformers.country_to_alphacode_transformer import (  # noqa: F401
    CountryToAlphaCodeTransformer,
)
from spetlr.etl.transformers.drop_oldest_duplicate_transformer import (  # noqa: F401
    DropOldestDuplicatesTransformer,
)
from spetlr.etl.transformers.dropColumnsTransformer import (  # noqa: F401
    DropColumnsTransformer,
)
from spetlr.etl.transformers.generate_md5_column_transformer import (  # noqa: F401
    GenerateMd5ColumnTransformer,
)
from spetlr.etl.transformers.join_dataframes_transformer import (  # noqa: F401
    JoinDataframesTransformer,
)
from spetlr.etl.transformers.select_and_cast_columns_transformer import (  # noqa: F401
    SelectAndCastColumnsTransformer,
)
from spetlr.etl.transformers.selectColumnsTransformer import (  # noqa: F401
    SelectColumnsTransformer,
)
from spetlr.etl.transformers.simple_dataframe_filter_transformer import (  # noqa: F401
    DataFrameFilterTransformer,
)
from spetlr.etl.transformers.simple_sql_transformer import (  # noqa: F401
    SimpleSqlServerTransformer,
)
from spetlr.etl.transformers.SimpleSqlTransformer import (  # noqa: F401
    SimpleSqlTransformer,
)
from spetlr.etl.transformers.timezone_transformer import (  # noqa: F401
    TimeZoneTransformer,
)
from spetlr.etl.transformers.union_transformer import UnionTransformer  # noqa: F401
from spetlr.etl.transformers.validfromto_transformer import (  # noqa: F401
    ValidFromToTransformer,
)

warnings.warn(
    "The transformers module is depredated, use etl.transformers instead",
    DeprecationWarning,
)
