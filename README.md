# TPower Utilities

This repository contains small utilities to interact with energy measurement services. It currently bundles two
client libraries:

- `gpm` – a wrapper around GreenPowerMonitor APIs.
- `prmte` – tools for the "Plataforma de Recolección de Medidas en Tiempo Real" (PRMTE) provided by the
  Coordinador Eléctrico Nacional in Chile.

## PRMTE library

The PRMTE library now supports the `medidas-v2` API.  A typical workflow is:

```python
from prmte.core import PRMTEClient
from prmte.api import get_measurements

client = PRMTEClient(api_key="<YOUR API KEY>")
# Fetch one month of data at hourly resolution
df = get_measurements(client, "PM123", period="202401", granularity="1H")
print(df.head())
```

### New features

- Support for the new `/measurement` endpoint.
- Convenience helper :func:`get_measurements` for retrieving and
  resampling data in one call.
- `transform_records` now accepts a `last_reading` argument which allows
  trimming to the last available timestamp.

### Additional helpers

- :func:`get_measurements_range` fetches data between two periods, defaulting to
  the current month when the end period is omitted.
- :func:`measurements_to_excel` exports measurements for several assets to an
  Excel workbook with the mapping table and the consolidated data in either
  ``long`` or ``wide`` format.

Additional helper methods can be added easily.  See `prmte/core.py` for
reference.

