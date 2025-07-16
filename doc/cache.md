---
jupytext:
  text_representation:
    format_name: myst
kernelspec:
  display_name: Python 3
  name: python3
---

# Caching Requests

```{code-cell}
---
tags: [remove-cell]
---
import time
from intake_esgf import ESGFCatalog
import intake_esgf
```

We would like your searches to be fast enough that you will want to just leave
them in your analysis scripts. This makes your scripts portable to other systems
and shareable with colleagues without having to move data yourself. However,
this means that each time you run your script, behind the scenes we are making
requests to the index node for information, and you will have to wait for those
responses.

In part, this is a good thing. It means that your script will always be using
the most up to date data. But if your search is slow, you will be waiting for
possibly a long time after each execution. This can be a real pain, especially
during your script's development. To reduce this and the burden it places on the
index nodes, we are utilizing [request
caching](https://requests-cache.readthedocs.io/en/stable/).

Consider the following search, wrapped in a performance timer.

```{code-cell}
search_time = time.perf_counter()
cat = ESGFCatalog().search(
    experiment_id=["historical"],
    variable_id=["tas", "thetao"],
    frequency="mon",
)
print(f"The first search took {time.perf_counter() - search_time:.1f} [s].")
```

And now we repeat the search:

```{code-cell}
search_time = time.perf_counter()
cat = ESGFCatalog().search(
    experiment_id=["historical"],
    variable_id=["tas", "thetao"],
    frequency="mon",
)
print(f"The second search took {time.perf_counter() - search_time:.1f} [s].")
```

Notice that the second search is faster. The first time your search made a
request, the system stored the request and the response that was sent. When you
ask for the same search again, instead of re-querying the server, it was loaded
from the local cache.

## Configuration

You can change the behavior of the request caching in the
[configuration](configure) options. Perhaps the most important option to
consider is the expiration of the cache. By default it is set to 1 hour, but you
may wish to change it depending on your use case. The relevant option is
`expire_after` (in seconds) in the options for `requests_cache`:

```{code-cell}
intake_esgf.conf.set(requests_cache=dict(expire_after=1)) # [s]
time.sleep(2) # sleep long enough for cache to expire
```

With these new cache options, repeating the search will again take more time.

```{code-cell}
search_time = time.perf_counter()
cat = ESGFCatalog().search(
    experiment_id=["historical"],
    variable_id=["tas", "thetao"],
    frequency="mon",
)
print(f"The third search took {time.perf_counter() - search_time:.1f} [s].")
```
