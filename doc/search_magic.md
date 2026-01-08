---
jupytext:
  text_representation:
    format_name: myst
kernelspec:
  display_name: Python 3
  name: python3
---

# Simplified Search

We are experimenting with using a small package [esgf-magic](https://github.com/nocollier/esgf-magic) to allow users to type terms from the ESGF control vocabulary (e.g. `historical`, `tas`) separated by spaces without the accompanying collection name (e.g. `experiment_id`, `variable_id` ). For example:

```{code-cell}
:tags: [skip-execution]
from intake_esgf import ESGFCatalog
cat = ESGFCatalog().search("gpp hist* cesm2* mon not_a_term")
print(cat)
```

Note that a search in this manner is case insensitive and the `*` wildcard may be used. This will expand to a faceted search only with terms and collections found within the control vocabulary. For example, the `not_a_term` is not in the CV and will be discarded in the search. To see exactly what we searched for, print out the session log and examine the search line.

```{code-cell}
:tags: [skip-execution]
print(cat.session_log())
```
