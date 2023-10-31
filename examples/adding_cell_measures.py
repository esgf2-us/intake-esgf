from intake_esgf import ESGFCatalog

# In order to illustrate how we handle cell measures, consider the r1i1p1f2 variant of
# UKESM1-0-LL for which `areacella` and `sftlf` (land fractions) are not included.

try:
    ESGFCatalog().search(
        source_id=["UKESM1-0-LL"],
        variant_label="r1i1p1f2",
        variable_id=["areacella", "sftlf"],
        experiment_id="historical",
    )
except ValueError:
    print("areacella and sftlf are not part of this source/variant.")

# This is a major pain point of analyzing ESGF model data. Sometimes the cell measure
# information is nowhere close to the experiment/variant which you are analyzing. Say
# that we wanted to do some analysis on the atmospheric temperature variable `tas` and
# the land gross primary productivity `gpp`.

cat = ESGFCatalog().search(
    source_id=["UKESM1-0-LL"],
    variant_label="r1i1p1f2",
    variable_id=["tas", "gpp"],
    experiment_id="historical",
    frequency="mon",
)

# When we ask for the dataset dictionary, we can pass a few keywords. First we will
# ignore the `table_id` facet when creating keys for the dataset dictionary. We do not
# really need to distinguish between `Amon` and `Lmon` here. The more salient keyword
# here is `add_measures`, which is enabled by default.

ds = cat.to_dataset_dict(ignore_facets="table_id", add_measures=True)

# Internally, before returning the dataset dictionary to you, we loop over each dataset
# and extract its search facets, and cell measures and methods. From this we can perform
# a search for the required measures that progressively drops facets until the variable
# is found. If you check the session logs shown below, you will see that first we tried
# dropping the `variant_label`, and then the `experiment_id` until we finally found the
# cell measures in `piControl`.

print(cat.session_log())

# Because the temperature is an atmospheric variable, you will see that `areacella` has
# been added to the dataset. However, the `gpp` also requires land fractions and both
# are added automatically.

print(ds["tas"])
print(ds["gpp"])
