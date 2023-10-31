# Attribution of Climate-Driven Carbon Cycle Anomalies

# In this example, we present an analysis that illustrates how changes in future climate
# anomalies (temperature, precipitation, and soil moisture) control anomalies in the
# carbon cycle as measured by gross primary productivity. We will restrict the anlysis
# to the monthly output from a single model for both the historical period as well as
# the aggressive SSP5-8.5 scenario.

from intake_esgf import ESGFCatalog

experiments = ["historical", "ssp585", "ssp370", "ssp245", "ssp126"]
variables = ["gpp", "tas", "pr", "mrso"]
cat = ESGFCatalog(esgf1_indices="esgf-node.llnl.gov").search(
    source_id="CESM2",
    variable_id=variables,
    experiment_id=experiments,
    frequency="mon",
)

# While the ESGF search results will return anything matching the criteria, in this case
# we are only interested in which unique combinations of `source_id`, `member_id`, and
# `grid_label` have all the variables we need for both experiments. Many times modeling
# groups upload different realizations but they do not contain all the variables either
# by oversight or design. Locating and removing these *incomplete* (from this analysis'
# point of view) is time consuming and difficult. To this end we provide a catalog
# function `remove_incomplete()` which takes as an argument a function that operates on
# a pandas dataframe and returns a boolean. Internally, we group the search results and
# then call the user-provided `complete` function on the grouped dataframe and remove
# entries deemed incomplete.


def is_complete(sub_df):
    """Return if this (source_id, member_id, grid_label) has all required variables."""
    missing = 0
    for exp in experiments:
        missing += len(
            set(variables).difference(
                sub_df[sub_df.experiment_id == exp].variable_id.unique()
            )
        )
    if missing:
        return False
    return True


cat.remove_incomplete(is_complete)
print(cat.model_groups().to_string())

# Pick just the numerically smallest variant
cat.remove_ensembles()
print(cat.model_groups().to_string())

# incomplete example

# load the dataset dictionary

# concat these files

# compute iav: remove trend then cycle

# maps of correlation of gpp anomalies with drivers

# member_id/variable_id/experiment_id
