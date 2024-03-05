---
jupytext:
  text_representation:
    format_name: myst
kernelspec:
  display_name: Python 3
  name: python3
---

# Simplifying Search with Model Groups

At a simple level, you can think of `intake-esgf` as analagous to the ESGF web [interface](https://aims2.llnl.gov/search) but where results are presented to you as a pandas dataframe in place of pages of web results. However, we believe that the user does not want to wade through either of these. Many times you want to see model results organized by unique combinations of `source_id`, `member_id`, and `grid_label`. That is to say, when you are going to perform an analysis, you would like your model outputs to be self-consistent and from the same run and grid, even across experiments. To assist you in honing in on what sets of results may be useful to your analysis, we introduce the notion of *model groups*.

Consider the following search, motivated by a desire to study controls (temperature, precipitation, soil moisture) on the carbon cycle (gross primary productivty) across a number of historical and future scenarios.

```{code-cell}
from intake_esgf import ESGFCatalog
cat = ESGFCatalog().search(
    experiment_id=["historical", "ssp585", "ssp370", "ssp245", "ssp126"],
    variable_id=["gpp", "tas", "pr", "mrso"],
    table_id=["Amon", "Lmon"],
)
print(cat)
```

Even if this exact application does not resonate with you, the situation is a familiar one. We have several thousand results with many different models and variants to sort through. To help guide you to which groups of models might be useful to you, we provide the following function.

```{code-cell}
cat.model_groups()
```

This returns a pandas series where the results have been grouped and sorted by `source_id`, `member_id`, and `grid_label` and the counts of datasets returned. Pandas will probably truncate this series. If you want to see the whole series, you can call `print(cat.model_groups().to_string())` instead. However, as there are still several hundred possibile model groups, we will not show that here.

## Removing Incomplete Groups

If you glance through the model groups, you will see that, relative to our search, many will be *incomplete*. By this we mean, that there are many model groups that will not have all the variables in all the experiments that we wish to include in our analysis. Since we are looking for 5 experiments and 4 variables, we need the model groups with 20 dataset results. We can check which groups satisfy this condition by operating on the model group pandas series.

```{code-cell}
mgs = cat.model_groups()
print(mgs[mgs==20])
```

The rest are incomplete and we would like a fast way to remove them from the search results. But the reality is that many times our *completeness* criteria is more complicated than just a number. In the above example, we may want all the variables for all the experiments, but if a model does not have a submission for, say, `ssp126`, that is acceptable.

`intake-esgf` provides an interface which uses a user-provided function to remove incomplete entries. Internally, we will loop over all model groups in the results and pass your function the portion of the dataframe that corresponds to the current model group. Your function then needs to return a boolean based on the contents of that sub-dataframe.

```{code-cell}
def should_i_keep_it(sub_df):
    # this model group has all experiments/variables
    if len(sub_df) == 20:
        return True
    # if any of these experiments is missing a variable, remove this
    for exp in ["historical", "ssp585", "ssp370", "ssp245"]:
        if len(sub_df[sub_df["experiment_id"] == exp]) != 4:
            return False
    # if the check makes it here, keep it
    return True
```

Then we pass this function to the catalog by the `remove_incomplete()` function and observe how it has impacted the search results.

```{code-cell}
cat.remove_incomplete(should_i_keep_it)
print(cat.model_groups())
```

## Removing Ensembles

Depending on the goals and scope of your analysis, you may want to use only a single variant per model. This can be challenging to locate as not all variants have all the experiments and models. However, now that we have removed the incomplete results, we can now call the `remove_ensembles()` function which will only keep the *smallest* `member_id` for each model group. By smallest, we mean that first entry after a hierarchical sort using the integer index values of each label in the `member_id`.

```{code-cell}
cat.remove_ensembles()
print(cat.model_groups())
```

Now the results are much more manageable and ready to be downloaded for use in your analysis.

## Feedback

What do you [think](https://github.com/esgf2-us/intake-esgf/issues/new?assignees=&labels=&projects=&template=feature_request.md&title=) of this interface? We have found that it saves our students days of work, but are interested in critical feedback. Can you think of simpler interface? Are there other analysis tasks that are painful and time consuming that we could automate?
