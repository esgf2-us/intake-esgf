# Beginner's Guide to ESGF

This guide is targetted at users who are new to obtaining CMIP data from ESGF. While many people work hard to provide the community access in an intuitive fashion, ESGF remains a data source for researchers who have some prior understanding about the data they wish to find and how they are organized. This tutorial is meant to gently expose the uninitiated to key concepts and step you through your first searches using `intake-esgf`.

## Why is it so hard?

Maybe you have some experience with searching for data in ESGF and found it complicated and difficult.

* The control vocabulary we use to describe datasets is technical, carefully chosen, and sometimes not intuitive to the beginner. We will disentangle some of this in this tutorial, but largely the control vocabulary is something the user must learn.
* As the community conducts more phases of the CMIP process, our ideas about what this control vocabulary should be change. This means that while there will be similarities between the vocabulary of CMIP5 to CMIP6, they are not identical and must be learned.
* The data that model centers produce is usually incomplete on some level. Modeling centers budget compute time and personnel to participate in different experiments, but their resources are finite. Not all models will successfully submit all variables for all the variants in all the experiments they run. The user is left to sort through what is there and make the most of it.
* There is no single index that contains all the information about CMIP holdings worldwide. If you want to be certain that you have found everything, you have to search all of the index nodes.
* While the web [interface](https://aims2.llnl.gov/search) will search in a distributed fashion over all index nodes, it will not report when an index has failed to return a response. In our experience, this happens often and can leave you with an impression that you have found everything there is, but in fact have not.
* In order to reduce download times for users around the globe, some datasets are replicated to many different locations. When you use a web [interface](https://aims2.llnl.gov/search) to search, you may find many instances of the same dataset just stored in a different location. This leads to many search results to sort through and can cause some ambiguity of what should be selected.

We have designed `intake-esgf` to hide as much of this complexity as we can to make the experience better for the user.

## CMIP6 Control Vocabulary

At the highest level, ESGF stores data in *projects* such as `CMIP5` and `CMIP6`. While there are some similarities, the *control vocabulary*, that is the metadata used to identify unique datasets, varies. In the following we will explain some of this vocabulary by starting with the more relevant terms.

* `experiment_id` - The identifier of the experiment. As part of the planning phase of the CMIP process, groups of researchers can write a paper detailing a specific method that a model is to be run. This allows modeling centers to read the paper and follow the protocol if they wish to be part of the experiment. You can browse the experiments [here](https://wcrp-cmip.github.io/CMIP6_CVs/docs/CMIP6_experiment_id.html) to see the indentifiers and some basic information.
* `source_id` - The identifier of the model. We use the term *source* instead of *model* in an attempt to make the control vocabular more general and in the future unify vocabularies among projects. Each model or model version will have a unique string identifying which model and/or configuration was run. [here](https://wcrp-cmip.github.io/CMIP6_CVs/docs/CMIP6_source_id.html)
* `variable_id` - The identifier of the variable.
