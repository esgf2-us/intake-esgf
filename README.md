[<img width=250px src=https://nvcl.energy.gov/content/images/project/earth-system-grid-federation-2-93.jpg>](https://climatemodeling.science.energy.gov/presentations/esgf2-building-next-generation-earth-system-grid-federation)

*Experimental version under development*

# intake-esgf

A small intake and intake-esm *inspired* package under development in ESGF2.
This package queries a sample index of the replicas hosted at Argonne National
Laboratory and returns the response as a pandas dataframe, mimicing the
interface developed by [intake-esm](https://github.com/intake/intake-esm). As a
user accesses ESGF data, this package will maintain a local cache of files
stored in `${HOME}/.esgf` as well as a log of searches and downloads.
