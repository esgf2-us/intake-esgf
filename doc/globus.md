---
jupytext:
  text_representation:
    format_name: myst
kernelspec:
  display_name: Python 3
  name: python3
---

# Globus Transfers

```{warning}
This page describes a feature that we are currently testing. The interface is not yet well defined and may change. Some cells will not render as they depend on user authentication and personal information. If you have input on how to improve this interface, please reach [out](https://github.com/esgf2-us/intake-esgf/issues).
```

## Setting Up

Chances are if you are reading this, you are already familiar with [Globus](https://www.globus.org/). In case that you are not, ​Globus is research cyberinfrastructure, developed so you can easily, reliably and securely move, share, & discover data no matter where it lives – from a supercomputer, lab cluster, tape archive, public cloud or laptop.

A portion of the ESGF data archive is stored in public [Guest Collections](https://docs.globus.org/globus-connect-server/v5/reference/collection/) and access information included in some ESGF index nodes. This means that a portion of the ESGF archive can be accessed using Globus Transfer. These transfers can be triggered seamlessly in `intake-esgf` if you satisfy a few requirements. You will need:

1. A Globus login. In order to manage permissions, Globus requires an identity. Simply try to login at [https://www.globus.org/](https://www.globus.org/) and first look through the list of supported institutions to login with your credentials. If your institution is not listed, you may login with another option list below the institution pulldown.
2. A place to send data. Globus transfer uses custom software to both send and receive data. In their parlance, you need write access to another *collection* which represents where you will send the data. It is possible to download to your personal computer. You will need to download [Globus Connect Personal](https://app.globus.org/collections/gcp) and have it running and connected when you initiate the transfer.
3. The `UUID` of the destination collection. The `UUID` can be found by navigating to the [collection](https://app.globus.org/file-manager/collections) in Globus, clicking the `⋮` to show the collection properties, and copying the `UUID` value listed.

## Initiating the Transfer

In this case we will use the [configuration](configure) options to only query the `anl-dev` globus-based index. This is just to keep the example script simple. There is Globus transfer information in several of the indices. For demonstration, we will search for a few files for a model whose file sizes are smaller.

```{code-cell}
:tags: [remove-cell]
import intake_esgf
from intake_esgf import ESGFCatalog
```

```{code-cell}
with intake_esgf.conf.set(indices={"ornl-dev": False}):
    cat = ESGFCatalog()
    cat.search(
        experiment_id="historical",
        source_id="CanESM5",
        frequency="mon",
        variable_id=[
            "pr",
            "tas",
            "gpp",
        ],
        member_id="r1i1p1f1",
    )
```

This portion of the process what you would do normally. To use globus transfers where possible, you need to include additional arguments to `to_dataset_dict()`. The first is `globus_endpoint`, the `UUID` of the destination collection to which you will transfer the data. The second is `globus_path`, any additional path you wish to add to the root path of the destination collection.

```{code-cell}
:tags: [skip-execution]
dsd = cat.to_dataset_dict(
    globus_endpoint=COLLECTION_UUID, # <-- your data here
    globus_path="data/ESGF-Data", # <-- additional path
)
```

Internally this will do several things:

1. We use the datasets present in your catalog to query the indices again for file information. This information is partitioned into that which has an associated Globus collection and that for which we will need to use https to download. The information that has a Globus collection is further partitioned preferring the collection with the fastest transfer times for you.
2. We remove the file information which we detect is already present in the local cache.
3. We submit the Globus transfer(s) and log the `task_id` to the `intake-esgf` [logfile](logging). You will not see anything on the screen to indicate that the transfer is ongoing, but can monitor the progress by going to your [activity](https://app.globus.org/activity) on globus.org.
4. Once the the Globus transfer(s) are underway, we will download the remaining files using https.
5. After the https downloads have completed, we block further progress until the Globus transfers report that they have succeeded.

Then the code progresses as usual, looking for cell measures and loading files into xarray Datasets.

## What Can Go Wrong?

While this interface maintains the `intake-esgf` paradigm, and also makes using Globus transfers very simple, it also poses a few difficulties.

1. Your `local_cache` may not include the location to where you transfered data. Technically you can use this approach to transfer data to any collection, not necesarily to the resource on which you are working.
2. Furthermore, even if you execute this script on the resource which corresponds to the `globus_endpoint` you provided to `to_dataset_dict()`, the local cache directory may not point to where Globus transferred data. So if the `collection_root = /home/username/` and you set your local cache to `/home/username/data/ESGF-Data`, then you should have given `to_dataset_dict()` the option `globus_path="data/ESGF-Data"`. However, the collection root is not something we can always query and thus have no way to verify.

Please [tell](https://github.com/esgf2-us/intake-esgf/issues) us what you think of this interface.
