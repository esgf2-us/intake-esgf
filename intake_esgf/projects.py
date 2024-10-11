"""Supported projects and their facet definitions."""

from abc import ABC, abstractmethod

from intake_esgf.exceptions import ProjectNotSupported


class ESGFProject(ABC):
    """
    A ESGF project base class.

    In order to unify the treatment of projects in intake-esgf, we implement this
    abstract base class which must be implemented for each project. This allows us to
    define how different facets are used by the codebase, and leaves the implementation
    abstract.
    """

    @abstractmethod
    def master_id_facets(self) -> list[str]:
        """
        Return the list of facets defining the master id.

        These exclude version numbers or the data node.
        """
        raise NotImplementedError()

    @abstractmethod
    def id_facets(self) -> list[str]:
        """
        Return the list of facets defining the full id.

        Usually of the form `{master_id}.v{version}|{data_node}."""
        raise NotImplementedError()

    @abstractmethod
    def relaxation_facets(self) -> list[str]:
        """
        Return the facets that may be dropped when searching for cell measures.
        """
        raise NotImplementedError()

    @abstractmethod
    def variable_description_facets(self) -> list[str]:
        """
        Return the facets that describe the specific variable.
        """
        raise NotImplementedError()

    @abstractmethod
    def variable_facet(self) -> str:
        """
        Return the facet name considered to be the `variable`.
        """
        raise NotImplementedError()

    @abstractmethod
    def model_facet(self) -> str:
        """
        Return the facet name considered to be the `model`.
        """
        raise NotImplementedError()

    @abstractmethod
    def variant_facet(self) -> str:
        """
        Return the facet name considered to be the `variant`.
        """
        raise NotImplementedError()

    @abstractmethod
    def grid_facet(self) -> str:
        """
        Return the facet name considered to be the `grid`.
        """
        raise NotImplementedError()

    def modelgroup_facets(self) -> list[str]:
        """
        Return the facets whose unique combinations define model groups.

        In most cases you will not need to implement this function and it uses the
        facets you give above to define a default which is likely correct.
        """
        return [
            f
            for f in [self.model_facet(), self.variant_facet(), self.grid_facet()]
            if f is not None
        ]

    def master_id(self, facets: dict[str, str]) -> str:
        """
        Return the master_id of the dataset using the project facets.

        In most cases, you will not need to implement this function.

        Parameters
        ----------
        facets : dict[str,str]
            A dictionary-like response which contains facets as keys which resolve to
            the values.
        """
        missing = set(self.master_id_facets()) - set(facets)
        if missing:
            raise ValueError(f"Input dict is missing required facets: {missing}")
        return ".".join(
            [
                facets[f][0] if isinstance(facets[f], list) else facets[f]
                for f in self.master_id_facets()
            ]
        )

    def id(self, facets: dict[str]) -> str:
        """
        Return the id (dataset_id) of the dataset using the project facets.

        In most cases, you will not need to implement this function.

        Parameters
        ----------
        facets : dict[str,str]
            A dictionary-like response which contains facets as keys which resolve to
            the values.
        """
        missing = set(self.id_facets()) - set(facets)
        if missing:
            raise ValueError(f"Input dict is missing required facets: {missing}")
        out = self.master_id(facets)
        out += f".v{facets[self.id_facets()[-2]]}|{facets[self.id_facets()[-1]]}"
        return out


class CMIP6(ESGFProject):
    def __init__(self):
        self.facets = [
            "mip_era",
            "activity_drs",
            "institution_id",
            "source_id",
            "experiment_id",
            "member_id",
            "table_id",
            "variable_id",
            "grid_label",
            "version",
            "data_node",
        ]

    def master_id_facets(self) -> list[str]:
        return self.facets[:-2]

    def id_facets(self) -> list[str]:
        return self.facets

    def relaxation_facets(self) -> list[str]:
        return ["member_id", "experiment_id", "activity_drs", "institution_id"]

    def variable_description_facets(self) -> list[str]:
        return ["table_id", "variable_id"]

    def variable_facet(self) -> str:
        return "variable_id"

    def model_facet(self) -> str:
        return "source_id"

    def variant_facet(self) -> str:
        return "member_id"

    def grid_facet(self) -> str:
        return "grid_label"


class CMIP5(ESGFProject):
    def __init__(self):
        self.facets = [
            "institute",
            "model",
            "experiment",
            "time_frequency",
            "realm",
            "cmor_table",
            "ensemble",
            "variable",  # not an official facet but we 'fix' the records on the fly
            "version",
            "data_node",
        ]

    def master_id_facets(self) -> list[str]:
        return self.facets[:-2]

    def id_facets(self) -> list[str]:
        return self.facets

    def relaxation_facets(self) -> list[str]:
        return ["ensemble", "experiment", "institute"]

    def variable_description_facets(self) -> list[str]:
        return ["time_frequency", "realm", "cmor_table", "variable"]

    def variable_facet(self) -> str:
        return "variable"

    def model_facet(self) -> str:
        return "model"

    def variant_facet(self) -> str:
        return "ensemble"

    def grid_facet(self) -> str:
        return None


class CMIP3(ESGFProject):
    def __init__(self):
        self.facets = [
            "project",
            "institute",
            "model",
            "experiment",
            "time_frequency",
            "realm",
            "ensemble",
            "variable",
            "version",
            "data_node",
        ]

    def master_id_facets(self) -> list[str]:
        return self.facets[:-2]

    def id_facets(self) -> list[str]:
        return self.facets

    def relaxation_facets(self) -> list[str]:
        return ["ensemble", "experiment", "institute"]

    def variable_description_facets(self) -> list[str]:
        return ["time_frequency", "realm", "variable"]

    def variable_facet(self) -> str:
        return "variable"

    def model_facet(self) -> str:
        return "model"

    def variant_facet(self) -> str:
        return "ensemble"

    def grid_facet(self) -> str:
        return None


projects = {"cmip6": CMIP6(), "cmip5": CMIP5(), "cmip3": CMIP3()}


def get_project_facets(content: dict[str, str | list[str]]) -> list[str]:
    """
    Return the facets for the project found defined in the given content.

    Parameters
    ----------
    content : dict[str, Union[str, list[str]]]
        Either the search keywords or the index content.

    Returns
    -------
    list[str]
        The facets constituting the id of the project records.
    """
    project_id = content.get("project", None)
    if project_id is None:
        project_id = "CMIP6"
    elif isinstance(project_id, list):
        project_id = project_id[0]
    project_id = str(project_id).lower()
    project = projects.get(project_id, None)
    if project is None:
        raise ProjectNotSupported(project_id)
    return project.id_facets()


def get_likely_project(facets: list | dict) -> str:
    """
    Return the project which is likely to correspond to the given facets.

    Unfortunately, the `project` is not always part of the dataset global attributes.
    This means that if you have a dataset and no other query, you need some logic to
    determine from which project the dataset came. Here we return the project whose
    master_id facets most match the input facets.
    """
    facets = set(facets)
    counts = {
        project_id: len(facets & set(project.master_id_facets()))
        for project_id, project in projects.items()
    }
    return max(counts, key=counts.get)


__all__ = ["projects", "get_project_facets", "get_likely_project"]
