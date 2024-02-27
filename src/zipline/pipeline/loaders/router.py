
from zipline.pipeline.data import TQAltData, TQData

from zipline.pipeline.loaders import TQuantFundamentalsPipelineLoader, TQuantAlternativesPipelineLoader


class TQuantLabPipelineLoaderRouter:
    """
    Routes to PipelineLoaders.
    """
    def __init__(self, sids_to_real_sids, calendar, default_loader, default_loader_columns):

        # Default
        self.default_loader = default_loader
        self.default_loader_columns = [col.unspecialize() for col in default_loader_columns]

        # TQ Financial Data
        self.tq_fin_loader = TQuantFundamentalsPipelineLoader(
            sids_to_real_sids)

        # TQ Alternative Data
        self.tq_alt_loader = TQuantAlternativesPipelineLoader(
            sids_to_real_sids)
    

    def isin(self, column, dataset):
        """
        Checks dataset membership regardless of specialization.
        """
        return column in dataset.columns or column.unspecialize() in dataset.columns

    def __call__(self, column):
        if column.unspecialize() in self.default_loader_columns:
            return self.default_loader

        # TQ Financial Data
        elif (
            hasattr(column.dataset, "dataset_family")
            and column.dataset.dataset_family == TQData):
            return self.tq_fin_loader
        
        # TQ Alternative Data
        elif (
            hasattr(column.dataset, "dataset_family")
            and column.dataset.dataset_family == TQAltData):
            return self.tq_alt_loader
        

        raise ValueError(
            "No PipelineLoader registered for column %s." % column.unspecialize()
        )