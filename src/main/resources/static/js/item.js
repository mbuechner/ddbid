$(document).ready(function() {
    const tableHelper = window.DDBID.table;

    tableHelper.initEntityTable({
        entity: 'item',
        providerTooltips: true,
        filterOptions: tableHelper.entityFilterOptions('item', [
            { columnIndex: 4, datalistSuffix: 'DatasetOptions', optionKey: 'dataset_id' },
            { columnIndex: 6, datalistSuffix: 'ProviderOptions', optionKey: 'provider_id' },
            { columnIndex: 7, datalistSuffix: 'SectorOptions', optionKey: 'sector_fct' },
            { columnIndex: 8, datalistSuffix: 'SupplierOptions', optionKey: 'supplier_id' }
        ]),
        columns: tableHelper.itemColumns()
    });
});
