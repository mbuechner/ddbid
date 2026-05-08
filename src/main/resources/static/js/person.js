$(document).ready(function() {
    const tableHelper = window.DDBID.table;

    tableHelper.initEntityTable({
        entity: 'person',
        filterOptions: tableHelper.entityFilterOptions('person', [
            { columnIndex: 3, datalistSuffix: 'VariantOptions', optionKey: 'variant_id' },
            { columnIndex: 5, datalistSuffix: 'TypeOptions', optionKey: 'type' }
        ]),
        columns: tableHelper.personColumns()
    });
});
