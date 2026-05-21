$(document).ready(function() {
    const tableHelper = window.DDBID.table;

    tableHelper.initEntityTable({
        entity: 'person',
        exactOnlyColumns: [1, 5],
        filterOptions: tableHelper.entityFilterOptions('person', [
            { columnIndex: 3, datalistSuffix: 'VariantOptions', optionKey: 'variant_id' },
            { columnIndex: 5, datalistSuffix: 'TypeOptions', optionKey: 'type' }
        ]),
        columns: tableHelper.personColumns()
    });
});
