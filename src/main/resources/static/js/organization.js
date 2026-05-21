$(document).ready(function() {
    const tableHelper = window.DDBID.table;

    tableHelper.initEntityTable({
        entity: 'organization',
        exactOnlyColumns: [1, 5],
        filterOptions: tableHelper.entityFilterOptions('organization', [
            { columnIndex: 3, datalistSuffix: 'VariantOptions', optionKey: 'variant_id' },
            { columnIndex: 5, datalistSuffix: 'TypeOptions', optionKey: 'type' }
        ]),
        columns: tableHelper.organizationColumns()
    });
});
