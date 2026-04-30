$(document).ready(function() {
    let latestTimestampValue = null;
    const tableHelper = window.DDBID.table;

    tableHelper.createColumnFilters();
    tableHelper.installProviderTooltips(document.getElementById('ddbid'));
    $("#ddbid_processing").addClass("alert alert-secondary");

    $('#ddbid').DataTable(tableHelper.baseDataTableOptions(
            'item',
            function() {
                return latestTimestampValue;
            },
            function() {
                const table = this.api();
                tableHelper.installDefaultFilters(
                        table,
                        'item/timestamp',
                        'itemTimestampOptions',
                        'itemStatusOptions',
                        function(value) {
                            latestTimestampValue = value;
                        });

                $.getJSON('item/filter-options', function(json) {
                    json = json || {};
                    tableHelper.attachDatalist(2, 'itemStatusOptions', tableHelper.stringOptions(json.status || ['MISSING', 'NEW', 'FOUND', 'ALL']));
                    tableHelper.attachDatalist(4, 'itemDatasetOptions', tableHelper.stringOptions(json.dataset_id));
                    tableHelper.attachDatalist(6, 'itemProviderOptions', tableHelper.stringOptions(json.provider_id));
                    tableHelper.attachDatalist(7, 'itemSectorOptions', tableHelper.stringOptions(json.sector_fct));
                    tableHelper.attachDatalist(8, 'itemSupplierOptions', tableHelper.stringOptions(json.supplier_id));
                });
            },
            [{
                    "data": "timestamp",
                    "className": "text-nowrap"
                },
                {
                    "data": "id",
                    "className": "text-nowrap",
                    "render": function(data, type) {
                        return type === 'display' && data ? tableHelper.ddbItemLink(data) : data;
                    }
                },
                {
                    "data": "status",
                    "className": "text-nowrap"
                },
                {
                    "data": "provider_item_id",
                    "className": "text-nowrap"
                },
                {
                    "data": "dataset_id",
                    "className": "text-wrap"
                },
                {
                    "data": "label",
                    "className": "text-wrap"
                },
                {
                    "data": "provider_id",
                    "className": "text-wrap",
                    "render": function(data, type) {
                        if (type === 'display' && data) {
                            return data.replace(/([A-Z0-9]{32})/g, function(match, providerId) {
                                return tableHelper.ddbProviderLink(providerId);
                            });
                        }
                        return data;
                    }
                },
                {
                    "data": "sector_fct",
                    "className": "text-nowrap",
                    "render": tableHelper.renderSector
                },
                {
                    "data": "supplier_id",
                    "className": "text-wrap"
                }
            ]));

    tableHelper.installBackToTop();
});
