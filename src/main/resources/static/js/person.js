$(document).ready(function() {
    let latestTimestampValue = null;
    const tableHelper = window.DDBID.table;

    tableHelper.createColumnFilters();
    $("#ddbid_processing").addClass("alert alert-secondary");

    $('#ddbid').DataTable(tableHelper.baseDataTableOptions(
            'person',
            function() {
                return latestTimestampValue;
            },
            function() {
                const table = this.api();
                tableHelper.installDefaultFilters(
                        table,
                        'person/timestamp',
                        'personTimestampOptions',
                        'personStatusOptions',
                        function(value) {
                            latestTimestampValue = value;
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
                        return type === 'display' && data ? tableHelper.ddbPersonLink(data) : data;
                    }
                },
                {
                    "data": "status",
                    "className": "text-nowrap"
                },
                {
                    "data": "variant_id",
                    "className": "text-wrap"
                },
                {
                    "data": "preferredName",
                    "className": "text-wrap"
                },
                {
                    "data": "type",
                    "className": "text-nowrap"
                }
            ]));

    tableHelper.installBackToTop();
});
