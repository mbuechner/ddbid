$(document).ready(function() {
    $('#ddbid').DataTable({
        "dom": 'B<"row mb-3"<"col-12 col-md-6 pb-2"i><"col-12 col-md-6 pb-2"f>><"pb-3 mb-5"r<"table-responsive"t>><"footer fixed-bottom mt-auto py-3 bg-light"<"float-right"p>>',
        "processing": true,
        "serverSide": true,
        "paging": true,
        "colReorder": true,
        "responsive": false,
        "pagingType": "first_last_numbers",
        "lengthMenu": [
            [5, 10, 25, 50, 100, 250, 500, -1],
            [5, 10, 25, 50, 100, 250, 500, "All"]
        ],
        "pageLength": 100,
        // "language": {
        //  url: "/assets/datatables/de-DE.json",
        //  decimal: ","
        // },
        "ajax": {
            "url": "person",
            "type": "POST",
            "dataType": "json",
            "contentType": "application/json",
            "data": function(d) {
                d.status = $('#statusBtn').data('id');
                d.timestamp = $('#timestampBtn').data('id');
                return JSON.stringify(d);
            }
        },
        "fixedHeader": true,
        "autoWidth": true,
        "initComplete": function(oSettings) {
            $.getJSON("person/timestamp", function(json) {
                $('#ddbid').DataTable().button().add("1-0", {
                    text: 'ALL',
                    className: "timestampBtnEntry",
                    attr: {
                        "id": "timestampBtnEntry-ALL",
                        "data-id": '-1'
                    },
                    action: function(e, dt, button, config) {
                        $('#timestampBtn').data('id', '-1');
                        $('.timestampBtnEntry').removeClass('active');
                        button.addClass('active');
                        $('#ddbid').DataTable().ajax.reload();
                    }
                });
                var count = 0;
                const oEntries = Object.entries(json)
                for (const [key, value] of oEntries) {
                    $('#ddbid').DataTable().button().add("1-0", {
                        text: key,
                        className: (++count === oEntries.length) ? "timestampBtnEntry active" : "timestampBtnEntry",
                        attr: {
                            "id": "timestampBtnEntry-" + value,
                            "data-id": value
                        },
                        action: function(e, dt, button, config) {
                            $('#timestampBtn').data('id', $('#timestampBtnEntry-' + value).data('id'));
                            $('.timestampBtnEntry').removeClass('active');
                            button.addClass('active');
                            $('#ddbid').DataTable().ajax.reload();
                        }
                    });
                }
            });
            $('#overlay').css("visibility", "hidden");
            $('#overlayBg').removeAttr("class");
        },
        buttons: [
            'pageLength',
            // 'colvis',
            {
                extend: 'collection',
                autoClose: true,
                text: 'Timestamp',
                attr: {
                    id: 'timestampBtn'
                },
                className: 'timestampBtn',
                buttons: []
            },
            {
                extend: 'collection',
                autoClose: true,
                text: 'Status',
                attr: {
                    id: 'statusBtn'
                },
                buttons: [{
                        text: 'MISSING',
                        className: 'statusBtn active',
                        attr: {
                            id: 'statusBtnMissing'
                        },
                        action: function(e, dt, button, config) {
                            $('.statusBtn').removeClass('active');
                            $('#statusBtnMissing').addClass('active');
                            $('#statusBtn').data('id', 'MISSING');
                            $('#ddbid').DataTable().ajax.reload();
                        }
                    },
                    {
                        text: 'NEW',
                        className: 'statusBtn',
                        attr: {
                            id: 'statusBtnNew'
                        },
                        action: function(e, dt, node, config) {
                            $('.statusBtn').removeClass('active');
                            $('#statusBtnNew').addClass('active');
                            $('#statusBtn').data('id', 'NEW');
                            $('#ddbid').DataTable().ajax.reload();
                        }
                    },
                    {
                        text: 'ALL',
                        className: 'statusBtn',
                        attr: {
                            id: 'statusBtnAll'
                        },
                        action: function(e, dt, node, config) {
                            $('.statusBtn').removeClass('active');
                            $('#statusBtnAll').addClass('active');
                            $('#statusBtn').data('id', 'ALL');
                            $('#ddbid').DataTable().ajax.reload();
                        }
                    }
                ]
            },
            'copy', 'excel'
        ],
        "columns": [{
                "data": "timestamp",
                "className": "text-nowrap"
            },
            {
                "data": "id",
                "className": "text-nowrap",
                "render": function(data, type, row, meta) {
                    if (type === 'display') {
                        data = '<a href="https://www.deutsche-digitale-bibliothek.de/person/gnd/' + data.substring(data.lastIndexOf('/') + 1, data.lenght) + '" target="_blank">' + data + '</a>';
                    }
                    return data;
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
        ],
        "createdRow": function(row, data, dataIndex) {
            if (data['status'] === 'NEW') {
                $(row).addClass('text-success');
            } else if (data['status'] === 'MISSING') {
                $(row).addClass('text-danger');
            }
        }
    });

    $(function() {
        $(window).scroll(function() {
            if ($(this).scrollTop() > 100) {
                $('#btn-back-to-top').fadeIn();
            } else {
                $('#btn-back-to-top').fadeOut();
            }
        });

        $('#btn-back-to-top').click(function() {
            $('body,html').animate({
                scrollTop: 0
            }, 800);
            return false;
        });
    });
});
