window.DDBID = window.DDBID || {};
window.DDBID.table = (function() {
    const STATUS_OPTIONS = ['MISSING', 'NEW', 'FOUND', 'ALL'];
    const SECTOR_LABELS = {
        "sec_01": "Archive",
        "sec_02": "Library",
        "sec_03": "Monument preservation",
        "sec_04": "Science",
        "sec_05": "Media library",
        "sec_06": "Museum",
        "sec_07": "Other"
    };
    const providerViewCache = new Map();

    function timestampToRequestValue(value) {
        if (value === null || value === undefined || value === '-1' || value === -1) {
            return '-1';
        }
        return String(value);
    }

    function isTimestampRequestValue(value) {
        if (value === null || value === undefined) {
            return false;
        }

        const text = String(value).trim();
        return text === '-1'
                || /^\d+$/.test(text)
                || /^\d{4}-\d{2}-\d{2}(\s*\(CW\d+\))?$/.test(text)
                || /^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:?\d{2})?$/.test(text);
    }

    function columnFilterInput(columnIndex) {
        return $('#ddbid thead .ddbid-column-filter[data-column-index="' + columnIndex + '"]').first();
    }

    function columnFilterValue(columnIndex) {
        const input = columnFilterInput(columnIndex);
        return input.length ? input.val() : null;
    }

    function setColumnFilterValue(columnIndex, value, readOnly) {
        const input = columnFilterInput(columnIndex);
        if (!input.length) {
            return;
        }
        input.val(value);
        input.prop('readonly', !!readOnly);
    }

    function attachDatalist(columnIndex, datalistId, options) {
        const input = columnFilterInput(columnIndex);
        if (!input.length) {
            return;
        }

        let datalist = $('#' + datalistId);
        if (!datalist.length) {
            datalist = $('<datalist>').attr('id', datalistId);
            $('body').append(datalist);
        }

        datalist.empty();
        options.forEach(function(option) {
            datalist.append($('<option>').attr('value', option.value).attr('label', option.label));
        });
        input.attr('list', datalistId);
    }

    function timestampOptions(entries) {
        const options = [{ value: '-1', label: 'ALL' }];
        entries.forEach(function(entry) {
            const dateMatch = String(entry[0]).match(/^\d{4}-\d{2}-\d{2}/);
            options.push({
                value: dateMatch ? dateMatch[0] : timestampToRequestValue(entry[1]),
                label: entry[0]
            });
        });
        return options;
    }

    function timestampEntryValue(entry) {
        const dateMatch = String(entry[0]).match(/^\d{4}-\d{2}-\d{2}/);
        return dateMatch ? dateMatch[0] : timestampToRequestValue(entry[1]);
    }

    function stringOptions(values) {
        return (values || []).map(function(value) {
            return { value: value, label: value };
        });
    }

    function statusOptions() {
        return STATUS_OPTIONS.map(function(status) {
            return { value: status, label: status };
        });
    }

    function escapeHtml(value) {
        return String(value)
                .replace(/&/g, '&amp;')
                .replace(/</g, '&lt;')
                .replace(/>/g, '&gt;')
                .replace(/"/g, '&quot;')
                .replace(/'/g, '&#39;');
    }

    function institutionName(data) {
        if (!data || typeof data !== 'object') {
            return null;
        }

        if (data['cortex-institution'] && data['cortex-institution'].name) {
            return data['cortex-institution'].name;
        }
        if (data['cortex-institution/name']) {
            return data['cortex-institution/name'];
        }
        if (data.institution && data.institution.name) {
            return data.institution.name;
        }
        if (data['institution/name']) {
            return data['institution/name'];
        }

        for (const value of Object.values(data)) {
            const found = institutionName(value);
            if (found) {
                return found;
            }
        }
        return null;
    }

    function fetchInstitutionName(providerId) {
        if (!providerViewCache.has(providerId)) {
            providerViewCache.set(providerId, fetch('https://api.deutsche-digitale-bibliothek.de/2/items/' + providerId + '/view')
                    .then(function(response) {
                        return response.ok ? response.json() : null;
                    })
                    .then(institutionName)
                    .catch(function() {
                        return null;
                    }));
        }
        return providerViewCache.get(providerId);
    }

    function loadProviderTooltip(link) {
        if (!link || link.dataset.ddbTooltipLoaded || link.dataset.ddbTooltipLoading) {
            return;
        }

        const providerId = link.dataset.ddbProviderId;
        if (!providerId) {
            return;
        }

        link.dataset.ddbTooltipLoading = 'true';
        fetchInstitutionName(providerId).then(function(name) {
            document.querySelectorAll('a[data-ddb-provider-id="' + providerId + '"]').forEach(function(providerLink) {
                if (name) {
                    providerLink.setAttribute('title', name);
                }
                providerLink.dataset.ddbTooltipLoaded = 'true';
                delete providerLink.dataset.ddbTooltipLoading;
            });
        });
    }

    function installProviderTooltips(root) {
        const container = typeof root === 'string' ? document.querySelector(root) : root;
        if (!container) {
            return;
        }

        container.addEventListener('mouseover', function(event) {
            loadProviderTooltip(event.target.closest('a[data-ddb-provider-id]'));
        });
        container.addEventListener('focusin', function(event) {
            loadProviderTooltip(event.target.closest('a[data-ddb-provider-id]'));
        });
    }

    function createColumnFilters() {
        const filterRow = $('<tr>').addClass('column-filter-row');
        $('#ddbid thead tr:first th').each(function(index) {
            const title = $(this).text();
            filterRow.append(
                    $('<th>').append(
                    $('<input>')
                    .attr('type', 'search')
                    .attr('data-column-index', index)
                    .attr('placeholder', 'Contains ' + title)
                    .addClass('form-control form-control-sm ddbid-column-filter')));
        });
        $('#ddbid thead').append(filterRow);
    }

    function applyColumnFilters(d) {
        $('.ddbid-column-filter').each(function() {
            const columnIndex = Number($(this).data('column-index'));
            if (!d.columns || !d.columns[columnIndex]) {
                return;
            }

            if (!d.columns[columnIndex].search) {
                d.columns[columnIndex].search = {};
            }
            d.columns[columnIndex].search.value = $(this).val() || '';
            d.columns[columnIndex].search.regex = false;
            delete d.columns[columnIndex].columnControl;
        });
    }

    function installColumnFilterDebounce(table) {
        const container = table.table().container();
        let timer = null;
        container.addEventListener('input', function(e) {
            if (!e.target.matches('.ddbid-column-filter')) {
                return;
            }

            window.clearTimeout(timer);
            timer = window.setTimeout(function() {
                table.draw();
            }, 650);
        }, true);
    }

    function sortControls() {
        return [{
                "target": 0,
                "content": [
                    "order",
                    {
                        "extend": "dropdown",
                        "text": "Sort",
                        "content": [
                            "orderAsc",
                            "orderDesc",
                            "orderAddAsc",
                            "orderAddDesc",
                            "orderRemove",
                            "orderClear"
                        ]
                    }
                ]
            }];
    }

    function ajaxData(latestTimestampValue) {
        return function(d) {
            const timestampValue = columnFilterValue(0) || latestTimestampValue();
            d.status = columnFilterValue(2) || 'MISSING';
            if (timestampValue) {
                d.timestamp = isTimestampRequestValue(timestampValue) ? timestampValue : '__invalid__';
            }
            applyColumnFilters(d);
            return JSON.stringify(d);
        };
    }

    function loadTimestamps(endpoint, datalistId, setLatestTimestamp, table) {
        $.getJSON(endpoint, function(json) {
            const entries = Object.entries(json || {});
            attachDatalist(0, datalistId, timestampOptions(entries));
            if (entries.length) {
                const latest = timestampEntryValue(entries[entries.length - 1]);
                setLatestTimestamp(latest);
                setColumnFilterValue(0, latest, false);
                table.ajax.reload();
            }
        });
    }

    function installDefaultFilters(table, timestampEndpoint, timestampDatalistId, statusDatalistId, setLatestTimestamp) {
        installColumnFilterDebounce(table);
        loadTimestamps(timestampEndpoint, timestampDatalistId, setLatestTimestamp, table);
        attachDatalist(2, statusDatalistId, statusOptions());
        setColumnFilterValue(2, 'MISSING', false);
    }

    function baseDataTableOptions(ajaxUrl, latestTimestampValue, initComplete, columns) {
        return {
            "dom": 'B<"row mb-3"<"col-12 pb-2"i>><"pb-3 mb-5"r<"table-responsive"t>><"footer fixed-bottom mt-auto py-3 bg-light"<"float-right"p>>',
            "processing": true,
            "serverSide": true,
            "paging": true,
            "colReorder": false,
            "responsive": false,
            "pagingType": "first_last_numbers",
            "searchDelay": 650,
            "lengthMenu": [
                [100, 250, 500, 1000, 2500, 5000],
                [100, 250, 500, 1000, 2500, 5000]
            ],
            "pageLength": 100,
            "titleRow": 0,
            "ajax": {
                "url": ajaxUrl,
                "type": "POST",
                "dataType": "json",
                "contentType": "application/json",
                "data": ajaxData(latestTimestampValue)
            },
            "fixedHeader": true,
            "columnControl": sortControls(),
            "autoWidth": false,
            "initComplete": initComplete,
            "buttons": [
                'pageLength',
                'copy',
                'excel'
            ],
            "columns": columns,
            "createdRow": function(row, data) {
                if (data.status === 'NEW') {
                    $(row).addClass('text-success');
                } else if (data.status === 'MISSING') {
                    $(row).addClass('text-danger');
                }
            }
        };
    }

    function installBackToTop() {
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
    }

    function ddbItemLink(id) {
        const safeId = escapeHtml(id);
        return '<a href="https://www.deutsche-digitale-bibliothek.de/item/' + safeId + '" target="_blank">' + safeId + '</a>';
    }

    function ddbPersonLink(id) {
        const gndId = String(id).substring(String(id).lastIndexOf('/') + 1);
        return '<a href="https://www.deutsche-digitale-bibliothek.de/person/gnd/' + escapeHtml(gndId) + '" target="_blank">' + escapeHtml(id) + '</a>';
    }

    function ddbOrganizationLink(id) {
        const safeId = escapeHtml(id);
        if (String(id).startsWith('http://d-nb.info/gnd/')) {
            const gndId = String(id).substring(String(id).lastIndexOf('/') + 1);
            return '<a href="https://www.deutsche-digitale-bibliothek.de/organization/gnd/' + escapeHtml(gndId) + '" target="_blank">' + safeId + '</a>';
        }
        return '<a href="https://www.deutsche-digitale-bibliothek.de/organization/' + safeId + '" target="_blank">' + safeId + '</a>';
    }

    function ddbProviderLink(providerId) {
        const safeProviderId = escapeHtml(providerId);
        return '<a class="ddbid-provider-link" href="https://www.deutsche-digitale-bibliothek.de/organization/' + safeProviderId + '" target="_blank" rel="noopener noreferrer" data-ddb-provider-id="' + safeProviderId + '">' + safeProviderId + '</a>';
    }

    function renderSector(data, type) {
        if (type === 'display' && data && SECTOR_LABELS[data]) {
            return '<span title="' + escapeHtml(SECTOR_LABELS[data]) + '">' + escapeHtml(data) + '</span>';
        }
        return data;
    }

    return {
        attachDatalist,
        baseDataTableOptions,
        createColumnFilters,
        ddbItemLink,
        ddbOrganizationLink,
        ddbPersonLink,
        ddbProviderLink,
        escapeHtml,
        installBackToTop,
        installDefaultFilters,
        installProviderTooltips,
        renderSector,
        stringOptions
    };
})();
