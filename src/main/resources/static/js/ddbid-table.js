window.DDBID = window.DDBID || {};
window.DDBID.table = (function() {
    const JSON_CACHE_TTL_MILLIS = 600000;
    const JSON_CACHE_PREFIX = 'ddbid-json-cache:';
    const STATUS_OPTIONS = ['MISSING', 'NEW', 'FOUND', 'ALL'];
    const FILTER_MODE_DEFAULT = 'equal';
    const FILTER_MODE_ALLOWED = new Set(['equal', 'contains']);
    const FILTER_MODE_EXACT_ONLY = new Set(['equal']);
    const columnModeConfig = new Map();
    const REQUEST_DRAW_DELAY_MILLIS = 350;
    const pendingDatalists = new Map();
    const pendingFilterState = new Map();
    let pendingControlSetupTimer = null;
    let pendingDrawTimer = null;
    const SECTOR_LABELS = {
        "sec_01": "Archive",
        "sec_02": "Library",
        "sec_03": "Monument protection",
        "sec_04": "Science",
        "sec_05": "Media",
        "sec_06": "Museum",
        "sec_07": "Other"
    };
    const providerViewCache = new Map();

    function timestampToRequestValue(value) {
        if (value === null || value === undefined || value === '') {
            return '';
        }
        if (value === '-1' || value === -1) {
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
        const headerRow = $('#ddbid thead tr').eq(1);
        if (!headerRow.length) {
            return $();
        }
        return headerRow.children('th, td').eq(columnIndex).find('.dtcc-search input').first();
    }

    function columnFilterModeInput(columnIndex) {
        const headerRow = $('#ddbid thead tr').eq(1);
        if (!headerRow.length) {
            return $();
        }
        return headerRow.children('th, td').eq(columnIndex).find('.dtcc-search select').first();
    }

    function columnFilterValue(columnIndex) {
        const input = columnFilterInput(columnIndex);
        return input.length ? input.val() : null;
    }

    function columnFilterMode(columnIndex) {
        const input = columnFilterModeInput(columnIndex);
        return input.length ? input.val() : null;
    }

    function filterState(columnIndex) {
        const existing = pendingFilterState.get(columnIndex) || {};
        if (!pendingFilterState.has(columnIndex)) {
            pendingFilterState.set(columnIndex, existing);
        }
        return existing;
    }

    function normalizeFilterMode(mode) {
        return FILTER_MODE_ALLOWED.has(mode) ? mode : FILTER_MODE_DEFAULT;
    }

    function columnIndexFromElement(element) {
        const current = $(element);
        const directIndex = current.data('column-index');
        if (directIndex !== undefined) {
            return Number(directIndex);
        }

        const cell = current.closest('th, td');
        if (!cell.length) {
            return -1;
        }
        return cell.index();
    }

    function setInputLoadingState(input, isLoading, loadingPlaceholder) {
        if (!input || !input.length) {
            return;
        }

        const originalPlaceholder = input.data('ddbid-original-placeholder');
        if (isLoading) {
            if (originalPlaceholder === undefined) {
                input.data('ddbid-original-placeholder', input.attr('placeholder') || '');
            }
            input.attr('placeholder', loadingPlaceholder || 'Loading suggestions...');
            input.addClass('is-loading');
            input.attr('aria-busy', 'true');
        } else {
            if (originalPlaceholder !== undefined) {
                input.attr('placeholder', originalPlaceholder);
            }
            input.removeClass('is-loading');
            input.removeAttr('aria-busy');
        }
    }

    function setColumnFiltersLoading(columnIndices, isLoading, loadingPlaceholder) {
        (columnIndices || []).forEach(function(columnIndex) {
            setInputLoadingState(columnFilterInput(columnIndex), isLoading, loadingPlaceholder);
        });
    }

    function setColumnFilterValue(columnIndex, value, readOnly, triggerSearch) {
        const input = columnFilterInput(columnIndex);
        if (!input.length) {
            return;
        }
        input.val(value);
        input.prop('readonly', !!readOnly);
        if (triggerSearch) {
            input.trigger('input');
            input.trigger('change');
            requestTableDraw();
        }
    }

    function requestTableDraw() {
        if (pendingDrawTimer !== null) {
            window.clearTimeout(pendingDrawTimer);
        }
        pendingDrawTimer = window.setTimeout(function() {
            pendingDrawTimer = null;
            if ($.fn.dataTable.isDataTable('#ddbid')) {
                $('#ddbid').DataTable().draw(false);
            }
        }, REQUEST_DRAW_DELAY_MILLIS);
    }

    function applyPendingDatalists() {
        pendingDatalists.forEach(function(config, columnIndex) {
            const input = columnFilterInput(columnIndex);
            if (!input.length) {
                return;
            }

            let datalist = $('#' + config.datalistId);
            if (!datalist.length) {
                datalist = $('<datalist>').attr('id', config.datalistId);
                $('body').append(datalist);
            }

            const serializedOptions = JSON.stringify(config.options || []);
            if (datalist.attr('data-ddbid-options') !== serializedOptions) {
                datalist.empty();
                config.options.forEach(function(option) {
                    datalist.append($('<option>').attr('value', option.value).attr('label', option.label));
                });
                datalist.attr('data-ddbid-options', serializedOptions);
            }

            if (input.attr('list') !== config.datalistId) {
                input.attr('list', config.datalistId);
            }
        });
    }

    function applyPendingFilterState() {
        pendingFilterState.forEach(function(config, columnIndex) {
            const input = columnFilterInput(columnIndex);
            const modeInput = columnFilterModeInput(columnIndex);
            if (!input.length || !modeInput.length) {
                return;
            }

            restrictFilterModeOptions(columnIndex);

            if (config.mode) {
                setColumnFilterMode(columnIndex, config.mode, false);
            }

            if (config.value !== undefined) {
                if (config.forceValue) {
                    setColumnFilterValue(columnIndex, config.value, !!config.readOnly, !!config.triggerSearch);
                    config.forceValue = false;
                    config.triggerSearch = false;
                } else if (input.val() !== config.value) {
                    input.val(config.value);
                }
            }

            input.prop('readonly', !!config.readOnly);

            updateColumnFilterPlaceholder(columnIndex);
        });
    }

    function applyPendingControlSetup() {
        applyPendingDatalists();
        applyPendingFilterState();
    }

    function controlsPresent(columnCount) {
        const headerRow = $('#ddbid thead tr').eq(1);
        if (!headerRow.length) {
            return false;
        }

        const inputs = headerRow.find('.dtcc-search input').length;
        const selects = headerRow.find('.dtcc-search select').length;
        return inputs >= columnCount && selects >= columnCount;
    }

    function clearPendingControlSetupTimer() {
        if (pendingControlSetupTimer !== null) {
            window.clearTimeout(pendingControlSetupTimer);
            pendingControlSetupTimer = null;
        }
    }

    function scheduleControlSetup(columnCount, attempts) {
        const remainingAttempts = attempts === undefined ? 30 : attempts;
        clearPendingControlSetupTimer();

        const run = function() {
            pendingControlSetupTimer = null;
            applyPendingControlSetup();

            if (controlsPresent(columnCount) || remainingAttempts <= 0) {
                return;
            }

            pendingControlSetupTimer = window.setTimeout(function() {
                scheduleControlSetup(columnCount, remainingAttempts - 1);
            }, 100);
        };

        if (window.requestAnimationFrame) {
            window.requestAnimationFrame(run);
            return;
        }

        pendingControlSetupTimer = window.setTimeout(run, 0);
    }

    function attachDatalist(columnIndex, datalistId, options) {
        pendingDatalists.set(columnIndex, { datalistId: datalistId, options: options || [] });
        scheduleControlSetup($('#ddbid thead tr:first th').length || columnIndex + 1, 5);
    }

    function columnControlIcons() {
        const icons = window.DataTable && window.DataTable.ColumnControl && window.DataTable.ColumnControl.icons;
        return icons && typeof icons === 'object' ? icons : null;
    }

    function updateColumnFilterPlaceholder(columnIndex) {
        const input = columnFilterInput(columnIndex);
        if (!input.length) {
            return;
        }

        const title = columnTitle(columnIndex);
        const mode = columnFilterMode(columnIndex) || FILTER_MODE_DEFAULT;
        input.attr('placeholder', (mode === 'equal' ? 'Exact ' : 'Contains ') + title);
    }

    function updateColumnFilterModeIcon(columnIndex) {
        const modeInput = columnFilterModeInput(columnIndex);
        if (!modeInput.length) {
            return;
        }

        const icon = modeInput.closest('.dtcc-search').find('.dtcc-search-type-icon').first();
        if (!icon.length) {
            return;
        }

        const mode = normalizeFilterMode(modeInput.val());
        const icons = columnControlIcons();
        const iconMarkup = icons && icons[mode] ? icons[mode] : null;
        if (!iconMarkup) {
            return;
        }
        if (icon.html() !== iconMarkup) {
            icon.html(iconMarkup);
        }
        const selectedOption = modeInput.find('option:selected').text();
        icon.attr('title', selectedOption || (mode === 'equal' ? 'Equals' : 'Contains'));
    }

    function rememberColumnFilterState(columnIndex, userTriggeredModeChange) {
        if (columnIndex < 0) {
            return;
        }

        const state = filterState(columnIndex);
        const mode = normalizeFilterMode(columnFilterMode(columnIndex) || state.mode);
        const value = columnFilterValue(columnIndex);

        if (userTriggeredModeChange || state.userSelectedMode) {
            state.mode = mode;
        }
        if (value !== null) {
            state.value = value;
        }
        if (userTriggeredModeChange) {
            state.userSelectedMode = true;
        }
        pendingFilterState.set(columnIndex, state);

        updateColumnFilterModeIcon(columnIndex);
        updateColumnFilterPlaceholder(columnIndex);
    }

    function installFilterStatePersistence() {
        const thead = $('#ddbid thead');
        if (!thead.length || thead.data('ddbid-filter-state-persistence')) {
            return;
        }

        thead.data('ddbid-filter-state-persistence', true);
        thead.on('input.ddbidFilterState change.ddbidFilterState', '.dtcc-search input', function() {
            rememberColumnFilterState(columnIndexFromElement(this), false);
        });
        thead.on('change.ddbidFilterState input.ddbidFilterState', '.dtcc-search select', function() {
            rememberColumnFilterState(columnIndexFromElement(this), true);
        });
    }

    function columnTitle(columnIndex) {
        const header = $('#ddbid thead tr:first th').eq(columnIndex);
        const title = header.find('.dt-column-title').first().text().trim();
        return title || header.clone().children().remove().end().text().trim();
    }

    function activeFilterDetails() {
        const details = [];
        const columnCount = $('#ddbid thead tr:first th').length;
        for (let columnIndex = 0; columnIndex < columnCount; columnIndex += 1) {
            const value = (columnFilterValue(columnIndex) || '').trim();
            if (!value) {
                continue;
            }

            if (columnIndex === 0) {
                details.push(columnTitle(columnIndex) + ': ' + value);
                continue;
            }

            if (columnIndex === 2) {
                details.push(columnTitle(columnIndex) + ': ' + value);
                continue;
            }

            const mode = columnFilterMode(columnIndex) || FILTER_MODE_DEFAULT;
            details.push(columnTitle(columnIndex) + ' ' + (mode === 'equal' ? 'is exactly' : 'contains') + ' ' + value);
        }
        return details;
    }

    function applyColumnControlPayload(d) {
        if (!d || !Array.isArray(d.columns)) {
            return;
        }

        d.columns.forEach(function(column, columnIndex) {
            if (!column) {
                return;
            }

            rememberColumnFilterState(columnIndex, false);

            const state = filterState(columnIndex);
            const value = ((columnFilterValue(columnIndex) || state.value || '') + '').trim();
            const logic = normalizeFilterMode(columnFilterMode(columnIndex) || state.mode);

            if (!value) {
                delete column.columnControl;
                return;
            }

            column.columnControl = {
                search: {
                    value: value,
                    logic: logic,
                    type: 'text'
                }
            };
        });
    }

    function setColumnFilterMode(columnIndex, mode, triggerSearch) {
        const input = columnFilterModeInput(columnIndex);
        if (!input.length) {
            return;
        }

        const normalizedMode = normalizeFilterMode(mode);

        if (!input.find('option[value="' + normalizedMode + '"]').length) {
            return;
        }

        if (input.val() === normalizedMode) {
            return;
        }

        input.val(normalizedMode);
        input.trigger('input');
        if (triggerSearch) {
            input.trigger('change');
            requestTableDraw();
        }
    }

    function restrictFilterModeOptions(columnIndex) {
        const input = columnFilterModeInput(columnIndex);
        if (!input.length) {
            return;
        }

        const allowed = columnModeConfig.get(columnIndex) || FILTER_MODE_ALLOWED;
        input.find('option').each(function() {
            const option = $(this);
            const value = option.attr('value') || '';
            if (!allowed.has(value)) {
                option.remove();
            } else if (value === 'equal') {
                option.text('Equals');
            } else if (value === 'contains') {
                option.text('Contains');
            }
        });

        if (!input.find('option[value="' + FILTER_MODE_DEFAULT + '"]').length && input.find('option').length) {
            input.val(input.find('option').first().attr('value'));
        }

        updateColumnFilterModeIcon(columnIndex);
    }

    function configureColumnModes(exactOnlyIndices) {
        (exactOnlyIndices || []).forEach(function(index) {
            columnModeConfig.set(index, FILTER_MODE_EXACT_ONLY);
        });
    }

    function setupColumnControlSearchModes(columnCount) {
        for (let index = 0; index < columnCount; index += 1) {
            const state = filterState(index);
            pendingFilterState.set(index, Object.assign({}, state, {
                mode: state.userSelectedMode ? normalizeFilterMode(state.mode) : FILTER_MODE_DEFAULT
            }));
        }

        applyPendingFilterState();
    }

    function hasContainsFilter(details) {
        return details.some(function(detail) {
            return detail.includes(' contains ');
        });
    }

    function ensureQueryStatus(table) {
        const container = $(table.table().container());
        let status = container.children('.ddbid-query-status');
        if (status.length) {
            return status;
        }

        container.addClass('ddbid-table-shell');
        status = $('<div>')
                .addClass('alert alert-info py-2 px-3 ddbid-query-status')
                .attr('role', 'status')
                .attr('aria-live', 'polite')
                .append($('<span>').addClass('ddbid-query-status-message'))
                .append($('<span>').addClass('ddbid-query-status-detail'));
        container.prepend(status);
        return status;
    }

    function setQueryStatus(table, visible, message, detail) {
        const status = ensureQueryStatus(table);
        status.toggleClass('is-visible', !!visible);
        status.find('.ddbid-query-status-message').text(message || '');
        status.find('.ddbid-query-status-detail').text(detail || '');
    }

    function installProcessingFeedback(table) {
        $(table.table().node()).on('processing.dt', function(e, settings, isProcessing) {
            if (!isProcessing) {
                setQueryStatus(table, false, '', '');
                return;
            }

            const details = activeFilterDetails();
            const detailText = details.length
                    ? details.join(' | ') + (hasContainsFilter(details) ? ' | Contains searches may take up to 5 minutes.' : '')
                    : 'Loading results...';
            setQueryStatus(table, true, 'Filtering database\u2026', detailText);
        });

        $(table.table().node()).on('xhr.dt error.dt', function() {
            setQueryStatus(table, false, '', '');
        });
    }

    function timestampOptions(entries) {
        const options = [{ value: '-1', label: 'All timestamps' }];
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

    function cacheKeyForEndpoint(endpoint) {
        return JSON_CACHE_PREFIX + endpoint;
    }

    function readCachedJson(endpoint, ttlMillis) {
        try {
            const raw = window.sessionStorage.getItem(cacheKeyForEndpoint(endpoint));
            if (!raw) {
                return null;
            }
            const payload = JSON.parse(raw);
            if (!payload || typeof payload.expiresAt !== 'number' || !('data' in payload)) {
                return null;
            }
            if (Date.now() >= payload.expiresAt) {
                return null;
            }
            return payload.data;
        } catch (e) {
            return null;
        }
    }

    function writeCachedJson(endpoint, data, ttlMillis) {
        try {
            const payload = {
                expiresAt: Date.now() + (ttlMillis || JSON_CACHE_TTL_MILLIS),
                data: data
            };
            window.sessionStorage.setItem(cacheKeyForEndpoint(endpoint), JSON.stringify(payload));
        } catch (e) {
            // Ignore cache write errors silently (e.g. private mode quota).
        }
    }

    function loadJsonWithCache(endpoint, onSuccess, onError, ttlMillis) {
        const cached = readCachedJson(endpoint, ttlMillis || JSON_CACHE_TTL_MILLIS);
        if (cached !== null) {
            onSuccess(cached, true);
            return;
        }

        $.getJSON(endpoint, function(json) {
            writeCachedJson(endpoint, json, ttlMillis || JSON_CACHE_TTL_MILLIS);
            onSuccess(json, false);
        }).fail(function() {
            if (onError) {
                onError();
            }
        });
    }

    function loadFilterOptions(endpoint, columnConfigs) {
        const configs = columnConfigs || [];
        const columnIndices = configs.map(function(config) {
            return config.columnIndex;
        });

        setColumnFiltersLoading(columnIndices, true, 'Loading suggestions...');
        loadJsonWithCache(endpoint, function(json) {
            const payload = json || {};
            configs.forEach(function(config) {
                const values = payload[config.optionKey] || config.fallback || [];
                attachDatalist(config.columnIndex, config.datalistId, stringOptions(values));
            });
            setColumnFiltersLoading(columnIndices, false);
        }, function() {
            setColumnFiltersLoading(columnIndices, false);
        });
    }

    function filterOption(columnIndex, datalistId, optionKey, fallback) {
        const config = {
            columnIndex: columnIndex,
            datalistId: datalistId,
            optionKey: optionKey
        };
        if (fallback) {
            config.fallback = fallback;
        }
        return config;
    }

    function entityDatalistId(entity, suffix) {
        return entity + suffix;
    }

    function entityFilterOptions(entity, extraOptions) {
        return [
            filterOption(2, entityDatalistId(entity, 'StatusOptions'), 'status', STATUS_OPTIONS.slice())
        ].concat((extraOptions || []).map(function(option) {
            return filterOption(
                    option.columnIndex,
                    entityDatalistId(entity, option.datalistSuffix),
                    option.optionKey,
                    option.fallback);
        }));
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

    function searchControls() {
        return [{
                "target": 1,
                "content": [
                    {
                        "extend": "searchText",
                        "clear": false,
                        "placeholder": "Search [title]"
                    }
                ]
            }];
    }

    function ajaxData() {
        return function(d) {
            applyColumnControlPayload(d);
            const timestampValue = columnFilterValue(0);
            d.status = columnFilterValue(2) || 'MISSING';
            if (timestampValue) {
                d.timestamp = isTimestampRequestValue(timestampValue) ? timestampValue : '__invalid__';
            }
            return JSON.stringify(d);
        };
    }

    function loadTimestamps(endpoint, datalistId) {
        setColumnFiltersLoading([0], true, 'Loading timestamps...');
        loadJsonWithCache(endpoint, function(json) {
            const entries = Object.entries(json || {});
            attachDatalist(0, datalistId, timestampOptions(entries));
            const latestValue = entries.length ? timestampEntryValue(entries[entries.length - 1]) : null;
            if (latestValue) {
                pendingFilterState.set(0, Object.assign({}, pendingFilterState.get(0), {
                    mode: 'equal',
                    value: latestValue,
                    readOnly: false,
                    triggerSearch: true,
                    forceValue: true
                }));
                applyPendingFilterState();
            }
            setColumnFiltersLoading([0], false);
        }, function() {
            setColumnFiltersLoading([0], false);
        });
    }

    function installDefaultFilters(table, timestampEndpoint, timestampDatalistId, statusDatalistId) {
        setupColumnControlSearchModes(table.columns().count());
        installFilterStatePersistence();
        loadTimestamps(timestampEndpoint, timestampDatalistId);
        attachDatalist(2, statusDatalistId, statusOptions());
        pendingFilterState.set(2, Object.assign({}, pendingFilterState.get(2), {
            mode: 'equal',
            value: 'MISSING',
            readOnly: false,
            triggerSearch: false,
            forceValue: true
        }));
        applyPendingFilterState();
    }

    function baseDataTableOptions(ajaxUrl, columns, order) {
        return {
            "dom": 'B<"row mb-3"<"col-12 pb-2"i>><"pb-3 mb-5"r<"table-responsive"t>><"footer fixed-bottom mt-auto py-3 bg-light"<"float-right"p>>',
            "processing": true,
            "serverSide": true,
            "paging": true,
            "colReorder": false,
            "responsive": false,
            "pagingType": "first_last_numbers",
            "searchDelay": 900,
            "lengthMenu": [
                [100, 250, 500, 1000, 2500, 5000],
                [100, 250, 500, 1000, 2500, 5000]
            ],
            "pageLength": 100,
            "order": order || [[1, 'asc']],
            "titleRow": 0,
            "ajax": {
                "url": ajaxUrl,
                "type": "POST",
                "dataType": "json",
                "contentType": "application/json",
                "data": ajaxData()
            },
            "fixedHeader": true,
            "columnControl": [
                ...sortControls(),
                ...searchControls()
            ],
            "autoWidth": false,
            "initComplete": function() {
                const table = this.api();
                installProcessingFeedback(table);
                const columnCount = table.columns().count();
                scheduleControlSetup(columnCount);
                $(table.table().node()).on('draw.dt column-reorder.dt', function() {
                    scheduleControlSetup(columnCount, 5);
                });
            },
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

    function initEntityTable(config) {
        configureColumnModes(config.exactOnlyColumns);
        const entity = config.entity;
        const table = $('#ddbid').DataTable(baseDataTableOptions(entity, config.columns, config.order));

        if (config.providerTooltips) {
            installProviderTooltips(document.getElementById('ddbid'));
        }

        installDefaultFilters(
                table,
                entity + '/timestamp',
                entityDatalistId(entity, 'TimestampOptions'),
                entityDatalistId(entity, 'StatusOptions'));

        if (config.filterOptions) {
            loadFilterOptions(entity + '/filter-options', config.filterOptions);
        }

        installBackToTop();
        return table;
    }

    function itemColumns() {
        return [{
                "data": "timestamp",
                "className": "text-nowrap"
            },
            {
                "data": "id",
                "className": "text-nowrap",
                "render": function(data, type) {
                    return type === 'display' && data ? ddbItemLink(data) : data;
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
                            return ddbProviderLink(providerId);
                        });
                    }
                    return data;
                }
            },
            {
                "data": "sector_fct",
                "className": "text-nowrap",
                "render": renderSector
            },
            {
                "data": "supplier_id",
                "className": "text-wrap"
            }
        ];
    }

    function personColumns() {
        return [{
                "data": "timestamp",
                "className": "text-nowrap"
            },
            {
                "data": "id",
                "className": "text-nowrap",
                "render": function(data, type) {
                    return type === 'display' && data ? ddbPersonLink(data) : data;
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
        ];
    }

    function organizationColumns() {
        return [{
                "data": "timestamp",
                "className": "text-nowrap"
            },
            {
                "data": "id",
                "className": "text-nowrap",
                "render": function(data, type) {
                    return type === 'display' && data ? ddbOrganizationLink(data) : data;
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
        ];
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
        ddbItemLink,
        ddbOrganizationLink,
        ddbPersonLink,
        ddbProviderLink,
        entityFilterOptions,
        escapeHtml,
        initEntityTable,
        itemColumns,
        installBackToTop,
        installDefaultFilters,
        installProviderTooltips,
        loadFilterOptions,
        loadJsonWithCache,
        organizationColumns,
        personColumns,
        renderSector,
        setColumnFiltersLoading,
        stringOptions
    };
})();
