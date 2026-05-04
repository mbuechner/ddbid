$(function() {
    const statisticsUrl = $('body').attr('data-statistics-url');
    const status = $('#statistics-status');
    const refresh = $('#statistics-refresh');
    const numberFormat = new Intl.NumberFormat('de-DE');
    const percentFormat = new Intl.NumberFormat('de-DE', {
        minimumFractionDigits: 1,
        maximumFractionDigits: 2
    });
    const tableHelper = window.DDBID.table;
    const charts = {};
    const sectorLabels = {
        "sec_01": "Archive",
        "sec_02": "Library",
        "sec_03": "Monument preservation",
        "sec_04": "Science",
        "sec_05": "Media library",
        "sec_06": "Museum",
        "sec_07": "Other"
    };

    const chartColors = {
        missing: '#c43c39',
        missingSoft: 'rgba(196, 60, 57, 0.18)',
        fresh: '#2f8f5b',
        freshSoft: 'rgba(47, 143, 91, 0.18)',
        provider: '#376996',
        sector: '#7b5d8d',
        sectorRatio: '#9c5f2d'
    };

    Chart.defaults.font.family = '"Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif';
    Chart.defaults.color = '#4f5862';

    function setLoading(isLoading) {
        refresh.prop('disabled', isLoading);
        refresh.find('i').toggleClass('fa-spin', isLoading);
    }

    function formatNumber(value) {
        return numberFormat.format(value || 0);
    }

    function formatPercent(value) {
        return percentFormat.format(value || 0) + '%';
    }

    function appendProviderLinks(cell, label) {
        const text = label || '(unknown)';
        const pattern = /[A-Za-z0-9]{32}/g;
        let lastIndex = 0;
        let match = pattern.exec(text);

        if (!match) {
            cell.text(text);
            if (sectorLabels[text]) {
                cell.attr('title', sectorLabels[text]);
            }
            return;
        }

        while (match) {
            if (match.index > lastIndex) {
                cell.append(document.createTextNode(text.substring(lastIndex, match.index)));
            }
            cell.append($(tableHelper.ddbProviderLink(match[0])));
            lastIndex = match.index + match[0].length;
            match = pattern.exec(text);
        }

        if (lastIndex < text.length) {
            cell.append(document.createTextNode(text.substring(lastIndex)));
        }
    }

    function destroyChart(id) {
        if (charts[id]) {
            charts[id].destroy();
        }
    }

    function renderMetric(id, value) {
        $(id).text(formatNumber(value));
    }

    function renderMetrics(data) {
        const rangeLabel = data.minTimestampLabel ? 'since ' + data.minTimestampLabel : '-';
        const latestLabel = data.latestTimestampLabel || '-';

        renderMetric('#stat-missing-total', data.missingItemsCount);
        renderMetric('#stat-new-total', data.newItemsCount);
        renderMetric('#stat-missing-latest', data.latestMissingItemsCount);
        renderMetric('#stat-new-latest', data.latestNewItemsCount);

        $('#stat-missing-range').text(rangeLabel);
        $('#stat-new-range').text(rangeLabel);
        $('#stat-missing-latest-date').text(latestLabel);
        $('#stat-new-latest-date').text(latestLabel);
        $('#stat-point-count').text(data.itemStatusKeys ? data.itemStatusKeys.length : 0);
    }

    function renderTrend(data) {
        destroyChart('itemStatusTrend');
        charts.itemStatusTrend = new Chart($('#itemStatusTrend'), {
            type: 'line',
            data: {
                labels: data.itemStatusKeys || [],
                datasets: [
                    {
                        label: 'MISSING',
                        data: data.itemMissingValues || [],
                        borderColor: chartColors.missing,
                        backgroundColor: chartColors.missingSoft,
                        pointRadius: 2,
                        pointHoverRadius: 5,
                        tension: 0.25,
                        fill: true
                    },
                    {
                        label: 'NEW',
                        data: data.itemNewValues || [],
                        borderColor: chartColors.fresh,
                        backgroundColor: chartColors.freshSoft,
                        pointRadius: 2,
                        pointHoverRadius: 5,
                        tension: 0.25,
                        fill: true
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: {
                    mode: 'index',
                    intersect: false
                },
                plugins: {
                    legend: {
                        position: 'top'
                    },
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                return context.dataset.label + ': ' + formatNumber(context.parsed.y);
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        ticks: {
                            maxRotation: 0,
                            autoSkip: true,
                            maxTicksLimit: 10
                        },
                        grid: {
                            display: false
                        }
                    },
                    y: {
                        beginAtZero: true,
                        ticks: {
                            callback: formatNumber
                        }
                    }
                }
            }
        });
    }

    function sliceTop(labels, values, limit) {
        const resultLabels = [];
        const resultValues = [];
        for (let i = 0; i < Math.min(labels.length, limit); i++) {
            resultLabels.push(labels[i] || '(unknown)');
            resultValues.push(values[i] || 0);
        }
        return {labels: resultLabels, values: resultValues};
    }

    function renderHorizontalBar(id, labels, values, label, color) {
        destroyChart(id);
        charts[id] = new Chart($('#' + id), {
            type: 'bar',
            data: {
                labels: labels,
                datasets: [{
                        label: label,
                        data: values,
                        backgroundColor: color,
                        borderWidth: 0,
                        borderRadius: 3
                    }]
            },
            options: {
                indexAxis: 'y',
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        display: false
                    },
                    tooltip: {
                        callbacks: {
                            title: function(items) {
                                const text = items.length ? items[0].label : '';
                                return sectorLabels[text] ? text + ' - ' + sectorLabels[text] : text;
                            },
                            label: function(context) {
                                return formatNumber(context.parsed.x);
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        beginAtZero: true,
                        ticks: {
                            callback: formatNumber
                        }
                    },
                    y: {
                        ticks: {
                            autoSkip: false,
                            callback: function(value) {
                                const text = this.getLabelForValue(value);
                                return text.length > 28 ? text.slice(0, 25) + '...' : text;
                            }
                        },
                        grid: {
                            display: false
                        }
                    }
                }
            }
        });
    }

    function renderHorizontalPercentBar(id, labels, values, label, color) {
        destroyChart(id);
        charts[id] = new Chart($('#' + id), {
            type: 'bar',
            data: {
                labels: labels,
                datasets: [{
                        label: label,
                        data: values,
                        backgroundColor: color,
                        borderWidth: 0,
                        borderRadius: 3
                    }]
            },
            options: {
                indexAxis: 'y',
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        display: false
                    },
                    tooltip: {
                        callbacks: {
                            title: function(items) {
                                const text = items.length ? items[0].label : '';
                                return sectorLabels[text] ? text + ' - ' + sectorLabels[text] : text;
                            },
                            label: function(context) {
                                return formatPercent(context.parsed.x);
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        beginAtZero: true,
                        ticks: {
                            callback: formatPercent
                        }
                    },
                    y: {
                        ticks: {
                            autoSkip: false,
                            callback: function(value) {
                                const text = this.getLabelForValue(value);
                                return text.length > 28 ? text.slice(0, 25) + '...' : text;
                            }
                        },
                        grid: {
                            display: false
                        }
                    }
                }
            }
        });
    }

    function renderRanking(target, labels, values) {
        const list = $(target);
        list.empty();
        if (!labels.length) {
            list.append($('<div>').addClass('text-muted small').text('No data'));
            return;
        }

        const total = values.reduce((sum, value) => sum + value, 0);
        const table = $('<table>').addClass('table table-sm table-borderless stat-table mb-0');
        const body = $('<tbody>');
        for (let i = 0; i < Math.min(labels.length, 8); i++) {
            const value = values[i] || 0;
            const percent = total > 0 ? Math.round((value / total) * 1000) / 10 : 0;
            const labelCell = $('<td>').addClass('stat-table-label');
            appendProviderLinks(labelCell, labels[i]);
            body.append(
                    $('<tr>').append(
                    labelCell,
                    $('<td>').addClass('stat-table-value').text(formatNumber(value) + ' / ' + percent + '%')
                    ));
        }
        table.append(body);
        list.append(table);
    }

    function renderSectorLossRanking(target, entries) {
        const list = $(target);
        list.empty();
        if (!entries.length) {
            list.append($('<div>').addClass('text-muted small').text('No data'));
            return;
        }

        const table = $('<table>').addClass('table table-sm table-borderless stat-table mb-0');
        const body = $('<tbody>');
        for (let i = 0; i < Math.min(entries.length, 8); i++) {
            const entry = entries[i] || {};
            const labelCell = $('<td>').addClass('stat-table-label');
            appendProviderLinks(labelCell, entry.sector || '(unknown)');
            body.append(
                    $('<tr>').append(
                    labelCell,
                    $('<td>').addClass('stat-table-value').text(
                    formatNumber(entry.missingCount) + ' / ' + formatNumber(entry.totalCount) + ' (' + formatPercent(entry.lossPercent) + ')'
                    )
                    ));
        }
        table.append(body);
        list.append(table);
    }

    function renderBreakdowns(data) {
        const provider = sliceTop(data.missingByProviderIdKeys || [], data.missingByProviderIdValues || [], 15);
        const sector = sliceTop(data.missingBySectorFctKeys || [], data.missingBySectorFctValues || [], 15);
        const sectorLoss = (data.missingBySectorLossRatios || []).slice(0, 15);
        const sectorLossLabels = sectorLoss.map(function(entry) {
            return (entry && entry.sector) ? entry.sector : '(unknown)';
        });
        const sectorLossValues = sectorLoss.map(function(entry) {
            return (entry && entry.lossPercent) ? entry.lossPercent : 0;
        });

        renderHorizontalBar('missingByProvider', provider.labels, provider.values, 'MISSING', chartColors.provider);
        renderHorizontalBar('missingBySector', sector.labels, sector.values, 'MISSING', chartColors.sector);
        renderHorizontalPercentBar('missingBySectorRatio', sectorLossLabels, sectorLossValues, 'Loss %', chartColors.sectorRatio);
        renderRanking('#missingByProviderList', provider.labels, provider.values);
        renderRanking('#missingBySectorList', sector.labels, sector.values);
        renderSectorLossRanking('#missingBySectorRatioList', sectorLoss);

    }

    function renderStatistics(data) {
        renderMetrics(data);
        renderTrend(data);
        renderBreakdowns(data);
        status.text(data.latestTimestampLabel ? 'Updated ' + data.latestTimestampLabel : 'Ready');
    }

    function loadStatistics(refreshData) {
        setLoading(true);
        status.text(refreshData ? 'Refreshing...' : 'Loading...');
        $.getJSON(statisticsUrl, {refresh: refreshData})
                .done(renderStatistics)
                .fail(function() {
                    status.text('Statistics could not be loaded');
                })
                .always(function() {
                    setLoading(false);
                });
    }

    refresh.on('click', function() {
        loadStatistics(true);
    });

    tableHelper.installProviderTooltips(document.querySelector('main'));
    loadStatistics(false);
});
