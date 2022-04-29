// Append '4d' to the colors (alpha channel), except for the hovered index
function handleHover(evt, item, legend) {
    legend.chart.data.datasets[0].backgroundColor.forEach((color, index, colors) => {
        colors[index] = index === item.index || color.length === 9 ? color : color + '4D';
    });
    legend.chart.update();
}

// Removes the alpha channel from background colors
function handleLeave(evt, item, legend) {
    legend.chart.data.datasets[0].backgroundColor.forEach((color, index, colors) => {
        colors[index] = color.length === 9 ? color.slice(0, -2) : color;
    });
    legend.chart.update();
}

function hexToRgbA(hex, alpha) {
    var c;
    if (/^#?([A-Fa-f0-9]{3}){1,2}$/.test(hex)) {
        if (hex.startsWith('#')) {
            c = hex.substring(1).split('');
        } else {
            c = hex.split('');
        }
        if (c.length === 3) {
            c = [c[0], c[0], c[1], c[1], c[2], c[2]];
        }
        c = '0x' + c.join('');
        return 'rgba(' + [(c >> 16) & 255, (c >> 8) & 255, c & 255].join(',') + ',' + alpha + ')';
    }
    throw new Error('Bad Hex');
}
const color = palette('tol-rainbow', 3);

function missingItemConfig(itemMissingKeys, itemMissingValues) {
    const missingItemConfig = {
        type: 'bar',
        data: {
            labels: itemMissingKeys,
            datasets: [
                {
                    label: 'item',
                    data: itemMissingValues,
                    borderColor: hexToRgbA(color[0], 1),
                    backgroundColor: hexToRgbA(color[0], 0.2),
                    yAxisID: 'y'
                }
            ]
        },
        options: {
            responsive: true,
            interaction: {
                mode: 'index',
                intersect: false
            },
            stacked: false,
            plugins: {
                title: {
                    display: true,
                    text: 'MISSING'
                }
            },
            scales: {
                y: {
                    type: 'linear',
                    display: true,
                    position: 'left'
                }
            }
        }
    };
    new Chart($('#missingItemConfig'), missingItemConfig);
}

function missingPersonConfig(personMissingKeys, personMissingValues) {
    const missingPersonConfig = {
        type: 'bar',
        data: {
            labels: personMissingKeys,
            datasets: [
                {
                    label: 'person',
                    data: personMissingValues,
                    borderColor: hexToRgbA(color[1], 1),
                    backgroundColor: hexToRgbA(color[1], 0.2),
                    yAxisID: 'y'
                }
            ]
        },
        options: {
            responsive: true,
            interaction: {
                mode: 'index',
                intersect: false
            },
            stacked: false,
            plugins: {
                title: {
                    display: true,
                    text: 'MISSING'
                }
            },
            scales: {
                y: {
                    type: 'linear',
                    display: true,
                    position: 'left'
                }
            }
        }
    };
    new Chart($('#missingPersonConfig'), missingPersonConfig);
}

function missingOrganizationConfig(organizationMissingKeys, organizationMissingValues) {
    const missingOrganizationConfig = {
        type: 'bar',
        data: {
            labels: organizationMissingKeys,
            datasets: [
                {
                    label: 'organization',
                    data: organizationMissingValues,
                    borderColor: hexToRgbA(color[2], 1),
                    backgroundColor: hexToRgbA(color[2], 0.2),
                    yAxisID: 'y'
                }
            ]
        },
        options: {
            responsive: true,
            interaction: {
                mode: 'index',
                intersect: false
            },
            stacked: false,
            plugins: {
                title: {
                    display: true,
                    text: 'MISSING'
                }
            },
            scales: {
                y: {
                    type: 'linear',
                    display: true,
                    position: 'left'
                }
            }
        }
    };
    new Chart($('#missingOrganizationConfig'), missingOrganizationConfig);
}

function missingByProvider_id(missingByProvider_idKeys, missingByProvider_idValues) {
    const missingByProvider_id = {
        type: 'doughnut',
        data: {
            labels: missingByProvider_idKeys,
            datasets: [
                {
                    label: 'MISSING by provider_id',
                    data: missingByProvider_idValues,
                    backgroundColor: palette('tol-rainbow', missingByProvider_idKeys.length).map(function (hex) {
                        return '#' + hex;
                    })
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                legend: {
                    display: false,
                    labels: {
                        generateLabels: (chart) => {
                            const datasets = chart.data.datasets;
                            return datasets[0].data.map((data, i) => ({
                                    text: `${data}: ${chart.data.labels[i]}`,
                                    fillStyle: datasets[0].backgroundColor[i],
                                }));
                        }
                    }
                },
                title: {
                    display: true,
                    text: 'MISSING by provider_id'
                },
                htmlLegend: {
                    // ID of the container to put the legend in
                    containerID: 'missingByProviderLegend',
                }
            }
        },
        plugins: [htmlLegendPlugin]
    };
    new Chart($('#missingByProvider'), missingByProvider_id);
}

function missingBySector_fct(missingBySector_fctKeys, missingBySector_fctValues) {
    const missingBySector_fct = {
        type: 'doughnut',
        data: {
            labels: missingBySector_fctKeys,
            datasets: [
                {
                    label: 'MISSING by sector_fct',
                    data: missingBySector_fctValues,
                    backgroundColor: palette('tol-rainbow', missingBySector_fctKeys.length).map(function (hex) {
                        return '#' + hex;
                    })
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                legend: {
                    display: false,
                    labels: {
                        generateLabels: (chart) => {
                            const datasets = chart.data.datasets;
                            return datasets[0].data.map((data, i) => ({
                                    text: `${data}: ${chart.data.labels[i]}`,
                                    fillStyle: datasets[0].backgroundColor[i]
                                }));
                        }
                    }
                },
                title: {
                    display: true,
                    text: 'MISSING by sector_fct'
                },
                htmlLegend: {
                    // ID of the container to put the legend in
                    containerID: 'missingBySectorLegend'
                }
            }
        },
        plugins: [htmlLegendPlugin]
    };
    new Chart($('#missingBySector'), missingBySector_fct);
}
