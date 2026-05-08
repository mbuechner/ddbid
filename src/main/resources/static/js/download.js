$(function() {
    const catalogUrl = $('body').attr('data-download-catalog-url');
    const status = $('#download-status');
    const refresh = $('#download-refresh');
    let loadingCatalog = false;

    function setLoading(isLoading) {
        loadingCatalog = isLoading;
        refresh.prop('disabled', loadingCatalog);
        refresh.find('i').toggleClass('fa-spin', loadingCatalog);
    }

    function renderEntry(entry) {
        const link = $('<a>')
                .addClass('download-entry-link text-break')
                .attr('href', entry.href)
                .text(entry.name);

        const meta = $('<span>').addClass('download-entry-size text-muted').text(entry.size ? entry.size : '');
        return $('<div>').addClass('list-group-item download-entry').append(link, meta);
    }

    function renderList(target, entries) {
        target.empty();
        if (!entries || entries.length === 0) {
            target.append($('<div>').addClass('list-group-item text-muted').text('No downloads available'));
            return;
        }
        for (const entry of entries) {
            target.append(renderEntry(entry));
        }
    }

    function renderGroup(group) {
        const dumpsId = 'downloads-' + group.type + '-dumps';
        const compareId = 'downloads-' + group.type + '-compare';
        const dumpCount = group.dumps ? group.dumps.length : 0;
        const compareCount = group.compare ? group.compare.length : 0;

        const section = $('<section>').addClass('col-12 col-xl-4');
        section.append(
                $('<div>').addClass('d-flex align-items-center justify-content-between mb-3').append(
                $('<h2>').addClass('h4 mb-0').text(group.label),
                $('<span>').addClass('badge text-bg-secondary').text(dumpCount + compareCount)
                )
                );

        section.append($('<h3>').addClass('h6 text-muted mb-2').text('Data dumps'));
        section.append($('<div>').addClass('list-group download-list mb-4').attr('id', dumpsId));
        section.append($('<h3>').addClass('h6 text-muted mb-2').text('Compare files'));
        section.append($('<div>').addClass('list-group download-list').attr('id', compareId));

        $('#download-groups').append(section);
        renderList($('#' + dumpsId), group.dumps);
        renderList($('#' + compareId), group.compare);
    }

    function renderCatalog(catalog) {
        $('#download-groups').empty();
        for (const group of catalog.groups) {
            renderGroup(group);
        }

        const generatedAt = catalog.generatedAt ? new Date(catalog.generatedAt).toLocaleString() : '';
        status.text(generatedAt ? 'Updated ' + generatedAt : 'Ready');
    }

    function loadCatalog(refreshCatalog) {
        setLoading(true);
        status.text(refreshCatalog ? 'Refreshing...' : 'Loading...');

        $.getJSON(catalogUrl, {refresh: refreshCatalog})
                .done(renderCatalog)
                .fail(function() {
                    status.text('Downloads could not be loaded');
                    $('#download-groups').empty();
                })
                .always(function() {
                    setLoading(false);
                });
    }

    refresh.on('click', function() {
        loadCatalog(true);
    });

    loadCatalog(false);
});
