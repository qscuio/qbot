/**
 * Sector info display
 */

/**
 * Update sector banner with info
 * @param {Object} sectorInfo 
 */
export function updateSectorBanner(sectorInfo) {
    const banner = document.getElementById('sector-banner');
    const container = document.getElementById('sector-container');

    if (!sectorInfo || (!sectorInfo.industry && (!sectorInfo.concepts || sectorInfo.concepts.length === 0))) {
        banner.style.display = 'none';
        return;
    }

    container.innerHTML = '';
    banner.style.display = 'flex';

    // Industry
    if (sectorInfo.industry) {
        const tag = createSectorTag(sectorInfo.industry.name, 'industry', sectorInfo.industry.performance);
        container.appendChild(tag);
    }

    // Concepts
    if (sectorInfo.concepts && sectorInfo.concepts.length > 0) {
        sectorInfo.concepts.forEach(c => {
            const tag = createSectorTag(c.name, 'concept', c.performance);
            container.appendChild(tag);
        });
    }
}

/**
 * Create a sector tag element
 * @param {string} name 
 * @param {string} type 
 * @param {Object} perf 
 * @returns {HTMLElement}
 */
function createSectorTag(name, type, perf) {
    const tag = document.createElement('div');
    tag.className = `sector-tag ${type}`;

    let html = `<span class="sector-name">${name}</span>`;

    if (perf) {
        // defined keys for iteration
        const periods = [
            { key: 'day', label: 'D' },
            { key: 'week', label: 'W' },
            { key: 'month', label: 'M' }
        ];

        periods.forEach(p => {
            const val = perf[p.key];
            if (val !== undefined && val !== null) {
                const colorClass = val > 0 ? 'up' : val < 0 ? 'down' : 'flat';
                const sign = val > 0 ? '+' : '';
                // Add a small spacer/separator if not the first
                html += ` <span class="sector-val ${colorClass}" style="margin-left:4px; font-size:10px;">${p.label}:${sign}${val.toFixed(1)}%</span>`;
            }
        });
    } else {
        html += ` <span class="sector-val flat">--%</span>`;
    }

    tag.innerHTML = html;
    return tag;
}
