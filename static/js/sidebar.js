const LG_BREAKPOINT = 1024;
const STORAGE_KEY = 'sidebar_collapsed';

const toggle = document.getElementById('sidebar-toggle');
const sidebar = document.getElementById('sidebar');

toggle.addEventListener('change', function () {
    localStorage.setItem(STORAGE_KEY, this.checked ? '0' : '1');
});

(function () {
    const stored = localStorage.getItem(STORAGE_KEY);
    const open = stored !== null ? stored !== '1' : window.innerWidth >= LG_BREAKPOINT;
    sidebar.style.transition = 'none';
    toggle.checked = open;
    sidebar.offsetWidth;
    sidebar.style.transition = '';
})();

let lastBreakpoint = window.innerWidth >= LG_BREAKPOINT ? 'lg' : 'md';
window.addEventListener('resize', function () {
    const bp = window.innerWidth >= LG_BREAKPOINT ? 'lg' : 'md';
    if (bp !== lastBreakpoint) {
        lastBreakpoint = bp;
        toggle.checked = bp === 'lg';
        localStorage.removeItem(STORAGE_KEY);
    }
});
