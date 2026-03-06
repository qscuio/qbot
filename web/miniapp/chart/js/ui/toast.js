/**
 * Toast notification UI
 */

/**
 * Show a toast notification
 * @param {string} msg - Message to display
 */
export function showToast(msg) {
    const toast = document.createElement('div');
    toast.textContent = msg;
    toast.style.position = 'fixed';
    toast.style.bottom = '80px';
    toast.style.left = '50%';
    toast.style.transform = 'translateX(-50%)';
    toast.style.background = 'rgba(0,0,0,0.7)';
    toast.style.color = 'white';
    toast.style.padding = '8px 16px';
    toast.style.borderRadius = '20px';
    toast.style.fontSize = '12px';
    toast.style.zIndex = '100';
    toast.style.opacity = '0';
    toast.style.transition = 'opacity 0.3s';

    document.body.appendChild(toast);

    // Trigger reflow
    toast.offsetHeight;
    toast.style.opacity = '1';

    setTimeout(() => {
        toast.style.opacity = '0';
        setTimeout(() => toast.remove(), 300);
    }, 2000);
}
