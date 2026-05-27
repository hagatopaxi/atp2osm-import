document.addEventListener("DOMContentLoaded", () => {
  const banner = document.getElementById("thank-you-banner");
  if (!banner) return;

  const importDate = new Date(banner.dataset.importDate);
  const diffMs = Date.now() - importDate.getTime();

  // Hide banner if import is older than 5 minutes
  if (diffMs > 5 * 60 * 1000) {
    banner.remove();
  }
});
