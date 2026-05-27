document.addEventListener("DOMContentLoaded", () => {
  const rows = document.querySelectorAll("tbody tr[data-importable]");
  const betaNotice = document.getElementById("beta-notice");

  // Populate counts
  document.getElementById("count-importable").textContent = [
    ...rows,
  ].filter((r) => r.dataset.importable === "true").length;
  document.getElementById("count-all").textContent = rows.length;

  function applyFilter(value) {
    rows.forEach((row) => {
      row.classList.toggle(
        "hidden",
        value === "importable" && row.dataset.importable !== "true"
      );
    });
    betaNotice.classList.toggle("hidden", value === "importable");
  }

  document.querySelectorAll('input[name="brand-filter"]').forEach((radio) => {
    radio.addEventListener("change", () => applyFilter(radio.value));
  });

  // Apply default filter (radio is already checked="importable" in HTML)
  applyFilter("importable");
});
