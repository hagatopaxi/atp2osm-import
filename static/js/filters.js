// Submits a filter bar as soon as a non-text control changes (radio, select,
// date) — text input keeps its explicit "Filtrer" button.
document.addEventListener("DOMContentLoaded", () => {
  document.querySelectorAll("form[data-autosubmit]").forEach((form) => {
    form
      .querySelectorAll("input[type=radio], input[type=checkbox], input[type=date], select")
      .forEach((c) => c.addEventListener("change", () => form.submit()));

    // Period presets: they fill the two date bounds instead of being a filter
    // of their own, so a period is always expressed as a pair of dates.
    const iso = (d) => new Date(d - d.getTimezoneOffset() * 60000).toISOString().slice(0, 10);
    // A preset is a pair of dates: the same computation says what it fills in
    // and, against the current bounds, which one is the active period.
    const bounds = (days) => {
      const today = new Date();
      return days
        ? [iso(new Date(today.getTime() - (days - 1) * 86400000)), iso(today)]
        : ["", ""];
    };
    const from = form.querySelector("input[name=from]");
    const to = form.querySelector("input[name=to]");
    form.querySelectorAll("button[data-range]").forEach((button) => {
      const [start, end] = bounds(Number(button.dataset.range));
      button.classList.toggle("btn-active", from.value === start && to.value === end);
      button.addEventListener("click", () => {
        from.value = start;
        to.value = end;
        form.submit();
      });
    });

    // autofocus puts the caret at the start: move it back to the end.
    const search = form.querySelector("input[autofocus]");
    if (search) {
      const end = search.value.length;
      search.setSelectionRange(end, end);
    }
  });
});
