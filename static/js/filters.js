// Submits a filter bar as soon as a non-text control changes (radio, select,
// date) — text input keeps its explicit "Filtrer" button.
document.addEventListener("DOMContentLoaded", () => {
  document.querySelectorAll("form[data-autosubmit]").forEach((form) => {
    form
      .querySelectorAll("input[type=radio], input[type=checkbox], input[type=date], select")
      .forEach((c) => c.addEventListener("change", () => form.submit()));

    // autofocus puts the caret at the start: move it back to the end.
    const search = form.querySelector("input[autofocus]");
    if (search) {
      const end = search.value.length;
      search.setSelectionRange(end, end);
    }
  });
});
