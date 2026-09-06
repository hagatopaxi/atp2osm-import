// Strings the scripts need, rendered by `_js_strings.html` as a JSON block —
// gettext extracts them from the template, so they live in the same catalog as
// the rest of the interface. Placeholders are written {name}.
const I18N_STRINGS = JSON.parse(
  document.getElementById("i18n-strings")?.textContent || "{}",
);

function t(key, vars = {}) {
  return (I18N_STRINGS[key] ?? key).replace(
    /\{(\w+)\}/g,
    (match, name) => vars[name] ?? match,
  );
}
