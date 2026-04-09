function extractWikidata(url) {
  const parts = url.split("/");
  const qCode = parts.find((part) => /^Q\d+$/.test(part));
  return qCode;
}

async function confirm_import() {
  const loading = document.getElementById("loading");
  loading.classList.remove("hidden");
  const button_validate = document.getElementById("submit_importation");
  const button_cancel = document.getElementById("cancel");
  button_validate.setAttribute("disabled", true);
  button_cancel.setAttribute("disabled", true);
  const wikidata = extractWikidata(window.location.href);
  const response = await fetch(`/brands/${wikidata}/upload`, { method: "POST" });
  if (!response.ok) {
    const data = await response.json();
    loading.classList.add("hidden");
    button_validate.removeAttribute("disabled");
    button_cancel.removeAttribute("disabled");
    const warning = document.getElementById("warning");
    warning.innerHTML = `<i class="iconoir-warning-circle"></i><span>Erreur lors de l'importation : ${data.errors.join(", ")}<br>Vous pouvez réessayer ultérieurement.</span>`;
    warning.classList.remove("alert-warning");
    warning.classList.add("alert-error");
    return;
  }
  window.location.href = "/";
}
