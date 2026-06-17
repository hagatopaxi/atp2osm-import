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
  const warning = document.getElementById("warning");
  const warningIcon = warning.querySelector("i");
  const warningText = warning.querySelector("span");

  const data = await response.json();

  if (!response.ok) {
    loading.classList.add("hidden");
    button_validate.removeAttribute("disabled");
    button_cancel.removeAttribute("disabled");
    warningIcon.className = "iconoir-warning-circle";
    warningText.textContent = `Erreur lors de l'intégration : ${data.errors.join(", ")} — Vous pouvez réessayer ultérieurement.`;
    warning.classList.remove("alert-warning");
    warning.classList.add("alert-error");
    return;
  }

  if (data.partial) {
    loading.classList.add("hidden");
    warningText.textContent = `Intégration partielle : certains départements n'ont pas pu être intégrés (${data.errors.join(", ")}). Redirection dans quelques secondes…`;
    setTimeout(() => { window.location.href = `/history/${data.id}`; }, 4000);
    return;
  }

  window.location.href = `/history/${data.id}`;
}
