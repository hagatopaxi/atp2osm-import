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
  await fetch(`/brands/${wikidata}/upload`, { method: "POST" });
  window.location.href = "/";
}
