function extractWikidata(url) {
  const parts = url.split("/");
  return parts.find((part) => /^Q\d+$/.test(part));
}

function renderInvalidations() {
  const container = document.getElementById("invalidations_list");
  const data = JSON.parse(sessionStorage.getItem("invalidations") || "[]");

  if (data.length === 0) {
    container.innerHTML =
      '<p class="text-base-content/60">Aucune invalidation trouvée.</p>';
    return;
  }

  for (const item of data) {
    const card = document.createElement("div");
    card.className =
      "card bg-base-100 border-error border shadow-md p-4 flex flex-row items-start gap-4";
    card.innerHTML = `
      <i class="iconoir-prohibition text-error text-2xl mt-1"></i>
      <div>
        <p class="font-semibold">${item.title}</p>
        ${item.comment ? `<p class="text-base-content/70 mt-1">${item.comment}</p>` : ""}
      </div>
    `;
    container.appendChild(card);
  }
}

async function confirmRejection() {
  const wikidata = extractWikidata(window.location.href);
  const data = JSON.parse(sessionStorage.getItem("invalidations") || "[]");
  const confirmBtn = document.getElementById("confirm_btn");
  const loading = document.getElementById("loading");

  confirmBtn.setAttribute("disabled", true);
  loading.classList.remove("hidden");

  const comment = data.map((item) => ({
    osm_id: item.osm_id,
    osm_type: item.osm_type,
    comment: item.comment,
  }));

  try {
    const res = await fetch(`/brands/${wikidata}/report-error`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ comment: JSON.stringify(comment) }),
    });

    if (!res.ok) throw new Error(res.statusText);

    sessionStorage.removeItem("invalidations");
    window.location.href = "/brands";
  } catch (err) {
    alert("Erreur lors de l'envoi : " + err.message);
    confirmBtn.removeAttribute("disabled");
    loading.classList.add("hidden");
  }
}

document.addEventListener("DOMContentLoaded", renderInvalidations);
